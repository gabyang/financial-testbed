from datetime import date, timedelta
import json, re, html, time
from bs4 import BeautifulSoup

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.regression import LinearRegression

BASE_PATH = "./test_data"  # root of SEC‑Filings & News folders

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (SparkSession.builder
         .appName("Workload2-Run")
         .enableHiveSupport()
         .getOrCreate())

spark.conf.set("spark.sql.shuffle.partitions", 200)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50 MB

# ---------------------------------------------------------------------------
# Register existing Parquet tables created by setup.py
# ---------------------------------------------------------------------------
for tbl in ["ticks", "profiles", "historic_estimates", "sec_eps_temp"]:
    spark.table(tbl).createOrReplaceTempView(tbl)

# ---------------------------------------------------------------------------
# Text cleaning UDF
# ---------------------------------------------------------------------------

def _clean_text(txt: str) -> str:
    txt = txt.lower()
    txt = re.sub(r"<style.*?</style>|<script.*?</script>", " ", txt, flags=re.S)
    txt = BeautifulSoup(txt, "html.parser").get_text(" ")
    txt = re.sub(r"[^\w\s$%]", " ", txt)
    txt = re.sub(r"\b\d+\b", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()

clean_udf = F.udf(_clean_text, T.StringType())

# ---------------------------------------------------------------------------
# ML pipeline
# ---------------------------------------------------------------------------
TOKENIZER  = RegexTokenizer(inputCol="text", outputCol="tokens", pattern="\\W")
STOPPER    = StopWordsRemover(inputCol="tokens", outputCol="filtered")
TF         = HashingTF(inputCol="filtered", outputCol="tf", numFeatures=20000)
IDF_STAGE  = IDF(inputCol="tf", outputCol="tfidf")
LR         = LinearRegression(featuresCol="tfidf", labelCol="pct_change")
NLP_PIPE   = Pipeline(stages=[TOKENIZER, STOPPER, TF, IDF_STAGE, LR])

# ---------------------------------------------------------------------------
# Helper to materialise SEC & news texts into a DataFrame for the window
# ---------------------------------------------------------------------------

def load_external_texts(window_start: date, window_end: date):
    sec_path  = f"{BASE_PATH}/SEC-Clean/{window_start}_{window_end}/*"
    news_path = f"{BASE_PATH}/News-Clean/{window_start}_{window_end}/*"

    df = (spark.read.text(sec_path).union(spark.read.text(news_path))
             .select(clean_udf("value").alias("text")))
    return df

# ---------------------------------------------------------------------------
# SQL assembling function – window boundaries are formatted inline
# ---------------------------------------------------------------------------

def movement_sql(w_start: date, w_end: date) -> str:
    return f"""
WITH estimate_vs_actual AS (
  SELECT e.symbol, e.fiscal_date_ending, s.start_date, e.eps_estimated, s.eps_diluted
  FROM historic_estimates e
  JOIN sec_eps_temp s
    ON e.symbol = s.symbol
   AND e.fiscal_date_ending = s.filing_period
),

daily_prices AS (
  SELECT symbol,
         CAST(time AS DATE) AS date,
         FIRST_VALUE(open)  OVER (PARTITION BY symbol, CAST(time AS DATE) ORDER BY time)                AS day_open,
         LAST_VALUE(close)  OVER (PARTITION BY symbol, CAST(time AS DATE) ORDER BY time
                                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS day_close,
         time
  FROM ticks
  WHERE time >= TIMESTAMP '{w_start}' AND time < TIMESTAMP '{w_end}'
),

distinct_daily_prices AS (
  SELECT DISTINCT symbol, date, day_open, day_close
  FROM daily_prices
),

price_movements AS (
  SELECT ev.symbol,
         ev.fiscal_date_ending   AS filing_period,
         ev.start_date,
         dp1.day_open            AS same_day_open,
         dp1.day_close           AS same_day_close,
         dp2.day_close           AS next_day_close,
         ROUND(100.0 * (dp1.day_close - dp1.day_open) / dp1.day_open, 2) AS same_day_change,
         ROUND(100.0 * (dp2.day_close - dp1.day_close) / dp1.day_close, 2) AS next_day_change
  FROM estimate_vs_actual ev
  JOIN distinct_daily_prices dp1
    ON ev.symbol = dp1.symbol AND ev.start_date = dp1.date
  LEFT JOIN distinct_daily_prices dp2
    ON dp1.symbol = dp2.symbol AND dp2.date = dp1.date + INTERVAL 1 day
),

filtered AS (
  SELECT *
  FROM price_movements
  WHERE abs(same_day_change) > 3 OR abs(next_day_change) > 3
)

SELECT DISTINCT f.symbol,
       f.filing_period,
       p.industry,
       f.same_day_change,
       f.next_day_change
FROM filtered f
JOIN profiles p USING(symbol)
"""

# ---------------------------------------------------------------------------
# Streaming‑style micro‑batch loop
# ---------------------------------------------------------------------------
stream_start = date(2020, 1, 1)
stream_end   = date(2023, 12, 31)
window_size  = timedelta(days=7)

logs = []
w = stream_start
while w < stream_end:
    t0 = time.perf_counter()

    window_df = spark.sql(movement_sql(w, w + window_size))
    record_ct = window_df.count()

    if record_ct:
        # External cleaned texts (produced offline or by another job)
        texts_df = load_external_texts(w, w + window_size)
        full_df = window_df.crossJoin(texts_df)  # small → broadcast
        full_df = full_df.withColumn("pct_change",
                                     F.when(F.isnull("next_day_change"), F.col("same_day_change"))
                                      .otherwise(F.col("same_day_change") + F.col("next_day_change")))

        @F.pandas_udf("industry string, top_terms array<string>", F.PandasUDFType.GROUPED_MAP)
        def per_industry(pdf):
            model = NLP_PIPE.fit(pdf)
            coeffs = model.stages[-1].coefficients
            vocab  = model.stages[2].vocabulary
            top    = [vocab[i] for i in coeffs.argsort()[-10:]]
            import pandas as pd
            return pd.DataFrame({"industry": [pdf.industry.iloc[0]], "top_terms": [top]})

        keyword_df = full_df.groupby("industry").apply(per_industry)
        keyword_df.write.mode("overwrite").json(f"nlp_keywords_{w}.json")

    t1 = time.perf_counter()
    logs.append({"window_start": str(w),
                 "records": record_ct,
                 "runtime_sec": round(t1 - t0, 2)})
    w += window_size

with open("stream_batch_logs_spark.json", "w") as fp:
    json.dump(logs, fp, indent=2)

spark.stop()
