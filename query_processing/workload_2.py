import pandas as pd
import psycopg2
# Bulk insert
from psycopg2.extras import execute_values
import json
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LinearRegression
import numpy as np

# Load SEC CSV
df_sec_eps = pd.read_csv("sec_extracts.csv", parse_dates=["start_date"])

# Connect to DB
conn = psycopg2.connect(
    dbname="financial_db", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

# Optional: Insert into a temp table for easier processing
cur.execute("""
    DROP TABLE IF EXISTS sec_eps_temp;
    CREATE TEMP TABLE sec_eps_temp (
        symbol VARCHAR(10),
        start_date DATE,
        filing_period DATE,
        eps_basic NUMERIC,
        eps_diluted NUMERIC,
        eps_basic9 NUMERIC,
        eps_diluted9 NUMERIC
    );
""")
conn.commit()

# Convert "-" to NaN
df_sec_eps.replace("-", None, inplace=True)

# Optionally, also convert empty strings and other symbols to None
df_sec_eps.replace({"": None, "NA": None, "N/A": None}, inplace=True)

# Explicitly cast numeric columns
numeric_cols = ['eps_basic', 'eps_diluted', 'eps_basic9', 'eps_diluted9']
df_sec_eps[numeric_cols] = df_sec_eps[numeric_cols].apply(pd.to_numeric, errors='coerce')

execute_values(cur, "INSERT INTO sec_eps_temp VALUES %s", df_sec_eps.values.tolist())
conn.commit()

query_filing_periods = """
WITH estimate_vs_actual AS (
    SELECT 
        e.symbol,
        e.fiscal_date_ending,
        s.start_date,
        e.eps_estimated,
        s.eps_diluted
    FROM historic_estimates e
    JOIN sec_eps_temp s 
        ON e.symbol = s.symbol 
       AND e.fiscal_date_ending = s.filing_period
    WHERE abs(e.eps_estimated - s.eps_diluted) > 0.01
),
daily_closes AS (
    SELECT 
        symbol,
        time::date AS date,
        AVG(close) AS avg_close
    FROM stock_ticks
    GROUP BY symbol, time::date
),
price_movements AS (
    SELECT 
        ev.symbol,
        ev.fiscal_date_ending AS filing_period,
        ev.start_date,
        dc1.avg_close AS close_today,
        dc2.avg_close AS close_next_day,
        ROUND(100.0 * (dc2.avg_close - dc1.avg_close) / dc1.avg_close, 2) AS pct_change
    FROM estimate_vs_actual ev
    JOIN daily_closes dc1 
        ON ev.symbol = dc1.symbol 
       AND ev.start_date = dc1.date
    LEFT JOIN daily_closes dc2 
        ON dc1.symbol = dc2.symbol 
       AND dc2.date = dc1.date + INTERVAL '1 day'
),
filtered AS (
    SELECT *
    FROM price_movements
    WHERE abs(pct_change) > 3
)
SELECT DISTINCT 
    f.symbol, 
    f.filing_period, 
    p.industry, 
    f.pct_change
FROM filtered f
JOIN profiles p ON f.symbol = p.symbol;

"""

df = pd.read_sql_query(query_filing_periods, conn)
df.to_csv("eps_surprise_movements.csv", index=False)
print("✅ Results written to eps_surprise_movements.csv")

df = pd.read_csv("eps_surprise_movements.csv")

# Connect to DB
conn = psycopg2.connect(
    dbname="financial_db", user="postgres", password="postgres", host="localhost", port="5432"
)
cur = conn.cursor()

def get_sec_text(symbol, filing_period, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.content
            FROM sec_filings f
            JOIN sec_filing_chunks c ON f.id = c.filing_id
            WHERE f.symbol = %s AND f.filing_type = '10-Q'
              AND f.filing_date >= %s
            ORDER BY c.chunk_index
        """, (symbol, filing_period))
        return " ".join(r[0] for r in cur.fetchall())

def get_news_text(symbol, filing_period, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT ac.chunk_text
            FROM articles a
            JOIN article_chunks ac ON a.id = ac.article_id
            WHERE a.symbol = %s AND a.date >= %s::date - INTERVAL '60 days' AND a.date <= %s::date + INTERVAL '60 days'
            ORDER BY a.date
        """, (symbol, filing_period, filing_period))
        return " ".join(r[0] for r in cur.fetchall())
    

industry_keywords = {}

for industry, group in df.groupby("industry"):
    print(f"🔍 Industry: {industry}")
    texts = []
    changes = []

    for row in group.itertuples():
        sec_text = get_sec_text(row.symbol, row.filing_period, conn)
        news_text = get_news_text(row.symbol, row.filing_period, conn)
        full_text = sec_text + " " + news_text
        if full_text.strip():
            texts.append(full_text)
            changes.append(row.pct_change)

    tfidf = TfidfVectorizer(max_features=1000, stop_words='english')
    X = tfidf.fit_transform(texts).toarray()
    y = np.array(changes)

    model = LinearRegression().fit(X, y)
    terms = tfidf.get_feature_names_out()
    coefs = model.coef_

    top_pos = sorted([(t, c) for t, c in zip(terms, coefs) if c > 0], key=lambda x: -x[1])[:10]
    top_neg = sorted([(t, c) for t, c in zip(terms, coefs) if c < 0], key=lambda x: x[1])[:10]

    industry_keywords[industry] = {
        "top_positive": top_pos,
        "top_negative": top_neg
    }

with open("industry_keywords.json", "w") as f:
    json.dump(industry_keywords, f, indent=2)


