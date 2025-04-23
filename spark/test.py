from pyspark.sql import SparkSession
import time

# Create Spark session
spark = SparkSession.builder \
    .appName("BenchmarkSQL") \
    .getOrCreate()
df_ticks = spark.read.parquet("/Users/gabriel.yang/test/financial-testbed/spark-warehouse/ticks")
df_ticks.createOrReplaceTempView("ticks")

# Define the SQL query (you can format this as multi-line string)
query = """
WITH tick_data AS (
    SELECT
        st.symbol,
        CAST(st.time AS DATE) AS trade_date,
        MIN(st.open) AS open_price,
        MAX(st.high) AS high_price,
        MIN(st.low) AS low_price,
        MAX(st.close) AS close_price,
        SUM(st.volume) AS total_volume
    FROM ticks st
    WHERE st.symbol IN ('AAPL', 'MSFT', 'NVDA')
      AND CAST(st.time AS DATE) BETWEEN DATE('2020-03-25') AND DATE('2020-04-01')
    GROUP BY st.symbol, CAST(st.time AS DATE)
),

avg_volume AS (
    SELECT
        symbol,
        AVG(total_volume) AS avg_weekly_volume
    FROM tick_data
    GROUP BY symbol
)

SELECT
    t.symbol,
    t.trade_date,
    DAYOFWEEK(t.trade_date) AS weekday,  -- Spark: 1=Sunday, 7=Saturday
    t.open_price,
    t.high_price,
    t.low_price,
    t.close_price,
    t.total_volume,
    a.avg_weekly_volume,
    CASE 
        WHEN t.total_volume > a.avg_weekly_volume * 1.5 THEN TRUE
        ELSE FALSE
    END AS is_volume_spike
FROM tick_data t
JOIN avg_volume a ON t.symbol = a.symbol
ORDER BY t.symbol, t.trade_date


"""

# Run the query 200 times
executions = 200
total_time = 0

for i in range(executions):
    start_time = time.time()
    spark.sql(query).collect()  # collect() forces execution
    elapsed = (time.time() - start_time) * 1000  # in ms
    total_time += elapsed

average_time = total_time / executions
print(f"Spark SQL Total Execution Time: {total_time:.2f} ms")
print(f"✅ Average Spark SQL Execution Time: {average_time:.2f} ms over {executions} runs")
