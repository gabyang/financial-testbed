from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
import time
from datetime import datetime

# Start timing the entire process
overall_start_time = time.time()
print(f"🕒 Setup started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Helper function to time operations
def timed_operation(operation_name, func, *args, **kwargs):
    start_time = time.time()
    print(f"⏳ Starting: {operation_name}")
    result = func(*args, **kwargs)
    elapsed = time.time() - start_time
    print(f"✅ Completed: {operation_name} in {elapsed:.2f} seconds")
    return result, elapsed

# Create the Spark session
spark_start = time.time()
spark = (SparkSession.builder
         .appName("Financial-NLP-Streaming")
         .config("spark.jars", "/Users/gabriel.yang/test/financial-testbed/postgresql-42.7.1.jar")
         .config("spark.executor.memory", "6g")
          .config("spark.driver.memory", "6g")
          .config("spark.memory.fraction", "0.6")
          .config("spark.memory.storageFraction", "0.3")
          .config("spark.sql.shuffle.partitions", "64")
         .enableHiveSupport()
         .getOrCreate())
spark_elapsed = time.time() - spark_start
print(f"✅ Spark session created in {spark_elapsed:.2f} seconds")

jdbc_url = "jdbc:postgresql://localhost:5432/financial_db"
jdbc_opts = {"user": "postgres", "password": "", "driver": "org.postgresql.Driver"}

def copy_pg_table(src_table: str, dst_table: str):
    read_start = time.time()
    df = (spark.read.format("jdbc")
         .option("url", jdbc_url)
         .option("dbtable", src_table)
         .options(**jdbc_opts)
         .load())
    read_elapsed = time.time() - read_start
    print(f"  - Read {src_table}: {read_elapsed:.2f} seconds ({df.count()} rows)")
    
    write_start = time.time()
    df.write.mode("overwrite").format("parquet").saveAsTable(dst_table)
    write_elapsed = time.time() - write_start
    print(f"  - Write {dst_table}: {write_elapsed:.2f} seconds")
    
    return read_elapsed + write_elapsed

# Time each table copy operation
operation_times = {}

print("\n📊 Copying PostgreSQL tables to Spark:")
operation_times["stock_ticks"] = timed_operation("Copy stock_ticks", copy_pg_table, "staging_stock_ticks", "ticks")[1]
operation_times["profiles"] = timed_operation("Copy profiles", copy_pg_table, "profiles", "profiles")[1]
operation_times["historic_estimates"] = timed_operation("Copy historic_estimates", copy_pg_table, "historic_estimates", "historic_estimates")[1]

# Define schema for SEC data
sec_schema = StructType([
    StructField("symbol", StringType()),
    StructField("start_date", DateType()),
    StructField("filing_period", DateType()),
    StructField("eps_basic", DoubleType()),
    StructField("eps_diluted", DoubleType()),
    StructField("eps_basic9", DoubleType()),
    StructField("eps_diluted9", DoubleType()),
])

# Time the SEC CSV loading operation
print("\n📄 Loading SEC CSV data:")
sec_start = time.time()
sec_df = spark.read.option("header", True).schema(sec_schema).csv("sec_extracts.csv")
row_count = sec_df.count()
sec_read_elapsed = time.time() - sec_start
print(f"  - Read CSV: {sec_read_elapsed:.2f} seconds ({row_count} rows)")

sec_write_start = time.time()
sec_df.write.mode("overwrite").format("parquet").saveAsTable("sec_eps_temp")
sec_write_elapsed = time.time() - sec_write_start
print(f"  - Write Parquet: {sec_write_elapsed:.2f} seconds")

operation_times["sec_csv"] = sec_read_elapsed + sec_write_elapsed

# Calculate overall execution time
overall_elapsed = time.time() - overall_start_time

# Print summary statistics
print("\n📋 Execution Summary:")
print(f"Start time: {datetime.fromtimestamp(overall_start_time).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total execution time: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
print("\nOperation times:")
for op, duration in operation_times.items():
    print(f"  - {op}: {duration:.2f} seconds ({(duration/overall_elapsed)*100:.1f}% of total)")

# Stop the Spark session
spark.stop()
print("\n�� Setup completed")
