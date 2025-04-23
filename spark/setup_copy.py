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

# ----------  Build the SparkSession  ----------
spark = (
    SparkSession.builder
        .appName("Financial-NLP-Ingest")
        # core cluster settings – tune for your cluster size
        .config("spark.executor.instances", "8")          # total executors
        .config("spark.executor.cores",     "4")          # CPU per executor
        .config("spark.executor.memory",    "8g")
        .config("spark.driver.memory",      "8g")
        # JDBC-specific defaults
        .config("spark.sql.adaptive.enabled", "true")     # AQE on
        .config("spark.sql.shuffle.partitions", "200")    # > executors × cores
        .config("spark.jars", "/Users/gabriel.yang/test/financial-testbed/postgresql-42.7.1.jar")
        .getOrCreate()
)

jdbc_url  = "jdbc:postgresql://localhost:5432/financial_db"
jdbc_opts = {
    "user": "postgres",
    "password": "",               # ← or env var
    "driver":  "org.postgresql.Driver",
    # batch/fetch tuning
    "fetchsize": "100000",        # rows per network round-trip
    "reWriteBatchedInserts": "true"
}

def copy_pg_table(src_table: str,
                  dst_table: str,
                  partition_col: str,
                  num_parts: int = 128):
    """
    High-throughput copy from PostgreSQL → Spark metastore (Parquet)
    """
    # 1️⃣  Get min/max once so we can parallel-slice safely
    lb, ub = (spark.read.format("jdbc")
                 .option("url", jdbc_url)
                 .option("query",
                         f"select min({partition_col}) as lb, "
                         f"max({partition_col}) as ub from {src_table}")
                 .options(**jdbc_opts).load().first())
    
    df = (spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", src_table)
            .option("partitionColumn", partition_col)
            .option("lowerBound",      str(lb))
            .option("upperBound",      str(ub))
            .option("numPartitions",   str(num_parts))
            .options(**jdbc_opts)
            .load()
            .repartition(num_parts, F.col(partition_col))     # even file sizes
          )

    # 2️⃣  Write out partitioned Parquet to avoid tiny files & speed later reads
    (df.write.mode("overwrite")
        .partitionBy("symbol")                     # or F.date_trunc('day', ts)
        .format("parquet")
        .saveAsTable(dst_table))


# Time each table copy operation
operation_times = {}

print("\n📊 Copying PostgreSQL tables to Spark:")
# operation_times["stock_ticks"] = timed_operation("Copy stock_ticks", copy_pg_table, "staging_stock_ticks", "ticks", partition_col="time")[1]
operation_times["profiles"] = timed_operation("Copy profiles", copy_pg_table, "profiles", "profiles", partition_col="symbol")[1]
# operation_times["historic_estimates"] = timed_operation("Copy historic_estimates", copy_pg_table, "historic_estimates", "historic_estimates", partition_col="symbol")[1]

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
print("\n🎉 Setup completed")
