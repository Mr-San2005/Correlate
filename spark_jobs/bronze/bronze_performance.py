import os
os.environ["HADOOP_USER_NAME"] = "root"

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, DateType

HDFS_URI = "hdfs://localhost:9000"
INGESTION_DATE = date.today().isoformat()

RAW_PATH = f"{HDFS_URI}/raw/app_performance/ingestion_date={INGESTION_DATE}/app_performance_daily.csv"
BRONZE_PATH = f"{HDFS_URI}/bronze/app_performance"

schema = StructType([
    StructField("date", DateType(), True),
    StructField("avg_response_time_ms", DoubleType(), True),
    StructField("error_rate_pct", DoubleType(), True),
    StructField("ingestion_date", DateType(), True),
])


def main():
    spark = SparkSession.builder \
        .appName("bronze_performance") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.csv(RAW_PATH, header=True, schema=schema)

    print(f"read {df.count()} rows from raw app_performance")
    df.printSchema()

    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(BRONZE_PATH)

    print(f"wrote app_performance bronze data -> {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()