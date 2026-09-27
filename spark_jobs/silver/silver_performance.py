import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/app_performance"
SILVER_PATH = f"{HDFS_URI}/silver/app_performance"


def main():
    spark = SparkSession.builder \
        .appName("silver_performance") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze app_performance")

    df = df.dropDuplicates(["date"])
    print(f"{df.count()} rows after deduplication")

    # rate-like values - flag + fill with the column's own average, not 0
    avg_response = df.select(F.avg("avg_response_time_ms")).first()[0]
    avg_error = df.select(F.avg("error_rate_pct")).first()[0]

    df = df.withColumn("avg_response_time_ms_was_missing", F.col("avg_response_time_ms").isNull())
    df = df.withColumn("error_rate_pct_was_missing", F.col("error_rate_pct").isNull())
    df = df.fillna({"avg_response_time_ms": avg_response, "error_rate_pct": avg_error})

    df = df.drop("ingestion_date")

    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote app_performance silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()