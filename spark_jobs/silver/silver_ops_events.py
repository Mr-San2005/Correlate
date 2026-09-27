import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/ops_events"
SILVER_PATH = f"{HDFS_URI}/silver/ops_events"


def main():
    spark = SparkSession.builder \
        .appName("silver_ops_events") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze ops_events")

    # here, a duplicate means an identical event row - not one-per-date like the others
    df = df.dropDuplicates(["date", "event_type", "event_count"])
    print(f"{df.count()} rows after deduplication")

    # an unknown event type is a real gap in what we know - label it, don't guess
    df = df.withColumn(
        "event_type",
        F.when(F.col("event_type").isNull(), "unknown").otherwise(F.col("event_type"))
    )
    df = df.fillna({"event_count": 0})

    df = df.drop("ingestion_date")

    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote ops_events silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()