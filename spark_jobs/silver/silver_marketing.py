import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/marketing"
SILVER_PATH = f"{HDFS_URI}/silver/marketing"


def main():
    spark = SparkSession.builder \
        .appName("silver_marketing") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze marketing")

    df = df.dropDuplicates(["date"])
    print(f"{df.count()} rows after deduplication")

    # spend is rate-like (a real ongoing amount) - fill with its own average
    avg_spend = df.select(F.avg("spend")).first()[0]
    df = df.withColumn("spend_was_missing", F.col("spend").isNull())
    df = df.fillna({"spend": avg_spend})

    df = df.drop("ingestion_date")

    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote marketing silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()