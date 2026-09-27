import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/web_traffic"
SILVER_PATH = f"{HDFS_URI}/silver/web_traffic"


def main():
    spark = SparkSession.builder \
        .appName("silver_traffic") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze web_traffic")

    df = df.dropDuplicates(["date"])
    print(f"{df.count()} rows after deduplication")

    # counts - flag + fill with 0
    df = df.withColumn("visits_was_missing", F.col("visits").isNull())
    df = df.withColumn("unique_visitors_was_missing", F.col("unique_visitors").isNull())
    df = df.fillna({"visits": 0, "unique_visitors": 0})

    df = df.drop("ingestion_date")

    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote web_traffic silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()