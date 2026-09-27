import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/sales"
SILVER_PATH = f"{HDFS_URI}/silver/sales"


def main():
    spark = SparkSession.builder \
        .appName("silver_sales") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze sales")

    # 1. remove exact duplicates on the real business key: one row per date
    df = df.dropDuplicates(["date"])
    print(f"{df.count()} rows after deduplication")

    # 2. flag + fill missing counts (orders) with 0
    df = df.withColumn("orders_was_missing", F.col("orders").isNull())
    df = df.fillna({"orders": 0})

    # 3. flag + fill missing rate-like values (revenue) with the column's own average
    avg_revenue = df.select(F.avg("revenue")).first()[0]
    df = df.withColumn("revenue_was_missing", F.col("revenue").isNull())
    df = df.fillna({"revenue": avg_revenue})

    # 4. drop the ingestion_date column - Silver is organized by real event date now
    df = df.drop("ingestion_date")

    # 5. write Silver, partitioned by the REAL event date
    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote sales silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()