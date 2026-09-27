import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
BRONZE_PATH = f"{HDFS_URI}/bronze/customers"
SILVER_PATH = f"{HDFS_URI}/silver/customers"


def main():
    spark = SparkSession.builder \
        .appName("silver_customers") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)
    print(f"read {df.count()} rows from bronze customers")

    df = df.dropDuplicates(["date"])
    print(f"{df.count()} rows after deduplication")

    # counts - flag + fill with 0
    df = df.withColumn("new_customers_was_missing", F.col("new_customers").isNull())
    df = df.withColumn("churned_customers_was_missing", F.col("churned_customers").isNull())
    df = df.fillna({"new_customers": 0, "churned_customers": 0})

    df = df.drop("ingestion_date")

    df.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(SILVER_PATH)

    print(f"wrote customers silver data -> {SILVER_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()