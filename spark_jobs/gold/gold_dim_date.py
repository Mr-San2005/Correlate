import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
GOLD_PATH = f"{HDFS_URI}/gold/dim_date"

START_DATE = "2025-01-01"
END_DATE = "2025-12-31"


def main():
    spark = SparkSession.builder \
        .appName("gold_dim_date") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    # generate every date in our range - this dimension isn't read from
    # any source, it's pure calendar logic
    df = spark.sql(f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS date")

    df = df.withColumn("year", F.year("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("quarter", F.quarter("date")) \
        .withColumn("day_of_week", F.dayofweek("date")) \
        .withColumn("day_name", F.date_format("date", "EEEE")) \
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))  # Spark: 1=Sunday, 7=Saturday

    df.write.mode("overwrite").parquet(GOLD_PATH)

    print(f"wrote {df.count()} rows -> {GOLD_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()