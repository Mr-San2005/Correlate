import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession

HDFS_URI = "hdfs://localhost:9000"


def test_silver_ops_events_keeps_every_real_event():
    spark = SparkSession.builder \
        .appName("test_silver_ops_events") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    bronze = spark.read.parquet(f"{HDFS_URI}/bronze/ops_events")
    silver = spark.read.parquet(f"{HDFS_URI}/silver/ops_events")

    real_events_in_bronze = bronze.select("event_id").distinct().count()

    # no event_id appears twice in silver
    assert silver.count() == silver.select("event_id").distinct().count()

    # and no real event was lost: silver has exactly one row per real event
    assert silver.count() == real_events_in_bronze

    # bronze had some duplicate rows, so silver must be smaller than bronze
    assert silver.count() < bronze.count()

    spark.stop()