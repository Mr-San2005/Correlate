import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession

HDFS_URI = "hdfs://localhost:9000"
SILVER_PATH = f"{HDFS_URI}/silver/sales"


def test_silver_sales_no_duplicates_and_no_nulls():
    spark = SparkSession.builder \
        .appName("test_silver_sales") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(SILVER_PATH)

    total_rows = df.count()
    distinct_dates = df.select("date").distinct().count()
    assert total_rows == distinct_dates, "found duplicate dates in silver sales"

    null_orders = df.filter(df.orders.isNull()).count()
    null_revenue = df.filter(df.revenue.isNull()).count()
    assert null_orders == 0
    assert null_revenue == 0

    assert "ingestion_date" not in df.columns

    spark.stop()