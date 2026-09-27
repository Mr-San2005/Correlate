import os
os.environ["HADOOP_USER_NAME"] = "root"

from datetime import date
from pyspark.sql import SparkSession

HDFS_URI = "hdfs://localhost:9000"
INGESTION_DATE = date.today().isoformat()
BRONZE_PATH = f"{HDFS_URI}/bronze/sales"


def test_bronze_sales_readable_and_typed():
    spark = SparkSession.builder \
        .appName("test_bronze_sales") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.parquet(BRONZE_PATH)

    assert df.count() > 0

    schema_fields = {f.name: f.dataType.typeName() for f in df.schema.fields}
    assert schema_fields["orders"] == "integer"
    assert schema_fields["revenue"] == "double"

    spark.stop()