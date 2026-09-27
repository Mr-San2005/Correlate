import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
import pytest

HDFS_URI = "hdfs://localhost:9000"

# table_name -> the column(s) that should never be null after cleaning
TABLES = {
    "sales": ["orders", "revenue"],
    "web_traffic": ["visits", "unique_visitors"],
    "app_performance": ["avg_response_time_ms", "error_rate_pct"],
    "marketing": ["spend"],
    "customers": ["new_customers", "churned_customers"],
    "ops_events": ["event_type", "event_count"],
}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder \
        .appName("test_silver_all") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    yield s
    s.stop()


@pytest.mark.parametrize("table_name,required_cols", TABLES.items())
def test_silver_table_has_no_nulls(spark, table_name, required_cols):
    path = f"{HDFS_URI}/silver/{table_name}"
    df = spark.read.parquet(path)

    assert df.count() > 0, f"{table_name} silver table is empty"
    assert "ingestion_date" not in df.columns, f"{table_name} still has ingestion_date"

    for col in required_cols:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, f"{table_name}.{col} still has {null_count} nulls"