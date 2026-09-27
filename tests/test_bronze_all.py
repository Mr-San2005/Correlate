import os
os.environ["HADOOP_USER_NAME"] = "root"

from datetime import date
from pyspark.sql import SparkSession
import pytest

HDFS_URI = "hdfs://localhost:9000"
INGESTION_DATE = date.today().isoformat()

TABLES = {
    "sales": {"orders": "integer", "revenue": "double"},
    "web_traffic": {"visits": "integer", "unique_visitors": "integer"},
    "app_performance": {"avg_response_time_ms": "double", "error_rate_pct": "double"},
    "marketing": {"spend": "double"},
    "customers": {"new_customers": "integer", "churned_customers": "integer"},
    "ops_events": {"event_type": "string", "event_count": "integer"},
}


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder \
        .appName("test_bronze_all") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    yield s
    s.stop()


@pytest.mark.parametrize("table_name,expected_types", TABLES.items())
def test_bronze_table(spark, table_name, expected_types):
    path = f"{HDFS_URI}/bronze/{table_name}"
    df = spark.read.parquet(path)

    assert df.count() > 0, f"{table_name} bronze table is empty"

    schema_fields = {f.name: f.dataType.typeName() for f in df.schema.fields}
    for col, expected_type in expected_types.items():
        assert schema_fields[col] == expected_type, (
            f"{table_name}.{col} expected {expected_type}, got {schema_fields.get(col)}"
        )