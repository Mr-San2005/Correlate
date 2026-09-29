import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
import pytest

HDFS_URI = "hdfs://localhost:9000"


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder \
        .appName("test_gold") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()
    yield s
    s.stop()


def test_dim_date_covers_full_year(spark):
    df = spark.read.parquet(f"{HDFS_URI}/gold/dim_date")
    assert df.count() == 365


def test_all_fact_tables_exist_and_are_nonempty(spark):
    fact_tables = [
        "fact_sales_daily", "fact_traffic_daily", "fact_performance_daily",
        "fact_marketing_daily", "fact_customers_daily", "fact_ops_events_daily",
    ]
    for table in fact_tables:
        df = spark.read.parquet(f"{HDFS_URI}/gold/{table}")
        assert df.count() > 0, f"{table} is empty"


def test_fact_metric_daily_is_long_format(spark):
    df = spark.read.parquet(f"{HDFS_URI}/gold/fact_metric_daily")
    assert set(df.columns) == {"date", "metric_name", "value"}

    distinct_metrics = df.select("metric_name").distinct().count()
    # we expect at least: orders, revenue, traffic_visits, traffic_unique_visitors,
    # avg_response_time_ms, error_rate_pct, marketing_spend, new_customers,
    # churned_customers, plus at least one ops_events_* metric
    assert distinct_metrics >= 10


def test_revenue_anomaly_visible_in_long_format(spark):
    # sanity check: the planted incident should still be visible after
    # every reshape we've done since Phase 2
    df = spark.read.parquet(f"{HDFS_URI}/gold/fact_metric_daily")
    revenue = df.filter(df.metric_name == "revenue").orderBy("date").toPandas()

    anomaly_day_value = revenue.iloc[200]["value"]
    normal_avg = revenue.iloc[190:199]["value"].mean()

    assert anomaly_day_value < normal_avg * 0.85