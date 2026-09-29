import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_URI = "hdfs://localhost:9000"
GOLD_PATH = f"{HDFS_URI}/gold/fact_metric_daily"

# fact table -> {source column name: clean metric name}
METRIC_SOURCES = {
    "fact_sales_daily": {"orders": "orders", "revenue": "revenue"},
    "fact_traffic_daily": {"visits": "traffic_visits", "unique_visitors": "traffic_unique_visitors"},
    "fact_performance_daily": {"avg_response_time_ms": "avg_response_time_ms", "error_rate_pct": "error_rate_pct"},
    "fact_marketing_daily": {"spend": "marketing_spend"},
    "fact_customers_daily": {"new_customers": "new_customers", "churned_customers": "churned_customers"},
}


def main():
    spark = SparkSession.builder \
        .appName("gold_fact_metric_daily") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    long_frames = []

    for fact_table, col_map in METRIC_SOURCES.items():
        df = spark.read.parquet(f"{HDFS_URI}/gold/{fact_table}")
        for source_col, metric_name in col_map.items():
            piece = df.select(
                "date",
                F.lit(metric_name).alias("metric_name"),
                F.col(source_col).cast("double").alias("value"),
            )
            long_frames.append(piece)

    # ops_events is shaped differently (has event_type instead of fixed columns)
    # treat each event type as its own metric, e.g. "ops_events_deploy"
    ops = spark.read.parquet(f"{HDFS_URI}/gold/fact_ops_events_daily")
    ops_long = ops.select(
        "date",
        F.concat(F.lit("ops_events_"), F.col("event_type")).alias("metric_name"),
        F.col("event_count").cast("double").alias("value"),
    )
    long_frames.append(ops_long)

    result = long_frames[0]
    for piece in long_frames[1:]:
        result = result.unionByName(piece)

    result.write.mode("overwrite").parquet(GOLD_PATH)
    print(f"wrote {result.count()} rows -> {GOLD_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()