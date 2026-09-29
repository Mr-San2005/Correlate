import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession

HDFS_URI = "hdfs://localhost:9000"

# silver source name -> (gold fact table name, columns to keep)
FACT_DEFINITIONS = {
    "sales": ("fact_sales_daily", ["orders", "revenue"]),
    "web_traffic": ("fact_traffic_daily", ["visits", "unique_visitors"]),
    "app_performance": ("fact_performance_daily", ["avg_response_time_ms", "error_rate_pct"]),
    "marketing": ("fact_marketing_daily", ["spend"]),
    "customers": ("fact_customers_daily", ["new_customers", "churned_customers"]),
}


def build_fact(spark, silver_name, fact_name, value_cols):
    silver_path = f"{HDFS_URI}/silver/{silver_name}"
    gold_path = f"{HDFS_URI}/gold/{fact_name}"

    df = spark.read.parquet(silver_path)
    df = df.select(["date"] + value_cols)  # keep only date + the real measurements

    df.write.mode("overwrite").parquet(gold_path)
    print(f"wrote {df.count()} rows -> {gold_path}")


def build_ops_events_fact(spark):
    # ops_events is different: multiple rows per day, not one.
    # the gold fact here is a daily COUNT per event type.
    silver_path = f"{HDFS_URI}/silver/ops_events"
    gold_path = f"{HDFS_URI}/gold/fact_ops_events_daily"

    df = spark.read.parquet(silver_path)
    daily_counts = df.groupBy("date", "event_type").sum("event_count") \
        .withColumnRenamed("sum(event_count)", "event_count")

    daily_counts.write.mode("overwrite").parquet(gold_path)
    print(f"wrote {daily_counts.count()} rows -> {gold_path}")


def main():
    spark = SparkSession.builder \
        .appName("gold_facts") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    for silver_name, (fact_name, value_cols) in FACT_DEFINITIONS.items():
        build_fact(spark, silver_name, fact_name, value_cols)

    build_ops_events_fact(spark)

    spark.stop()


if __name__ == "__main__":
    main()