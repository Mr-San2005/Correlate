import os
os.environ["HADOOP_USER_NAME"] = "root"

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, DateType
)

HDFS_URI = "hdfs://localhost:9000"
INGESTION_DATE = date.today().isoformat()

RAW_PATH = f"{HDFS_URI}/raw/sales/ingestion_date={INGESTION_DATE}/sales_daily.csv"
BRONZE_PATH = f"{HDFS_URI}/bronze/sales"

schema = StructType([
    StructField("date", DateType(), True),
    StructField("orders", IntegerType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("ingestion_date", DateType(), True),
])


def main():
    spark = SparkSession.builder \
        .appName("bronze_sales") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.csv(RAW_PATH, header=True, schema=schema)

    print(f"read {df.count()} rows from raw sales")
    df.printSchema()

    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(BRONZE_PATH)

    print(f"wrote sales bronze data -> {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()