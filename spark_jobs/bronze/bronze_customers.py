import os
os.environ["HADOOP_USER_NAME"] = "root"

from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType

HDFS_URI = "hdfs://localhost:9000"
INGESTION_DATE = date.today().isoformat()

RAW_PATH = f"{HDFS_URI}/raw/customers/ingestion_date={INGESTION_DATE}/customers_daily.csv"
BRONZE_PATH = f"{HDFS_URI}/bronze/customers"

schema = StructType([
    StructField("date", DateType(), True),
    StructField("new_customers", IntegerType(), True),
    StructField("churned_customers", IntegerType(), True),
    StructField("ingestion_date", DateType(), True),
])


def main():
    spark = SparkSession.builder \
        .appName("bronze_customers") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    df = spark.read.csv(RAW_PATH, header=True, schema=schema)

    print(f"read {df.count()} rows from raw customers")
    df.printSchema()

    df.write \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .parquet(BRONZE_PATH)

    print(f"wrote customers bronze data -> {BRONZE_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()