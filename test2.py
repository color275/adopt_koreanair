import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pytz
import sys

spark = SparkSession.builder.appName("merge") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.demo.warehouse", "s3://chiholee-tmp/adopt-koreanair/iceberg_test/") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS demo.koreanair")

delta_data_location = "s3://chiholee-tmp/adopt-koreanair/delta_data/"
delta_data = spark.read.parquet(delta_data_location)

delta_data.createOrReplaceTempView("delta_data")

# print(delta_data.show())

delta_data = spark.sql("""
select *
FROM delta_data
""")

print(delta_data.show())

# Spark 세션 종료
spark.stop()
