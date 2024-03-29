import pyspark
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from pyspark import SparkConf
from datetime import datetime
import pytz


jar_files = [    
]

hadoop_config = {
}

spark = SparkSession.builder.appName("TEST")
spark = spark.config("spark.jars", ",".join(jar_files))

for key, value in hadoop_config.items():
    spark = spark.config(f"{key}", value)

spark = spark.getOrCreate()

postgre_url = "jdbc:postgresql://ecommerce-postgresql.cluster-cgkgybnzurln.ap-northeast-2.rds.amazonaws.com:5432/postgres"
user = "appuser"
password = "Appuser12#$"
postgre_driver = "org.postgresql.Driver"

execution_date = airflow's execution_date

sql = f"""
select * 
from delta_data
where last_update_time > {execution_date}
"""

postgresql_df = spark.read.format("jdbc") \
          .option("url",postgre_url) \
          .option("driver",postgre_driver) \
          .option("user",user) \
          .option("password",password) \
          .option("query", sql) \
          .load()


korea_timezone = pytz.timezone('Asia/Seoul')
current_time_kr = datetime.now(korea_timezone)
filename = current_time_kr.strftime('%Y-%m-%d-%H-%M-%S')


# S3 저장 설정
s3_output_path = f"s3://chiholee-tmp/adopt-koreanair/{filename}/"

# 데이터를 S3에 parquet 형태로 저장
postgresql_df.write.parquet(s3_output_path)

# Spark 세션 종료
spark.stop()


