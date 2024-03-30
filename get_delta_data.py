import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pytz
import sys

# 실행 인자에서 시분초까지 포함한 execution_date 받기
if len(sys.argv) > 1:
    execution_date_str = sys.argv[1]
    # 2024-03-30T01:52:16.543763+00:00
    # 2024-03-30T01:50:00+00:00
    print("# execution_date_str : ", execution_date_str)
    execution_date_str = execution_date_str.split("+")[0]
    if "." in execution_date_str:
        execution_date_str = execution_date_str.split(".")[0]
    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S')
else:
    execution_date = datetime.now()

# UTC 타임존으로 설정 후 한국 시간(KST)으로 변환
utc_zone = pytz.timezone('UTC')
execution_date = utc_zone.localize(execution_date)
korea_timezone = pytz.timezone('Asia/Seoul')
kst_execution_date = execution_date.astimezone(korea_timezone)

# PySpark 세션 설정
spark = SparkSession.builder.appName("GET_DELTA_DATA") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# PostgreSQL 접속 설정
postgre_url = "jdbc:postgresql://ecommerce-postgresql.cluster-cgkgybnzurln.ap-northeast-2.rds.amazonaws.com:5432/postgres"
user = "appuser"
password = "Appuser12#$"
postgre_driver = "org.postgresql.Driver"

start_time = kst_execution_date
end_time = start_time + timedelta(minutes=10)

# SQL 쿼리
sql = f"""
select * 
from delta_data
where last_update_time >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
  and last_update_time < '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
"""

print("# sql : ", sql)

postgresql_df = spark.read.format("jdbc") \
    .option("url", postgre_url) \
    .option("driver", postgre_driver) \
    .option("user", user) \
    .option("password", password) \
    .option("query", sql) \
    .load()

# 'last_update_time' 컬럼을 사용하여 연, 월, 일, 시, 분 컬럼을 추가
df_with_partitions = postgresql_df.withColumn("year", year("last_update_time")) \
    .withColumn("month", month("last_update_time")) \
    .withColumn("day", dayofmonth("last_update_time")) \
    .withColumn("hour", hour("last_update_time")) \
    .withColumn("minute", (minute("last_update_time") / 10).cast("int") * 10)

# S3 저장 경로 설정
s3_output_path = "s3://chiholee-tmp/adopt-koreanair/delta_data/"

# 데이터를 10분 단위 파티션으로 S3에 parquet 형태로 저장
df_with_partitions.write.partitionBy("year", "month", "day", "hour", "minute") \
    .mode("overwrite") \
    .parquet(s3_output_path)

# Spark 세션 종료
spark.stop()
