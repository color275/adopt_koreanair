# spark-submit get_delta_data.py

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, LongType
from datetime import datetime, timedelta
import pytz
import sys

if len(sys.argv) > 1:
    execution_date_str = sys.argv[1]
    # ISO 8601 형식의 문자열 처리: 타임존 정보를 제거
    execution_date_str, tz_str = execution_date_str.split("+")  # 타임존 분리
    execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S.%f')

    # UTC 타임존으로 설정 (ISO 8601 문자열은 UTC 기준임)
    utc_zone = pytz.timezone('UTC')
    execution_date = utc_zone.localize(execution_date)

else:
    # 인자가 제공되지 않았을 경우 현재 날짜와 시간 사용 (UTC로 가정)
    execution_date = datetime.now(pytz.timezone('UTC'))

# 한국 시간(KST)으로 변환
korea_timezone = pytz.timezone('Asia/Seoul')
kst_execution_date = execution_date.astimezone(korea_timezone)

print("UTC Execution Date:", execution_date)
print("KST Execution Date:", kst_execution_date)

# PySpark 세션 설정
jar_files = []
hadoop_config = {}

spark = SparkSession.builder.appName("TEST") \
    .config("spark.jars", ",".join(jar_files))

for key, value in hadoop_config.items():
    spark = spark.config(f"{key}", value)

spark = spark.getOrCreate()

# PostgreSQL 접속 설정
postgre_url = "jdbc:postgresql://ecommerce-postgresql.cluster-cgkgybnzurln.ap-northeast-2.rds.amazonaws.com:5432/postgres"
user = "appuser"
password = "Appuser12#$"
postgre_driver = "org.postgresql.Driver"

# Airflow에서 제공되는 execution_date 사용 (실제 환경에서는 Airflow 변수에서 가져옴)
# execution_date_str = '{{ execution_date }}'  # Airflow Template Variable
# execution_date = datetime.strptime(execution_date_str, '%Y-%m-%dT%H:%M:%S%z')

# 시간 범위 설정
start_time = kst_execution_date
end_time = start_time + timedelta(days=1)  # 다음 실행 시간, 일일 배치 처리 가정

print("# start_time : ", start_time)
print("# end_time : ", end_time)

# SQL 쿼리 수정: last_update_time이 timestamp 타입이므로 직접 비교
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

# 한국 시간대 설정 및 현재 시간 계산
korea_timezone = pytz.timezone('Asia/Seoul')
current_time_kr = datetime.now(korea_timezone)
filename = current_time_kr.strftime('%Y-%m-%d-%H-%M-%S')

# S3 저장 경로 설정
s3_output_path = f"s3://chiholee-tmp/adopt-koreanair/delta_data2/{filename}/"

# 데이터를 S3에 parquet 형태로 저장
postgresql_df.write.parquet(s3_output_path)

# Spark 세션 종료
spark.stop()

