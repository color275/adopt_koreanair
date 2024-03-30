import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import pytz
import sys
from pyspark.sql.functions import *

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

spark = SparkSession.builder.appName("MERGE_DATA") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.demo.warehouse", "s3://chiholee-tmp/adopt-koreanair/iceberg_test/") \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS demo.koreanair")

delta_data_location = "s3://chiholee-tmp/adopt-koreanair/delta_data/"
delta_data = spark.read.parquet(delta_data_location)
delta_data.createOrReplaceTempView("delta_data")

start_time = kst_execution_date
# start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
year = start_time.year
month = start_time.month
day = start_time.day
hour = start_time.hour
minute = start_time.minute
minute = minute // 10 * 10

print("# year : ",year)
print("# month : ",month)
print("# day : ",day)
print("# hour : ",hour)
print("# minute : ",minute)
# end_time = start_time + timedelta(minutes=10)

delta_data_iu_sql = f"""
select seq, order_no, product_no, order_price, last_update_time
FROM delta_data
where year = '{year}'
  and month = '{month}'
  and day = '{day}'
  and hour = '{hour}'
  and minute ='{minute}'
  and op_code in ('I','U')
"""

print("# SQL : ", delta_data_iu_sql)

insert_update_df = spark.sql(delta_data_iu_sql)

insert_update_df.createOrReplaceTempView("insert_update_df")

merge_sql = f"""
        MERGE INTO demo.koreanair.merge_data t    
        USING insert_update_df s
        ON  t.seq = s.seq
        and t.order_no = s.order_no
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

spark.sql(merge_sql)

spark.sql("""
select *
from demo.koreanair.merge_data
""").show(100, False)






# Spark 세션 종료
spark.stop()
