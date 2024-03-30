# aws s3 cp /home/hadoop/adopt_koreanair/dags/airflow_get_delta_data.py s3://airflow-ken/dags/airflow_get_delta_data.py

from os import path
from datetime import datetime, timedelta
import airflow  
from airflow import DAG  
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

  
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2023, 3, 29, 23, 20),
    # 'start_date': datetime(2023, 3, 29, 14, 20),
    'start_date': datetime(2023, 3, 29),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
    # 'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'get_delta_data',
    default_args=default_args,
    description='A DAG that runs every minute',
    # schedule_interval=timedelta(minutes=10),
    schedule_interval = '*/10 * * * *',
    catchup=False  # 이전 미실행된 스케줄 실행 안함
)

SPARK_TEST_STEPS1 = [
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '/home/hadoop/adopt_koreanair/get_delta_data.py',
                "{{ ts }}"  # execution_date를 YYYY-MM-DD HH:MM:SS 형식으로 전달
            ]
        }
    }
]

SPARK_TEST_STEPS2 = [
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '/home/hadoop/adopt_koreanair/merge_data.py',
                "{{ ts }}"  # execution_date를 YYYY-MM-DD HH:MM:SS 형식으로 전달
            ]
        }
    }
]



step_adder1 = EmrAddStepsOperator(
    task_id='get_delta_data',
    job_flow_id="j-UDAFCZUT2MS",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS1,
    dag=dag
)

step_checker1 = EmrStepSensor(
    task_id='get_delta_data_watch',
    job_flow_id="j-UDAFCZUT2MS",
    step_id="{{ task_instance.xcom_pull('get_delta_data', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

glue_crawler_config = {
        "Name": "koreanair_delta_data",
        "Role": 'arn:aws:iam::531744930393:role/AnalyticsworkshopGlueRole',
        "DatabaseName": "koreanair",
        "Targets": {"S3Targets": [{"Path": "s3://chiholee-tmp/adopt-koreanair/delta_data/"}]},
    }
    
delta_data_crawler = GlueCrawlerOperator(
    task_id="delta_data_crawler",
    config=glue_crawler_config,
    dag=dag)

step_adder2 = EmrAddStepsOperator(
    task_id='merge_delta_data',
    job_flow_id="j-UDAFCZUT2MS",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS2,
    dag=dag
)

step_checker2 = EmrStepSensor(
    task_id='merge_delta_data_watch',
    job_flow_id="j-UDAFCZUT2MS",
    step_id="{{ task_instance.xcom_pull('merge_delta_data', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

step_adder1 >> step_checker1 >> delta_data_crawler >> step_adder2 >> step_checker2
# step_adder

