from os import path
from datetime import timedelta  
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

# S3_BUCKET_NAME = "chiholee-tmp"
# GLUE_ROLE_ARN = "arn:aws:iam::531744930393:role/AnalyticsworkshopGlueRole"

dag_name = 'data-pipeline'
# Unique identifier for the DAG
correlation_id = "{{ run_id }}"
  
default_args = {  
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(  
    dag_name,
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)

SPARK_TEST_STEPS = [
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                        '/home/hadoop/adopt_koreanair/test.py']
            }
        }
    ]

# cluster_creator = EmrCreateJobFlowOperator(
#     task_id='create_emr_cluster',
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id='aws_default',
#     emr_conn_id='emr_default',
#     dag=dag
# )

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="j-2Z3P3QIM6TYTH",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker1 = EmrStepSensor(
    task_id='watch_step1',
    job_flow_id="j-2Z3P3QIM6TYTH",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)


step_adder >> step_checker1

