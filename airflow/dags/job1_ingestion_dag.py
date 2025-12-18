from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.job1_producer import job_producer
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id = 'job1_ingestion',
    default_args = default_args,
    schedule_interval='*/5 * * * *',
    start_date = datetime(2025,12,16),
    catchup=False,
    max_active_runs=1
) as dag:
    
    producer_task = PythonOperator(
        task_id = "job_producer_task",
        python_callable = job_producer
    )