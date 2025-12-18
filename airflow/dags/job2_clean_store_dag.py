from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.job2_cleaner import data_cleaning
from src.db_utils import sql_write
import logging


def run_clean_and_store():
    db_name = '/opt/airflow/data/weather_event.db'
    
    df = data_cleaning()
    
    if not df.empty:
        sql_write(df,db_name)
    else:
        print("No new data to process this hour.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'job2_clean_store',
    default_args = default_args,
    schedule_interval = "@hourly",
    start_date = datetime(2025,12,16),
    catchup=False,
    max_active_runs=1
) as dag:
    
    process_data = PythonOperator(
        task_id = "process_data_task",
        python_callable = run_clean_and_store
    )