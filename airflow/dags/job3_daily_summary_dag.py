from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.job3_analytics import daily_analytics

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='job3_daily_summary',
    default_args=default_args,
    schedule_interval='@daily',  
    start_date=datetime(2025, 12, 16),
    catchup=False
) as dag:

    analytics_task = PythonOperator(
        task_id='compute_daily_metrics',
        python_callable=daily_analytics,
    )