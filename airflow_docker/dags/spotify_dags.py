from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from run_spotify_etl import call_spotify_etl
from save_to_db import call_save_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'spotify_dag',
    default_args=default_args,
    description='DAG with Spotify ETL process!',
    schedule_interval='@daily',
)

task_1 = PythonOperator(
    task_id='run_spotify_etl',
    python_callable = call_spotify_etl,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='save_to_db',
    python_callable = call_save_to_db,
    dag=dag,
)

task_1 >> task_2