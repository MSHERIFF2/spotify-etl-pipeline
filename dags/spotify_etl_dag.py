from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add scripts to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import run_extraction
from scripts.transform_load import run_transform_load

default_args = {
    'owner': 'sheriffdeen',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='Spotify ETL using PySpark and Airflow',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=run_extraction,
    )

    transform_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=run_transform_load,
    )

    extract_task >> transform_load_task