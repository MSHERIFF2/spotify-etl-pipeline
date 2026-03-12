import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Force the path to your project
PROJECT_ROOT = "/workspaces/spotify-etl-pipeline"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# 2. Explicitly import. If these fail, check if the function names 
# in extract.py and transform_load.py match exactly!
from scripts.extract import run_extraction
from scripts.transform_load import run_transform_load

default_args = {
    'owner': 'Sheriffdeen',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='A Spotify Data Pipeline using PySpark and Star Schema',
    schedule_interval='@daily',
    catchup=False,
    tags=['pyspark', 'data_engineering'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_raw_data',
        python_callable=run_extraction,
    )

    transform_load_task = PythonOperator(
        task_id='pyspark_transform_load',
        python_callable=run_transform_load,
    )

    extract_task >> transform_load_task