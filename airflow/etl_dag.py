from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extraction import data_extraction
from transforming import transform_tripdata
from src.load import load_data
from pathlib import Path

OUTPUT_PATH = Path("/airflow/processed_data/tripdata_processed.parquet")

def extraction_task(**kwargs):
    df = data_extraction()
    kwargs['ti'].xcom_push(key='df', value=df)

def transform_task(**kwargs):
    df = kwargs['ti'].xcom_pull(key='df', task_ids='extract')
    df_transformed = transform_tripdata(df)
    kwargs['ti'].xcom_push(key='df_transformed', value=df_transformed)

def load_task(**kwargs):
    df_transformed = kwargs['ti'].xcom_pull(key='df_transformed', task_ids='transform')
    load_data(df_transformed, OUTPUT_PATH)

with DAG('tripdata_etl', start_date=datetime(2026, 3, 1), schedule_interval='@daily') as dag:
    extract = PythonOperator(task_id='extract', python_callable=extraction_task)
    transform = PythonOperator(task_id='transform', python_callable=transform_task)
    load = PythonOperator(task_id='load', python_callable=load_task)

    extract >> transform >> load
