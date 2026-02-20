from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from scripts.silver import run_silver_pipeline

PROJECT_ID  = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("AIRFLOW_VAR_GCP_BUCKET_NAME")
TABLE_NAMES = os.getenv("AIRFLOW_VAR_GCP_TABLE_NAMES", "").split(",")

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='02_silver_trusted_layer',
    default_args=default_args,
    description='Transformação Bronze -> Silver com Validação de Contrato',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['medallion', 'silver', 'quality']
) as dag:

    for table in TABLE_NAMES:
        t1 = PythonOperator(
            task_id=f'validate_and_transform_{table}',
            python_callable=run_silver_pipeline,
            op_kwargs={
                'bucket': BUCKET_NAME,
                'table_name': table
            }
        )
        
        t2 = GCSToBigQueryOperator(
            task_id=f'load_silver_to_bq_{table}',
            bucket=BUCKET_NAME,
            source_objects=[f'datalake/silver/{table}/*.parquet'],
            destination_project_dataset_table=f'{PROJECT_ID}.silver.{table}',
            source_format='PARQUET',
            write_disposition='WRITE_TRUNCATE'
        )

        t1 >> t2