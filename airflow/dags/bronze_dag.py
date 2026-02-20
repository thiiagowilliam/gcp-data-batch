from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from scripts.bronze import ingest_raw_to_bronze

PROJECT_ID  = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("AIRFLOW_VAR_GCP_BUCKET_NAME")
TABLE_NAMES = os.getenv("AIRFLOW_VAR_GCP_TABLE_NAMES", "").split(",")

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='01_bronze_ingestion_layer',
    default_args=default_args,
    description='Ingestão Raw (CSV) para Bronze (Parquet + BQ)',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['medallion', 'bronze']
) as dag:

    for table in TABLE_NAMES:
        t1 = PythonOperator(
            task_id=f'convert_raw_to_parquet_{table}',
            python_callable=ingest_raw_to_bronze,
            op_kwargs={
                'bucket': BUCKET_NAME,
                'table_name': table
            }
        )

        t2 = GCSToBigQueryOperator(
            task_id=f'load_bronze_to_bq_{table}',
            bucket=BUCKET_NAME,
            source_objects=[f'datalake/bronze/{table}/*.parquet'],
            destination_project_dataset_table=f'{PROJECT_ID}.bronze.{table}',
            source_format='PARQUET',
            write_disposition='WRITE_TRUNCATE',
            autodetect=True
        )

        t1 >> t2