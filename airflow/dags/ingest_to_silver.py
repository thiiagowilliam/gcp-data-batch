from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os
from ingest_to_silver import process_silver_layer

PROJECT_ID   = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID")
BUCKET_NAME  = os.getenv("AIRFLOW_VAR_GCP_BUCKET_NAME")
PREFIX_PATH  = os.getenv("AIRFLOW_VAR_GCP_PREFIX_PATH", 'datalake/')
DATASET_NAME = os.getenv("AIRFLOW_VAR_GCP_DATASET_NAME")
REGION       = os.getenv("AIRFLOW_VAR_GCP_REGION")

TABELAS = ["vendas", "clientes"]

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='datalake_processamento_dinamico',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['gcs', 'pandas', 'bigquery'],
) as dag:

    for tabela in TABELAS:
        espera_por_arquivo_csv = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'espera_por_arquivo_csv_{tabela}',
            bucket=BUCKET_NAME,
            prefix=f"{PREFIX_PATH}raw/{tabela}/",
            poke_interval=120,
            timeout=7200,
            mode='reschedule'
        )

        roda_transformacao = PythonOperator(
            task_id=f"roda_transformacao_pandas_{tabela}",
            python_callable=process_silver_layer,
            op_kwargs={
                "bucket_name": BUCKET_NAME,
                "table_name": tabela,
                "prefix_path": PREFIX_PATH
            }
        )

        carrega_silver_no_bq = GCSToBigQueryOperator(
            task_id=f'carrega_silver_no_bq_{tabela}',
            bucket=BUCKET_NAME,
            source_objects=[f'{PREFIX_PATH}trusted/{tabela}/*.parquet'],
            destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_NAME}.{tabela}',
            source_format='PARQUET',
            write_disposition='WRITE_APPEND',
            autodetect=True,
        )

        espera_por_arquivo_csv >> roda_transformacao >> carrega_silver_no_bq