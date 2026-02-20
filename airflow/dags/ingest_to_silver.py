from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

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
    tags=['gcs', 'pyspark', 'bigquery'],
) as dag:

    for tabela in TABELAS:
        espera_por_arquivo_csv = GCSObjectsWithPrefixExistenceSensor(
            task_id=f'espera_por_arquivo_csv_{tabela}',
            bucket=BUCKET_NAME,
            prefix=f"{PREFIX_PATH}raw/{tabela}/",
            poke_interval=60,
            timeout=7200,
            mode='reschedule'
        )

        roda_pyspark = DataprocCreateBatchOperator(
            task_id=f"roda_transformacao_pyspark_{tabela}",
            project_id=PROJECT_ID,
            region=REGION,
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": f"gs://{BUCKET_NAME}/dags/ingest_to_silver.py",
                    "args": [BUCKET_NAME, tabela, PREFIX_PATH]
                }
            },
            batch_id=f"proc-{tabela}-{{{{ ds_nodash }}}}-{{{{ task_instance.try_number }}}}"
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

        espera_por_arquivo_csv >> roda_pyspark >> carrega_silver_no_bq