import os
import json
import pandas as pd
import pandera as pa
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

class SilverProcessor:
    def __init__(self, bucket: str, table_name: str):
        self.bucket = bucket
        self.table_name = table_name
        self.dags_folder = os.environ.get('DAGS_FOLDER', 'dags/')
        self.contract_path = os.path.join(self.dags_folder, 'contracts', 'schemas.json')
        self.input_path = f"gs://{self.bucket}/datalake/bronze/{self.table_name}/"
        self.output_path = f"gs://{self.bucket}/datalake/silver/{self.table_name}/"

    def _get_contract_config(self) -> dict:
        if not os.path.exists(self.contract_path):
            raise FileNotFoundError(f"Contrato não encontrado em {self.contract_path}")
            
        with open(self.contract_path, 'r') as f:
            full_contract = json.load(f)
        
        config = full_contract.get(f"silver_{self.table_name}")
        if not config:
            raise ValueError(f"Configuração 'silver_{self.table_name}' ausente no contrato.")
        return config

    def _build_pandera_schema(self, config: dict) -> pa.DataFrameSchema:
        """Mapeia os tipos do BigQuery para validação Pandera."""
        type_map = {
            "INTEGER": "int64",
            "STRING": "str",
            "FLOAT": "float64",
            "DATE": "datetime64[ns]",
            "BOOLEAN": "bool"
        }

        columns = {
            col['name']: pa.Column(
                dtype=type_map.get(col['type'], "str"),
                nullable=(col['mode'] == "NULLABLE"),
                coerce=True 
            ) for col in config['columns']
        }
        
        return pa.DataFrameSchema(columns, strict=True)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica a blitz de qualidade do Pandera."""
        print(f"Validando contrato para {self.table_name}...")
        config = self._get_contract_config()
        schema = self._build_pandera_schema(config)
        return schema.validate(df)

    def transform(self, df_spark):
        for col_name, dtype in df_spark.dtypes:
            if dtype == 'string':
                df_spark = df_spark.withColumn(col_name, F.trim(F.col(col_name)))
            
        return df_spark.withColumn("dw_processed_at", F.current_timestamp())

    def execute(self):
        """Fluxo Principal: Ingestão -> Validação -> Transformação -> Escrita."""
        spark = SparkSession.builder \
            .appName(f"Silver_Processor_{self.table_name}") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

        try:
            df_pd = pd.read_parquet(self.input_path)
            df_validated_pd = self.validate(df_pd)
            df_spark = spark.createDataFrame(df_validated_pd)
            df_final = self.transform(df_spark)
            df_final.write.mode("overwrite").parquet(self.output_path)
            print(f"Sucesso: Camada Silver atualizada para {self.table_name}")

        finally:
            spark.stop()

def run_silver_pipeline(bucket: str, table_name: str):
    """Entrypoint para o PythonOperator do Airflow."""
    processor = SilverProcessor(bucket, table_name)
    processor.execute()