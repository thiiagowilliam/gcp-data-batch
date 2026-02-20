import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType
import pyspark.sql.functions as F

class VendasProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.partition_field = "data_venda"
        self.schema = StructType([
            StructField("id", IntegerType(), nullable=False),          
            StructField("cliente_id", IntegerType(), nullable=False),  
            StructField("produto_id", IntegerType(), nullable=True),
            StructField("data_venda", DateType(), nullable=False),      
            StructField("valor_total", FloatType(), nullable=True),
            StructField("quantidade", IntegerType(), nullable=True),
            StructField("metodo_pagto", StringType(), nullable=True)
        ])

    def validate(self, df):
        df_valid = df.filter(
            F.col("id").isNotNull() & 
            F.col("cliente_id").isNotNull() & 
            F.col("data_venda").isNotNull()
        )
        df_invalid = df.subtract(df_valid)
        if df_invalid.count() > 0:
            print(f"Atenção: {df_invalid.count()} registros de vendas falharam na validação.")
            
        return df_valid

    def transform(self, df):
        return df \
            .withColumn("metodo_pagto", F.upper(F.trim(F.col("metodo_pagto")))) \
            .withColumn("valor_total", F.round(F.col("valor_total"), 2)) \
            .withColumn("quantidade", F.coalesce(F.col("quantidade"), F.lit(0))) 

class ClientesProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.partition_field = "data_cadastro"
        self.schema = StructType([
            StructField("id", IntegerType(), nullable=False),             
            StructField("nome", StringType(), nullable=True),
            StructField("email", StringType(), nullable=True),
            StructField("telefone", StringType(), nullable=True),
            StructField("cidade", StringType(), nullable=True),
            StructField("estado", StringType(), nullable=True),
            StructField("data_cadastro", DateType(), nullable=False),   
            StructField("status", StringType(), nullable=True)
        ])

    def validate(self, df):
        df_valid = df.filter(
            F.col("id").isNotNull() & 
            F.col("data_cadastro").isNotNull()
        )
        return df_valid

    def transform(self, df):
        return df \
            .withColumn("email", F.lower(F.trim(F.col("email")))) \
            .withColumn("estado", F.upper(F.trim(F.col("estado")))) \
            .withColumn("status", F.initcap(F.trim(F.col("status")))) \
            .withColumn("cidade", F.initcap(F.trim(F.col("cidade"))))

def main():
    spark = SparkSession.builder \
        .appName("DatalakeValidation") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.network.timeout", "800s") \
        .getOrCreate()
    
    bucket_name = sys.argv[1]
    table_name = sys.argv[2]
    prefix_path = sys.argv[3] 
    
    if table_name == "vendas":
        processor = VendasProcessor(spark)
    elif table_name == "clientes":
        processor = ClientesProcessor(spark)
    else:
        raise ValueError(f"Tabela desconhecida: {table_name}")

    raw_path = f"gs://{bucket_name}/{prefix_path}raw/{table_name}/*.csv"
    trusted_path = f"gs://{bucket_name}/{prefix_path}trusted/{table_name}/"

    df_raw = spark.read.csv(raw_path, header=True, schema=processor.schema)
    df_valid = processor.validate(df_raw)
    df_trusted = processor.transform(df_valid)
    
    df_trusted.write.mode("overwrite").partitionBy(processor.partition_field).parquet(trusted_path)

if __name__ == "__main__":
    main()