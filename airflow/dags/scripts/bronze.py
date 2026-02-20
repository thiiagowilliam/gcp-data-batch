from pyspark.sql import SparkSession

def ingest_raw_to_bronze(bucket: str, table_name: str):
    spark = SparkSession.builder \
        .appName(f"Bronze_Ingest_{table_name}") \
        .getOrCreate()

    raw_path = f"gs://{bucket}/datalake/raw/{table_name}/*.csv"
    bronze_path = f"gs://{bucket}/datalake/bronze/{table_name}/"

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(raw_path)

    df.write.mode("overwrite").parquet(bronze_path)
    spark.stop()