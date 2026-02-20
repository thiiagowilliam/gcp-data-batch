import sys
import pandas as pd
import gcsfs
from validation import validate_data

class VendasSilver:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['metodo_pagto'] = df['metodo_pagto'].str.strip().str.upper()
        df['valor_total'] = df['valor_total'].round(2)
        df['quantidade'] = df['quantidade'].fillna(0).astype(int)
        df['data_venda'] = pd.to_datetime(df['data_venda']).dt.date
        return df

class ClientesSilver:
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['email'] = df['email'].str.strip().str.lower()
        df['estado'] = df['estado'].str.strip().str.upper()
        df['status'] = df['status'].str.strip().str.title()
        df['cidade'] = df['cidade'].str.strip().str.title()
        df['data_cadastro'] = pd.to_datetime(df['data_cadastro']).dt.date
        return df

def process_silver_layer(bucket_name: str, table_name: str, prefix_path: str):
    raw_path = f"gs://{bucket_name}/{prefix_path}raw/{table_name}/*.csv"
    silver_path = f"gs://{bucket_name}/{prefix_path}trusted/{table_name}/"

    fs = gcsfs.GCSFileSystem()
    csv_files = fs.glob(raw_path)
    
    if not csv_files:
        sys.exit(0)

    df_raw = pd.concat([pd.read_csv(f"gs://{f}") for f in csv_files], ignore_index=True)
    
    df_valid = validate_data(df_raw, table_name)

    if table_name == "vendas":
        transformer = VendasSilver()
        partition_col = "data_venda"
    elif table_name == "clientes":
        transformer = ClientesSilver()
        partition_col = "data_cadastro"
    else:
        raise ValueError("Tabela desconhecida")

    df_silver = transformer.transform(df_valid)

    df_silver.to_parquet(
        silver_path,
        engine='pyarrow',
        partition_cols=[partition_col],
        index=False
    )

if __name__ == "__main__":
    process_silver_layer(sys.argv[1], sys.argv[2], sys.argv[3])