import pandera as pa
import pandas as pd

vendas_schema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(int, nullable=False, coerce=True),
        "cliente_id": pa.Column(int, nullable=False, coerce=True),
        "produto_id": pa.Column(float, nullable=True, coerce=True),
        "data_venda": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "valor_total": pa.Column(float, nullable=True, coerce=True),
        "quantidade": pa.Column(float, nullable=True, coerce=True),
        "metodo_pagto": pa.Column(str, nullable=True, coerce=True)
    }
)

clientes_schema = pa.DataFrameSchema(
    columns={
        "id": pa.Column(int, nullable=False, coerce=True),
        "nome": pa.Column(str, nullable=True, coerce=True),
        "email": pa.Column(str, nullable=True, coerce=True),
        "telefone": pa.Column(str, nullable=True, coerce=True),
        "cidade": pa.Column(str, nullable=True, coerce=True),
        "estado": pa.Column(str, nullable=True, coerce=True),
        "data_cadastro": pa.Column(pa.DateTime, nullable=False, coerce=True),
        "status": pa.Column(str, nullable=True, coerce=True)
    }
)

def validate_data(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    schema = vendas_schema if table_name == "vendas" else clientes_schema
    required_cols = [col_name for col_name, col in schema.columns.items() if not col.nullable]
    
    df_clean = df.dropna(subset=required_cols).copy()
    
    return schema.validate(df_clean)