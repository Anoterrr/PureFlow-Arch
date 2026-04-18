"""Customers data pipelines defined using the DataPipelineFactory."""
import os
from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "customers")

# --- Bronze Layer ---
stg_clientes_bronze = DataPipelineFactory.create_asset(
    name="stg_clientes_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/crm_clientes/dt={{ execution_date }}/clientes.json",
        "format": "json"
    },
    target={
        "path": "s3://bronze/crm_clientes/dt={{ execution_date }}/clientes.parquet",
        "format": "parquet"
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "stg_clientes_bronze.sql")),
    expectations=[
        {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "cliente_id"}},
        {
            "expectation": "ExpectColumnValuesToMatchRegex",
            "kwargs": {"column": "email", "regex": r"[^@]+@[^@]+\.[^@]+"}
        }
    ]
)

# --- Silver Layer ---
clientes_silver = DataPipelineFactory.create_asset(
    name="clientes_silver",
    group_name="silver",
    depends_on=["stg_clientes_bronze"],
    source={
        "path": "s3://bronze/crm_clientes/dt={{ execution_date }}/clientes.parquet",
        "format": "parquet"
    },
    target={
        "path": "s3://silver/clientes/dt={{ execution_date }}/clientes.parquet",
        "format": "parquet"
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "clientes_silver.sql"))
)
