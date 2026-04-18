"""Sales data pipelines defined using the DataPipelineFactory."""
import os
from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "sales")

# --- 1. Bronze Layer ---
stg_vendas_bronze = DataPipelineFactory.create_asset(
    name="stg_vendas_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/erp_vendas/dt={{ execution_date }}/vendas.csv",
        "format": "csv"
    },
    target={
        "path": "s3://bronze/erp_vendas/dt={{ execution_date }}/vendas.parquet",
        "format": "parquet"
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "stg_vendas_bronze.sql")),
    expectations=[
        {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "id"}},
        {
            "expectation": "ExpectTableRowCountToBeBetween",
            "kwargs": {"min_value": 1, "max_value": 2000000}
        }
    ]
)

# --- 2. Silver Layer ---
vendas_silver = DataPipelineFactory.create_asset(
    name="vendas_silver",
    group_name="silver",
    depends_on=["stg_vendas_bronze"],
    source={
        "path": "s3://bronze/erp_vendas/dt={{ execution_date }}/vendas.parquet",
        "format": "parquet"
    },
    target={
        "path": "s3://silver/vendas/dt={{ execution_date }}/vendas.parquet",
        "format": "parquet"
    },
    sql_transform=DataPipelineFactory.load_sql(os.path.join(SQL_DIR, "vendas_silver.sql")),
    expectations=[
        {
            "expectation": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "preco", "min_value": 0, "max_value": 10000}
        }
    ]
)
