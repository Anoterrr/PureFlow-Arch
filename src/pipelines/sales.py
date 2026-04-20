"""Sales data pipelines defined using the DataPipelineFactory."""

import os

from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "sales")

# --- 1. Bronze Layer ---
stg_sales_bronze, stg_sales_bronze_check = DataPipelineFactory.create_asset(
    name="stg_sales_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/sales_erp/dt={{ execution_date }}/sales.csv",
        "format": "csv",
    },
    target={
        "path": "s3://bronze/sales_erp/dt={{ execution_date }}/sales.parquet",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(
        os.path.join(SQL_DIR, "stg_sales_bronze.sql")
    ),
    expectations=[
        {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "id"}},
        {
            "expectation": "ExpectTableRowCountToBeBetween",
            "kwargs": {"min_value": 1, "max_value": 2000000},
        },
    ],
)

# --- 2. Silver Layer ---
sales_silver, sales_silver_check = DataPipelineFactory.create_asset(
    name="sales_silver",
    group_name="silver",
    depends_on=["stg_sales_bronze"],
    source={
        "path": "s3://bronze/sales_erp/dt={{ execution_date }}/sales.parquet",
        "format": "parquet",
    },
    target={
        "path": "s3://silver/sales/dt={{ execution_date }}/sales.parquet",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(
        os.path.join(SQL_DIR, "sales_silver.sql")
    ),
    expectations=[
        {
            "expectation": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "price", "min_value": 0, "max_value": 10000},
        }
    ],
)
