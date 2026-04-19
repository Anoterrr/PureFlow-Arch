"""Customers data pipelines defined using the DataPipelineFactory."""

import os

from core.factory import DataPipelineFactory

# Paths to SQL files
SQL_DIR = os.path.join("src", "sql", "customers")

# --- Bronze Layer ---
stg_customers_bronze = DataPipelineFactory.create_asset(
    name="stg_customers_bronze",
    group_name="bronze",
    source={
        "path": "s3://landing-zone/customers_crm/dt={{ execution_date }}/customers.json",
        "format": "json",
    },
    target={
        "path": "s3://bronze/customers_crm/dt={{ execution_date }}/customers.parquet",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(
        os.path.join(SQL_DIR, "stg_customers_bronze.sql")
    ),
    expectations=[
        {
            "expectation": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "customer_id"},
        },
        {
            "expectation": "ExpectColumnValuesToMatchRegex",
            "kwargs": {"column": "email", "regex": r"[^@]+@[^@]+\.[^@]+"},
        },
    ],
)

# --- Silver Layer ---
customers_silver = DataPipelineFactory.create_asset(
    name="customers_silver",
    group_name="silver",
    depends_on=["stg_customers_bronze"],
    source={
        "path": "s3://bronze/customers_crm/dt={{ execution_date }}/customers.parquet",
        "format": "parquet",
    },
    target={
        "path": "s3://silver/customers/dt={{ execution_date }}/customers.parquet",
        "format": "parquet",
    },
    sql_transform=DataPipelineFactory.load_sql(
        os.path.join(SQL_DIR, "customers_silver.sql")
    ),
)
