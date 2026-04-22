"""Shared configuration for data generation and ingestion."""

import os

# Base date for data generation and processing
BASE_DATE = os.getenv("BASE_DATE", "2026-04-19")

# S3 / MinIO Configuration
S3_BUCKET_LANDING = os.getenv("S3_BUCKET_LANDING", "landing-zone")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "silver")
S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD", "gold")
S3_BUCKET_QUARANTINE = os.getenv("S3_BUCKET_QUARANTINE", "quarantine")


def get_s3_paths(base_date: str):
    """Returns the S3 URI paths for ingestion and validation layers."""
    return {
        # Landing (Input)
        "sales_landing": (
            f"s3://{S3_BUCKET_LANDING}/sales_erp/dt={base_date}/sales.csv"
        ),
        "customers_landing": (
            f"s3://{S3_BUCKET_LANDING}/customers_crm/dt={base_date}/customers.json"
        ),
        # Bronze (Raw Parquet)
        "sales_bronze": f"s3://{S3_BUCKET_BRONZE}/sales_erp/dt={base_date}/sales.parquet",
        "customers_bronze": (
            f"s3://{S3_BUCKET_BRONZE}/customers_crm/dt={base_date}/customers.parquet"
        ),
        # Silver (Validated Delta/Parquet)
        "sales_silver": f"s3://{S3_BUCKET_SILVER}/sales_erp/dt={base_date}/",
        "customers_silver": f"s3://{S3_BUCKET_SILVER}/customers_crm/dt={base_date}/",
        # Gold (Business Insights/Aggregations)
        "sales_summary": f"s3://{S3_BUCKET_GOLD}/sales_summary/dt={base_date}/",
        # Quarantine (Failed validation)
        "sales_quarantine": (
            f"s3://{S3_BUCKET_QUARANTINE}/sales_erp/dt={base_date}/sales.parquet"
        ),
        "customers_quarantine": (
            f"s3://{S3_BUCKET_QUARANTINE}/customers_crm/dt={base_date}/customers.parquet"
        ),
    }
