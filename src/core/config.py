"""Shared configuration for data generation and ingestion."""
import os
from datetime import datetime

# Default date set to today if not provided
BASE_DATE = os.getenv("EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d"))

# S3 / MinIO Configuration
S3_BUCKET_LANDING = os.getenv("S3_BUCKET_LANDING", "landing-zone")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "silver")
S3_BUCKET_GOLD = os.getenv("S3_BUCKET_GOLD", "gold")
S3_BUCKET_QUARANTINE = os.getenv("S3_BUCKET_QUARANTINE", "quarantine")


def get_s3_paths(base_date=BASE_DATE):
    """Returns the S3 URI paths for ingestion and validation layers."""
    return {
        # Landing (Input)
        "vendas_landing": f"s3://{S3_BUCKET_LANDING}/erp_vendas/dt={base_date}/vendas.csv",
        "clientes_landing": f"s3://{S3_BUCKET_LANDING}/crm_clientes/dt={base_date}/clientes.json",

        # Bronze (Raw Parquet)
        "vendas_bronze": f"s3://{S3_BUCKET_BRONZE}/erp_vendas/dt={base_date}/vendas.parquet",
        "clientes_bronze": f"s3://{S3_BUCKET_BRONZE}/crm_clientes/dt={base_date}/clientes.parquet",

        # Silver (Validated Delta/Parquet)
        "vendas_silver": f"s3://{S3_BUCKET_SILVER}/erp_vendas/dt={base_date}/",
        "clientes_silver": f"s3://{S3_BUCKET_SILVER}/crm_clientes/dt={base_date}/",

        # Gold (Business Insights/Aggregations)
        "sales_summary": f"s3://{S3_BUCKET_GOLD}/sales_summary/dt={base_date}/",

        # Quarantine (Failed validation)
        "vendas_quarantine":
            f"s3://{S3_BUCKET_QUARANTINE}/erp_vendas/dt={base_date}/vendas.parquet",
        "clientes_quarantine":
            f"s3://{S3_BUCKET_QUARANTINE}/crm_clientes/dt={base_date}/clientes.parquet",
    }
