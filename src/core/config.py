"""Shared configuration for data generation and ingestion."""
import os

# Local paths for development/generation
BASE_LANDING_ZONE = "data/minio_data/landing-zone"
BASE_DATE = "2024-03-29"

# S3 / MinIO Configuration
S3_BUCKET_LANDING = "landing-zone"
S3_BUCKET_BRONZE = "bronze"
S3_BUCKET_SILVER = "silver"
S3_BUCKET_QUARANTINE = "quarantine"


def get_paths(base_date=BASE_DATE):
    """Returns the Hive-style paths for sales and customers."""
    vendas_path = f"{BASE_LANDING_ZONE}/erp_vendas/dt={base_date}"
    clientes_path = f"{BASE_LANDING_ZONE}/crm_clientes/dt={base_date}"

    # Ensure directories exist
    os.makedirs(vendas_path, exist_ok=True)
    os.makedirs(clientes_path, exist_ok=True)

    return vendas_path, clientes_path


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

        # Quarantine (Failed validation)
        "vendas_quarantine":
            f"s3://{S3_BUCKET_QUARANTINE}/erp_vendas/dt={base_date}/vendas.parquet",
        "clientes_quarantine":
            f"s3://{S3_BUCKET_QUARANTINE}/crm_clientes/dt={base_date}/clientes.parquet",
    }
