"""Shared configuration for data generation and ingestion."""
import os

# Base path for the Medallion architecture
BASE_LANDING_ZONE = "data/minio_data/landing-zone"
BASE_DATE = "2024-03-29"


def get_paths(base_date=BASE_DATE):
    """Returns the Hive-style paths for sales and customers."""
    vendas_path = f"{BASE_LANDING_ZONE}/erp_vendas/dt={base_date}"
    clientes_path = f"{BASE_LANDING_ZONE}/crm_clientes/dt={base_date}"

    # Ensure directories exist
    os.makedirs(vendas_path, exist_ok=True)
    os.makedirs(clientes_path, exist_ok=True)

    return vendas_path, clientes_path
