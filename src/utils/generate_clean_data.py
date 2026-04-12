"""Module for generating synthetic big data in Hive-partitioned structure."""
from datetime import datetime
import os
import pandas as pd

from core.config import get_paths, BASE_DATE
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_vendas
from ingestion.upload_to_landing import upload_to_landing


def generate_clean_big_data():
    """Generates clean sales and customer data and uploads them to MinIO via API."""
    # Configuration and directory creation (local buffer)
    vendas_path, clientes_path = get_paths()
    
    # 1. Generate clean crm_clientes (~100k rows)
    n_customers = 100_000
    logger.info("🚀 Generating %d customers (CLEAN)...", n_customers)
    customers = generate_base_customers(n_customers)
    customers["created_at"] = [
        datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_customers)
    ]

    df_customers = pd.DataFrame(customers)
    local_clientes = f"{clientes_path}/clientes.json"
    df_customers.to_json(local_clientes, orient="records", lines=True)
    
    # Official Ingestion call for UI visibility
    upload_to_landing(local_clientes, f"crm_clientes/dt={BASE_DATE}/clientes.json")

    # 2. Generate clean erp_vendas (~1M rows)
    n_vendas = 1_000_000
    logger.info("🚀 Generating %d sales records (CLEAN)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=BASE_DATE
    )
    df_vendas = pd.DataFrame(vendas)

    local_vendas = f"{vendas_path}/vendas.csv"
    df_vendas.to_csv(local_vendas, index=False)
    
    # Official Ingestion call for UI visibility
    upload_to_landing(local_vendas, f"erp_vendas/dt={BASE_DATE}/vendas.csv")

    logger.info("✅ Clean Big Data generated and indexed successfully!")


if __name__ == "__main__":
    generate_clean_big_data()
