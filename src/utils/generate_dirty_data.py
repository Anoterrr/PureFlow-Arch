"""Module for generating synthetic big data with intentional anomalies."""

import numpy as np
import pandas as pd

from core.config import BASE_DATE, get_paths
from core.logger import logger
from ingestion.upload_to_landing import upload_to_landing
from utils.generators import generate_base_customers, generate_base_vendas


def generate_dirty_big_data():
    """Generates partitioned sales and customer data with anomalies and uploads to MinIO."""
    # Configuration and directory creation
    vendas_path, clientes_path = get_paths()

    # 1. Generate crm_clientes (~100k rows)
    n_customers = 100_000
    logger.info("🚀 Generating %d customers (DIRTY)...", n_customers)
    customers = generate_base_customers(n_customers)

    df_customers = pd.DataFrame(customers)
    local_clientes = f"{clientes_path}/clientes.json"
    df_customers.to_json(local_clientes, orient="records", lines=True)

    # Official Ingestion call
    upload_to_landing(local_clientes, f"crm_clientes/dt={BASE_DATE}/clientes.json")

    # 2. Generate erp_vendas (~1M rows)
    n_vendas = 1_000_000
    logger.info("🚀 Generating %d sales records (DIRTY)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(5.0, 2000.0),
        base_date=BASE_DATE,
        customer_id_offset=500,
    )
    df_vendas = pd.DataFrame(vendas)

    # 3. Inserting Anomalies
    logger.info("⚠️ Inserting intentional anomalies for quality testing...")
    df_vendas.loc[0:500, "sale_value"] = -50.0
    df_vendas.loc[1000:1500, "customer_id"] = np.nan
    df_vendas.loc[2000:2100, "sale_value"] = 99_999_999.0

    local_vendas = f"{vendas_path}/vendas.csv"
    df_vendas.to_csv(local_vendas, index=False)

    # Official Ingestion call
    upload_to_landing(local_vendas, f"erp_vendas/dt={BASE_DATE}/vendas.csv")

    logger.info("✅ Dirty Big Data generated and indexed successfully!")


if __name__ == "__main__":
    generate_dirty_big_data()
