"""Module for generating synthetic big data with intentional anomalies."""
import pandas as pd
import numpy as np

from core.config import get_paths, BASE_DATE
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_vendas


def generate_dirty_big_data():
    """Generates partitioned sales and customer data with anomalies."""
    # Configuration and directory creation
    vendas_path, clientes_path = get_paths()

    # 1. Generate crm_clientes (~100k rows)
    n_customers = 100_000
    logger.info("🚀 Generating %d customers (DIRTY)...", n_customers)
    customers = generate_base_customers(n_customers)

    df_customers = pd.DataFrame(customers)
    df_customers.to_json(f"{clientes_path}/clientes.json",
                         orient="records", lines=True)

    # 2. Generate erp_vendas (~1M rows)
    n_vendas = 1_000_000
    logger.info("🚀 Generating %d sales records (DIRTY)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(5.0, 2000.0),
        base_date=BASE_DATE,
        customer_id_offset=500
    )
    df_vendas = pd.DataFrame(vendas)

    # 3. Inserting Anomalies
    logger.info("⚠️ Inserting intentional anomalies for quality testing...")
    df_vendas.loc[0:500, "sale_value"] = -50.0
    df_vendas.loc[1000:1500, "customer_id"] = np.nan
    df_vendas.loc[2000:2100, "sale_value"] = 99_999_999.0

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)
    logger.info("✅ Dirty Big Data generated successfully in: %s", vendas_path)


if __name__ == "__main__":
    generate_dirty_big_data()
