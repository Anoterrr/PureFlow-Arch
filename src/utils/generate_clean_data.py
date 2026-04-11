"""Module for generating synthetic big data in Hive-partitioned structure."""
import pandas as pd
from datetime import datetime

from core.config import get_paths, BASE_DATE
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_vendas

def generate_clean_big_data():
    """Generates clean sales and customer data using Hive-style partitioning."""
    vendas_path, clientes_path = get_paths()

    n_customers = 100_000
    logger.info("🚀 Generating %d customers (CLEAN)...", n_customers)
    customers = generate_base_customers(n_customers)
    customers["created_at"] = [
        datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_customers)
    ]

    df_customers = pd.DataFrame(customers)
    df_customers.to_json(f"{clientes_path}/clientes.json",
                         orient="records", lines=True)

    n_vendas = 1_000_000
    logger.info("🚀 Generating %d sales records (CLEAN)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=BASE_DATE
    )
    df_vendas = pd.DataFrame(vendas)

    logger.info("✨ Data generated without anomalies.")
    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)
    logger.info("✅ Clean Big Data generated in: %s", vendas_path)

if __name__ == "__main__":
    generate_clean_big_data()
