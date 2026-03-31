"""Module for generating synthetic big data in Hive-partitioned structure."""
from datetime import datetime
import pandas as pd
from core.config import get_paths, BASE_DATE
from utils.generators import generate_base_customers, generate_base_vendas
from core.logger import logger


def generate_clean_big_data():
    """Generates clean sales and customer data using Hive-style partitioning."""
    # Configuration and directory creation
    vendas_path, clientes_path = get_paths()

    # 1. Generate clean crm_clientes (~100k rows)
    n_customers = 100_000
    logger.info(f"🚀 Generating {n_customers} customers (CLEAN)...")
    customers = generate_base_customers(n_customers)
    # Add CLEAN specific field
    customers["created_at"] = [
        datetime.strptime(BASE_DATE, "%Y-%m-%d") for _ in range(n_customers)
    ]

    df_customers = pd.DataFrame(customers)
    # Keeping JSON for consistency with the flow
    df_customers.to_json(f"{clientes_path}/clientes.json", orient="records", lines=True)

    # 2. Generate clean erp_vendas (~1M rows)
    n_vendas = 1_000_000
    logger.info(f"🚀 Generating {n_vendas} sales records (CLEAN)...")
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=BASE_DATE
    )
    df_vendas = pd.DataFrame(vendas)

    # No anomalies inserted here - this is "Good Data"
    logger.info("✨ Data generated without anomalies.")

    df_vendas.to_csv(f"{vendas_path}/vendas.csv", index=False)
    logger.info(f"✅ Clean Big Data generated successfully in: {vendas_path}")


if __name__ == "__main__":
    generate_clean_big_data()

