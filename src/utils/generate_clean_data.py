"""Module for generating synthetic big data in Hive-partitioned structure."""

import pandas as pd

from core.config import BASE_DATE, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_vendas


def generate_clean_big_data():
    """Generates clean sales and customer data and writes directly to MinIO."""
    # 1. Initialize Connection
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    s3_paths = get_s3_paths()

    # 2. Generate clean crm_clientes (~100k rows)
    n_customers = 100_000
    logger.info("🚀 Generating %d customers (CLEAN)...", n_customers)
    customers = generate_base_customers(n_customers)
    customers["created_at"] = [BASE_DATE] * n_customers

    df_customers = pd.DataFrame(customers)  # pylint: disable=unused-variable

    logger.info(
        "📤 Writing customers directly to Landing Zone: %s",
        s3_paths["clientes_landing"],
    )
    conn.execute(
        f"COPY df_customers TO '{s3_paths['clientes_landing']}' (FORMAT 'JSON', ARRAY TRUE)"
    )

    # 3. Generate clean erp_vendas (~1M rows)
    n_vendas = 1_000_000
    logger.info("🚀 Generating %d sales records (CLEAN)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(10.0, 5000.0),
        base_date=BASE_DATE,
    )
    df_vendas = pd.DataFrame(vendas)  # pylint: disable=unused-variable

    logger.info(
        "📤 Writing sales directly to Landing Zone: %s", s3_paths["vendas_landing"]
    )
    conn.execute(
        f"COPY df_vendas TO '{s3_paths['vendas_landing']}' (FORMAT 'CSV', HEADER TRUE)"
    )

    conn.close()
    logger.info("✅ Clean Big Data generated and written to MinIO successfully!")


if __name__ == "__main__":
    generate_clean_big_data()
