"""Module for generating synthetic dirty data to test validation gates."""
from datetime import datetime
import pandas as pd
import numpy as np

from core.config import get_s3_paths, BASE_DATE
from core.connection import ConnectionFactory
from core.logger import logger
from utils.generators import generate_base_customers, generate_base_vendas

def generate_dirty_big_data():
    """Generates dirty sales and customer data with intentional errors."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    
    s3_paths = get_s3_paths()
    
    # 1. Generate Dirty Customers (Null IDs)
    n_customers = 1000
    logger.info("🚀 Generating %d customers (DIRTY)...", n_customers)
    customers = generate_base_customers(n_customers)
    customers["created_at"] = [BASE_DATE] * n_customers
    df_customers = pd.DataFrame(customers)
    
    # Introduce Nulls in id
    df_customers.loc[0:10, 'id'] = None
    
    logger.info("📤 Writing dirty customers to Landing Zone...")
    conn.execute(f"COPY df_customers TO '{s3_paths['clientes_landing']}' (FORMAT 'JSON', ARRAY TRUE)")

    # 2. Generate Dirty Sales (Negative values, invalid dates)
    n_vendas = 5000
    logger.info("🚀 Generating %d sales records (DIRTY)...", n_vendas)
    vendas = generate_base_vendas(
        n_vendas=n_vendas,
        n_customers=n_customers,
        amount_range=(-500.0, 5000.0), # Intentional negative values
        base_date=BASE_DATE
    )
    df_vendas = pd.DataFrame(vendas)
    
    # Introduce Nulls in IDs
    df_vendas.loc[0:50, 'id'] = None
    
    logger.info("📤 Writing dirty sales to Landing Zone...")
    conn.execute(f"COPY df_vendas TO '{s3_paths['vendas_landing']}' (FORMAT 'CSV', HEADER TRUE)")

    conn.close()
    logger.info("⚠️ Dirty Data generated successfully! Validation gates should catch this.")

if __name__ == "__main__":
    generate_dirty_big_data()
