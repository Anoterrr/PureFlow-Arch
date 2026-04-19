"""Module for generating intentionally corrupt data at different layers."""

import os
from core.config import get_s3_paths, BASE_DATE
from core.connection import ConnectionFactory
from core.logger import logger

def corrupt_landing_zone():
    """Corrupts data in the Landing Zone (CSV/JSON)."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    s3_paths = get_s3_paths()

    logger.warning("🧨 [Corruptor] Corrupting Landing Zone data...")
    
    # 1. Corrupt Sales (CSV) - Inject Null IDs and Negative Prices
    conn.execute(f"""
        COPY (
            SELECT 
                CASE WHEN range % 10 = 0 THEN NULL ELSE range END as id,
                CASE WHEN range % 15 = 0 THEN -100.0 ELSE 50.0 END as preco,
                '2024-01-01' as data,
                1 as cliente_id,
                'Produto Corrompido' as produto
            FROM range(1, 1001)
        ) TO '{s3_paths['sales_landing']}' (FORMAT 'CSV', HEADER TRUE)
    """)

    # 2. Corrupt Customers (JSON) - Invalid Emails
    conn.execute(f"""
        COPY (
            SELECT 
                range as id,
                'invalid-email-' || range as email,
                'User ' || range as name,
                '{BASE_DATE}' as created_at
            FROM range(1, 501)
        ) TO '{s3_paths['customers_landing']}' (FORMAT 'JSON', ARRAY TRUE)
    """)
    
    conn.close()
    logger.info("✅ Landing Zone corrupted.")

def corrupt_bronze_layer():
    """Injects bad data directly into Bronze Parquet files."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    s3_paths = get_s3_paths()

    logger.warning("🧨 [Corruptor] Corrupting Bronze Layer...")

    # Inject sales with NULL product names or invalid dates in Bronze
    # This simulates a bug in the ingestion script
    conn.execute(f"""
        COPY (
            SELECT 
                range as id,
                101 as cliente_id,
                CASE WHEN range % 5 = 0 THEN NULL ELSE 'Fake Product' END as produto,
                -10.5 as preco,
                '1900-01-01' as data_venda,
                now() as _ingested_at,
                'sales' as _domain,
                'corrupted_ingest.csv' as _source_file
            FROM range(10000, 10500)
        ) TO '{s3_paths['sales_bronze']}' (FORMAT 'PARQUET')
    """)

    conn.close()
    logger.info("✅ Bronze Layer corrupted.")

if __name__ == "__main__":
    corrupt_landing_zone()
    corrupt_bronze_layer()
