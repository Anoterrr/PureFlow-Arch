"""Transformation from Bronze to Silver: Applying Business Rules."""
import os
import duckdb
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger

def transform_bronze_to_silver():
    """Reads Bronze data, applies business rules, and saves to Silver."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    paths = get_s3_paths()
    
    logger.info("🥈 [Silver Transformation] Applying business rules...")
    
    # Business Rules:
    # 1. Filter out invalid sales (sale_value must be positive).
    # 2. Ensure IDs are present.
    # 3. Add a derived column for auditing.
    
    transformation_sql = f"""
        COPY (
            SELECT 
                *,
                'CLEANED' as _quality_status,
                current_timestamp as _processed_at
            FROM read_parquet('{paths['vendas_bronze']}')
            WHERE sale_value > 0 
              AND id IS NOT NULL
        ) TO '{paths['vendas_silver']}' (FORMAT 'PARQUET')
    """
    
    try:
        conn.execute(transformation_sql)
        logger.info(f"✅ Silver layer materialized at: {paths['vendas_silver']}")
    except Exception as e:
        logger.error(f"❌ Silver transformation failed: {e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    transform_bronze_to_silver()
