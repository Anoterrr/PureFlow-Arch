"""Transformation from Bronze to Silver: Applying Business Rules using Delta Lake."""

import pandas as pd
from deltalake.writer import write_deltalake
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger

def transform_bronze_to_silver():
    """Reads Bronze data, applies business rules, and saves to Silver in Delta format."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    paths = get_s3_paths()

    logger.info("🥈 [Silver Transformation] Applying business rules & converting to Delta...")

    # Business Rules:
    # 1. Filter out invalid sales (sale_value must be positive).
    # 2. Ensure IDs are present.
    # 3. Add quality metadata.
    
    query = f"""
        SELECT
            *,
            'CLEANED' as _quality_status,
            current_timestamp as _processed_at
        FROM read_parquet('{paths['vendas_bronze']}')
        WHERE sale_value > 0 
          AND id IS NOT NULL
    """

    try:
        # Convert to Pandas then write as Delta (most reliable way for S3/MinIO in this setup)
        df = conn.execute(query).df()
        
        # S3 storage options for deltalake library
        import os
        storage_options = {
            "aws_endpoint_url": os.getenv("S3_ENDPOINT", "http://minio:9000"),
            "aws_access_key_id": os.getenv("STORAGE_USER", "admin"),
            "aws_secret_access_key": os.getenv("STORAGE_PASSWORD", "strongpassword123"),
            "aws_allow_http": "true",
            "aws_s3_allow_unsafe_rename": "true" # Required for some S3-compatible storages
        }

        write_deltalake(
            paths['vendas_silver'],
            df,
            mode="overwrite",
            storage_options=storage_options
        )
        
        logger.info("✅ Silver layer materialized as DELTA at: %s", paths['vendas_silver'])
    except Exception as err:
        logger.error("❌ Silver transformation failed: %s", err)
        raise err
    finally:
        conn.close()

if __name__ == "__main__":
    transform_bronze_to_silver()


if __name__ == "__main__":
    transform_bronze_to_silver()
