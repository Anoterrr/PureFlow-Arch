"""Module for generating intentionally corrupt data at different layers."""

from core.config import BASE_DATE, get_s3_paths
from core.connection import ConnectionFactory
from core.logger import logger


def corrupt_landing_zone(execution_date=None):
    """Corrupts data in the Landing Zone (CSV/JSON)."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    
    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Landing Zone data for date %s...", base_date)

    # 1. Corrupt Sales (CSV) - Inject Null IDs and Negative Prices
    conn.execute(
        """
        COPY (
            SELECT
                CASE WHEN range % 10 = 0 THEN NULL ELSE range END as id,
                CASE WHEN range % 15 = 0 THEN -100.0 ELSE 50.0 END as preco,
                '2024-01-01' as data,
                1 as cliente_id,
                'Produto Corrompido' as produto
            FROM range(1, 1001)
        ) TO ? (FORMAT 'CSV', HEADER TRUE)
        """,
        [s3_paths["sales_landing"]],
    )

    # 2. Corrupt Customers (JSON) - Invalid Emails
    conn.execute(
        """
        COPY (
            SELECT
                range as id,
                'invalid-email-' || range as email,
                'User ' || range as name,
                ? as created_at
            FROM range(1, 501)
        ) TO ? (FORMAT 'JSON', ARRAY TRUE)
        """,
        [base_date, s3_paths["customers_landing"]],
    )

    conn.close()
    logger.info("✅ Landing Zone corrupted.")


def corrupt_bronze_layer(execution_date=None):
    """Injects bad data directly into Bronze Parquet files."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    
    base_date = execution_date or BASE_DATE
    s3_paths = get_s3_paths(base_date=base_date)

    logger.warning("🧨 [Corruptor] Corrupting Bronze Layer for date %s...", base_date)

    # Inject sales with NULL product names or invalid dates in Bronze
    # This simulates a bug in the ingestion script
    conn.execute(
        """
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
        ) TO ? (FORMAT 'PARQUET')
        """,
        [s3_paths["sales_bronze"]],
    )

    conn.close()
    logger.info("✅ Bronze Layer corrupted.")


if __name__ == "__main__":
    corrupt_landing_zone()
    corrupt_bronze_layer()
