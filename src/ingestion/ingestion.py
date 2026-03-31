"""Module for ingesting raw data from MinIO (S3) into the Bronze layer."""
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def ingest_to_bronze():
    """Reads raw CSV/JSON from S3 and saves as Parquet in the Bronze bucket."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    # Get Hive-partitioned S3 paths
    paths = get_s3_paths()

    logger.info("🚀 Starting Ingestion: Landing Zone (S3) -> Bronze (S3)...")

    # 1. Ingest erp_vendas (CSV)
    logger.info(f"📥 Ingesting Vendas: {paths['vendas_landing']}")
    conn.execute(
        f"""
        COPY (SELECT *, now() as ingested_at FROM read_csv_auto('{paths['vendas_landing']}'))
        TO '{paths['vendas_bronze']}' (FORMAT 'PARQUET')
    """
    )

    # 2. Ingest crm_clientes (JSON)
    logger.info(f"📥 Ingesting Clientes: {paths['clientes_landing']}")
    conn.execute(
        f"""
        COPY (SELECT *, now() as ingested_at FROM read_json_auto('{paths['clientes_landing']}'))
        TO '{paths['clientes_bronze']}' (FORMAT 'PARQUET')
    """
    )

    logger.info("✔️ Ingestion to Bronze complete!")
    conn.close()


if __name__ == "__main__":
    ingest_to_bronze()
