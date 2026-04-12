"""Main orchestrator for the PureFlow-Arch pipeline."""
from ingestion.ingestion import ingest_to_bronze
from validation.gx_validator import validate_landing_data
from transformations.silver_to_gold import silver_to_gold_transformation
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def run_full_pipeline():
    """Runs the complete E2E pipeline: Landing -> Bronze -> Validation -> Silver -> Gold."""
    logger.info("🌊 Starting PureFlow-Arch E2E Pipeline...")

    # 1. Ingestion: Landing -> Bronze (Raw Parquet)
    logger.info("Step 1: Ingesting Landing Zone to Bronze Layer...")
    ingest_to_bronze()

    # 2. Validation: Gatekeeper on Landing Data
    logger.info("Step 2: Running Great Expectations Gatekeeper...")
    validate_landing_data()

    # 3. Silver Layer promotion
    logger.info("Step 3: Promoting Bronze to Silver Layer (Delta Lake)...")
    promote_to_silver()

    # 4. Gold Layer transformation
    logger.info("Step 4: Transforming Silver to Gold Layer (DuckDB)...")
    silver_to_gold_transformation()

    logger.info("✨ PureFlow-Arch Pipeline completed successfully!")


def promote_to_silver():
    """Moves validated Bronze data to Silver Layer."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)

    paths = get_s3_paths()

    try:
        # Promote Vendas
        logger.info("🥈 Promoting Vendas to Silver: %s", paths['vendas_silver'])
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{paths['vendas_bronze']}')) "
            f"TO '{paths['vendas_silver']}' (FORMAT 'PARQUET')"
        )

        # Promote Clientes
        logger.info("🥈 Promoting Clientes to Silver: %s", paths['clientes_silver'])
        conn.execute(
            f"COPY (SELECT * FROM read_parquet('{paths['clientes_bronze']}')) "
            f"TO '{paths['clientes_silver']}' (FORMAT 'PARQUET')"
        )

    except Exception as err:
        logger.error("❌ Promotion to Silver failed: %s", err)
        raise err
    finally:
        conn.close()


if __name__ == "__main__":
    run_full_pipeline()
