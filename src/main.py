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

    # 2. Validation: Gatekeeper on Landing Data (as per Task 1)
    logger.info("Step 2: Running Great Expectations Gatekeeper...")
    validate_landing_data()

    # 3. Silver Layer: If valid, move/transform to Silver (Delta Lake)
    # For this demo, we'll simulate the Bronze to Silver move as Delta
    logger.info("Step 3: Promoting Bronze to Silver Layer (Delta Lake)...")
    promote_to_silver()

    # 4. Gold Layer: Transform Silver to Gold (DuckDB)
    logger.info("Step 4: Transforming Silver to Gold Layer (DuckDB)...")
    silver_to_gold_transformation()

    logger.info("✨ PureFlow-Arch Pipeline completed successfully!")


def promote_to_silver():
    """Moves validated Bronze data to Silver Layer in Delta Lake format."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(conn)
    
    paths = get_s3_paths()
    
    # In a real Delta environment, we'd use a dedicated library, 
    # but DuckDB's COPY to Delta (via extension) is a great local-first approach.
    try:
        # Promote Vendas
        conn.execute(f"COPY (SELECT * FROM read_parquet('{paths['vendas_bronze']}')) TO '{paths['vendas_silver']}' (FORMAT 'PARQUET')")
        # Note: True Delta Lake requires a transaction log (_delta_log). 
        # DuckDB's delta extension handles reads, for writes we simulate with parquet for this architecture.
        
        # Promote Clientes
        conn.execute(f"COPY (SELECT * FROM read_parquet('{paths['clientes_bronze']}')) TO '{paths['clientes_silver']}' (FORMAT 'PARQUET')")
        
        logger.info(f"🥈 Data promoted to Silver: {paths['vendas_silver']}")
    except Exception as e:
        logger.error(f"❌ Promotion to Silver failed: {e}")
        raise e
    finally:
        conn.close()


if __name__ == "__main__":
    run_full_pipeline()
