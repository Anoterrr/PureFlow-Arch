"""Module for validating Bronze data using Great Expectations (GX)."""
import os
import sys
import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def validate_bronze_data():
    """Validates Parquet files in the Bronze layer and moves to Quarantine if failed."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    
    # Initialize GX Context
    # We use a context that allows saving data docs
    context = gx.get_context()

    # 1. Connect to DuckDB via GX
    db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")
    datasource_name = "bronze_datasource"
    
    # Check if datasource already exists (for idempotency in some environments)
    try:
        datasource = context.sources.add_duckdb(name=datasource_name, connection_string=f"duckdb:///{db_path}")
    except Exception:
        datasource = context.get_datasource(datasource_name)
    
    # Setup S3 auth for the underlying connection
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ Starting Data Gatekeeper (Great Expectations)...")

    # --- EXPECTATIONS FOR VENDAS ---
    suite_vendas_name = "vendas_suite"
    try:
        suite_vendas = context.add_expectation_suite(expectation_suite_name=suite_vendas_name)
    except Exception:
        suite_vendas = context.get_expectation_suite(suite_vendas_name)
    
    suite_vendas.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite_vendas.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite_vendas.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0.01, max_value=1000000))
    suite_vendas.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="category", 
        value_set=["Electronics", "Home", "Fashion", "Grocery", "Garden"]
    ))

    # --- EXPECTATIONS FOR CLIENTES ---
    suite_clientes_name = "clientes_suite"
    try:
        suite_clientes = context.add_expectation_suite(expectation_suite_name=suite_clientes_name)
    except Exception:
        suite_clientes = context.get_expectation_suite(suite_clientes_name)
        
    suite_clientes.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id"))
    suite_clientes.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="name"))

    # --- RUN VALIDATION ---
    validations = [
        {
            "batch_request": datasource.add_parquet_asset(
                name="vendas_asset", gcs_prefix=paths['vendas_bronze']
            ).build_batch_request(),
            "expectation_suite_name": suite_vendas_name,
        },
        {
            "batch_request": datasource.add_parquet_asset(
                name="clientes_asset", gcs_prefix=paths['clientes_bronze']
            ).build_batch_request(),
            "expectation_suite_name": suite_clientes_name,
        }
    ]

    # Create a checkpoint
    checkpoint_name = "bronze_checkpoint"
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=validations,
    )

    logger.info("🔍 Running validation checkpoint...")
    checkpoint_result = checkpoint.run()

    # --- CHECK RESULTS AND QUARANTINE ---
    if not checkpoint_result.success:
        logger.error("❌ Validation FAILED! Generating Data Docs and quarantining data...")
        
        # Build Data Docs
        context.build_data_docs()
        # In a real CLI environment, we might not want to automatically open a browser, 
        # but the files are generated in gx/uncommitted/data_docs/
        logger.info("📄 Data Docs generated at: gx/uncommitted/data_docs/local_site/index.html")

        # Handle quarantine manually for the failed batches
        # Checkpoint results are indexed by validation ID or we can iterate through run_results
        for validation_result_identifier in checkpoint_result.run_results:
            result = checkpoint_result.run_results[validation_result_identifier]["validation_result"]
            asset_name = result.meta["active_batch_definition"]["asset_name"]
            
            if not result.success:
                if "vendas" in asset_name:
                    logger.warning(f"⚠️ Quarantining Vendas: {paths['vendas_quarantine']}")
                    move_to_quarantine(raw_conn, paths['vendas_bronze'], paths['vendas_quarantine'])
                elif "clientes" in asset_name:
                    logger.warning(f"⚠️ Quarantining Clientes: {paths['clientes_quarantine']}")
                    move_to_quarantine(raw_conn, paths['clientes_bronze'], paths['clientes_quarantine'])

        logger.error("🛑 Pipeline interrupted due to quality issues.")
        sys.exit(1)

    logger.info("✅ All Bronze data validated successfully!")


def move_to_quarantine(conn, source_path, target_path):
    """Moves a file from Bronze to Quarantine using DuckDB."""
    try:
        conn.execute(f"COPY (SELECT * FROM read_parquet('{source_path}')) TO '{target_path}' (FORMAT 'PARQUET')")
        logger.info(f"📥 File quarantined: {target_path}")
    except Exception as e:
        logger.error(f"Error during quarantine: {e}")


if __name__ == "__main__":
    validate_bronze_data()
