"""Module for validating raw data in Landing Zone using Great Expectations (GX)."""
import os
import sys
import shutil
import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def validate_landing_data():
    """Validates raw data in the Landing Zone and moves to Quarantine if failed."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    
    # Initialize GX Context in a specific directory
    context_root_dir = os.path.abspath("gx")
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)
    context = gx.get_context(context_root_dir=context_root_dir)

    # 1. Connect to DuckDB via GX for validation processing
    # DuckDB is used as the execution engine for GX
    datasource_name = "landing_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except Exception:
        datasource = context.sources.add_duckdb(name=datasource_name)
    
    # Setup S3 auth for the underlying DuckDB connection
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ Starting Data Gatekeeper (Great Expectations)...")

    # --- EXPECTATIONS FOR VENDAS ---
    suite_name = "vendas_landing_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    
    # Task 1.1: 'id' column must not be null
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    
    # Task 1.2: 'sale_value' must be greater than 0
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="sale_value", min_value=0.01, max_value=None
    ))
    
    # Task 1.3: 'sale_date' must match ISO format
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="sale_date", strftime_format="%Y-%m-%d"
    ))

    # --- RUN VALIDATION ---
    # We validate the CSV in the landing zone
    vendas_landing_path = paths['vendas_landing']
    
    # Define Asset
    asset_name = "vendas_landing_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        # We use read_csv_auto logic via DuckDB asset
        asset = datasource.add_query_asset(
            name=asset_name, 
            query=f"SELECT * FROM read_csv_auto('{vendas_landing_path}')"
        )

    # Create Checkpoint
    checkpoint_name = "landing_vendas_checkpoint"
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "batch_request": asset.build_batch_request(),
                "expectation_suite_name": suite_name,
            }
        ],
    )

    logger.info(f"🔍 Running validation for: {vendas_landing_path}")
    checkpoint_result = checkpoint.run()

    # --- CIRCUIT BREAKER & DATA DOCS ---
    if not checkpoint_result.success:
        logger.error("❌ Validation FAILED! Quarantining data and stopping pipeline...")
        
        # Task 1.4: Generate Data Docs HTML report
        context.build_data_docs()
        logger.info(f"📄 Data Docs generated in: {context_root_dir}/uncommitted/data_docs/")

        # Task 1.5: Move to quarantine (Circuit Breaker)
        quarantine_path = paths['vendas_quarantine']
        move_to_quarantine(raw_conn, vendas_landing_path, quarantine_path)
        
        logger.error("🛑 Pipeline stopped due to data quality issues.")
        sys.exit(1)

    logger.info("✅ Landing data validated successfully!")
    # Build data docs even on success for audit
    context.build_data_docs()


def move_to_quarantine(conn, source_path, target_path):
    """Moves a file to Quarantine using DuckDB (to handle S3 paths natively)."""
    try:
        # Move raw data to quarantine folder
        # We copy then potentially the orchestrator would delete from landing
        conn.execute(f"COPY (SELECT * FROM read_csv_auto('{source_path}')) TO '{target_path}' (FORMAT 'PARQUET')")
        logger.info(f"📥 Data moved to quarantine: {target_path}")
    except Exception as e:
        logger.error(f"Error during quarantine move: {e}")


if __name__ == "__main__":
    validate_landing_data()

