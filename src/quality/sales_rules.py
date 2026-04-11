"""Domain-specific Validation Rules for Sales (Great Expectations)."""
import os
import sys
import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger

def validate_sales_quality():
    """Runs GX validation specifically for the Sales domain."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    
    # Task 2.2: Context at gx/uncommitted/data_docs/
    context_root_dir = os.path.abspath("gx")
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)
    context = gx.get_context(context_root_dir=context_root_dir)

    # DuckDB as the gatekeeper execution engine
    datasource_name = "sales_quality_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except Exception:
        datasource = context.sources.add_duckdb(name=datasource_name)
    
    # Setup S3 auth for the underlying connection
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ [Sales Gatekeeper] Running Great Expectations...")

    # --- SALES-SPECIFIC EXPECTATION SUITE ---
    suite_name = "sales_quality_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    
    # Domain-specific rules
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="sale_value", min_value=0.0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(column="sale_date", strftime_format="%Y-%m-%d"))

    # Asset definition (reading from Bronze for pre-Silver validation)
    bronze_vendas_path = paths['vendas_bronze']
    asset_name = "sales_bronze_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_query_asset(
            name=asset_name, 
            query=f"SELECT * FROM read_parquet('{bronze_vendas_path}')"
        )

    # Run Checkpoint
    checkpoint_name = "sales_quality_checkpoint"
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[{"batch_request": asset.build_batch_request(), "expectation_suite_name": suite_name}],
    )

    result = checkpoint.run()

    # Task 2.3: Circuit Breaker Logic
    if not result.success:
        logger.error("❌ [Sales Quality] Validation failed! Moving data to quarantine...")
        context.build_data_docs()
        
        quarantine_path = paths['vendas_quarantine']
        # Circuit Breaker Move
        raw_conn.execute(f"COPY (SELECT * FROM read_parquet('{bronze_vendas_path}')) TO '{quarantine_path}' (FORMAT 'PARQUET')")
        
        logger.error(f"🛑 [Sales Quality] Circuit breaker engaged. Data moved to: {quarantine_path}")
        sys.exit(1)

    logger.info("✅ [Sales Quality] All checks passed. Proceeding to Silver layer.")
    context.build_data_docs()

if __name__ == "__main__":
    validate_sales_quality()
