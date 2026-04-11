"""Domain-specific Validation Rules for Silver (Great Expectations)."""
import os
import sys
import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger

def validate_silver_quality():
    """Validates Silver data post-business rules application."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    
    context_root_dir = os.path.abspath("gx")
    context = gx.get_context(context_root_dir=context_root_dir)

    datasource_name = "silver_quality_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except Exception:
        datasource = context.sources.add_duckdb(name=datasource_name)
    
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ [Silver Gatekeeper] Running Great Expectations...")

    # --- SILVER EXPECTATION SUITE ---
    suite_name = "silver_quality_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    
    # Business-level rules:
    # 1. We expect _quality_status to be 'CLEANED'
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="_quality_status", value_set=["CLEANED"]
    ))
    
    # 2. Re-verify basic constraints (redundant but safe)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="sale_value", min_value=0.01))

    # Asset reading from Silver
    silver_vendas_path = paths['vendas_silver']
    asset_name = "sales_silver_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except Exception:
        asset = datasource.add_query_asset(
            name=asset_name, 
            query=f"SELECT * FROM read_parquet('{silver_vendas_path}/*.parquet')"
        )

    checkpoint = context.add_or_update_checkpoint(
        name="silver_checkpoint",
        validations=[{"batch_request": asset.build_batch_request(), "expectation_suite_name": suite_name}],
    )

    result = checkpoint.run()

    if not result.success:
        logger.error("❌ [Silver Quality] Validation failed! Stopping pipeline before Gold layer.")
        context.build_data_docs()
        sys.exit(1)

    logger.info("✅ [Silver Quality] All checks passed. Proceeding to Gold layer.")
    context.build_data_docs()

if __name__ == "__main__":
    validate_silver_quality()
