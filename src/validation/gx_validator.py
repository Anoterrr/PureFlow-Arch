"""Module for validating raw data in Landing Zone using Great Expectations (GX)."""
import sys
import duckdb

import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_landing_data():
    """Validates raw data in the Landing Zone and moves to Quarantine."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, raw_conn = setup_gx_backend(
        context, "landing_datasource", factory
    )

    logger.info("🛡️ Starting Data Gatekeeper (Great Expectations)...")

    suite = get_or_create_suite(context, "vendas_landing_suite")
    _add_landing_expectations(suite)

    vendas_landing_path = paths['vendas_landing']
    try:
        asset = datasource.get_asset("vendas_landing_asset")
    except (ValueError, KeyError):
        asset = datasource.add_query_asset(
            name="vendas_landing_asset",
            query=f"SELECT * FROM read_csv_auto('{vendas_landing_path}')"
        )

    checkpoint = context.add_or_update_checkpoint(
        name="landing_vendas_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": "vendas_landing_suite",
        }],
    )

    logger.info("🔍 Running validation for: %s", vendas_landing_path)
    checkpoint_result = checkpoint.run()

    if not checkpoint_result.success:
        _handle_landing_failure(context, raw_conn, vendas_landing_path, paths)

    logger.info("✅ Landing data validated successfully!")
    context.build_data_docs()

def _add_landing_expectations(suite):
    """Expectations for raw landing data."""
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="sale_value", min_value=0.01, max_value=None
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="sale_date", strftime_format="%Y-%m-%d"
    ))

def _handle_landing_failure(context, conn, source_path, paths):
    """Handles landing validation failure by quarantining."""
    logger.error("❌ Validation FAILED! Quarantining data...")
    context.build_data_docs()
    quarantine_path = paths['vendas_quarantine']
    move_to_quarantine(conn, source_path, quarantine_path)
    logger.error("🛑 Pipeline stopped due to quality issues.")
    sys.exit(1)

def move_to_quarantine(conn, source_path, target_path):
    """Moves a file to Quarantine using DuckDB."""
    try:
        conn.execute(
            f"COPY (SELECT * FROM read_csv_auto('{source_path}')) "
            f"TO '{target_path}' (FORMAT 'PARQUET')"
        )
        logger.info("📥 Data moved to quarantine: %s", target_path)
    except duckdb.Error as err:
        logger.error("Error during quarantine move: %s", err)


if __name__ == "__main__":
    validate_landing_data()
