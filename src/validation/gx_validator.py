"""Module for validating raw data in Landing Zone using Great Expectations (GX)."""
import sys

import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context

# pylint: disable=too-many-locals, duplicate-code
def validate_landing_data():
    """Validates raw data in the Landing Zone and moves to Quarantine."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource_name = "landing_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except (ValueError, KeyError):
        datasource = context.sources.add_duckdb(name=datasource_name)

    # pylint: disable=protected-access
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ Starting Data Gatekeeper (Great Expectations)...")

    suite_name = "vendas_landing_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except (ValueError, KeyError):
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="sale_value", min_value=0.01, max_value=None
    ))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
        column="sale_date", strftime_format="%Y-%m-%d"
    ))

    vendas_landing_path = paths['vendas_landing']
    asset_name = "vendas_landing_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except (ValueError, KeyError):
        asset = datasource.add_query_asset(
            name=asset_name,
            query=f"SELECT * FROM read_csv_auto('{vendas_landing_path}')"
        )

    checkpoint = context.add_or_update_checkpoint(
        name="landing_vendas_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": suite_name,
        }],
    )

    logger.info("🔍 Running validation for: %s", vendas_landing_path)
    checkpoint_result = checkpoint.run()

    if not checkpoint_result.success:
        logger.error("❌ Validation FAILED! Quarantining data...")
        context.build_data_docs()
        quarantine_path = paths['vendas_quarantine']
        move_to_quarantine(raw_conn, vendas_landing_path, quarantine_path)
        logger.error("🛑 Pipeline stopped due to quality issues.")
        sys.exit(1)

    logger.info("✅ Landing data validated successfully!")
    context.build_data_docs()


def move_to_quarantine(conn, source_path, target_path):
    """Moves a file to Quarantine using DuckDB."""
    try:
        conn.execute(
            f"COPY (SELECT * FROM read_csv_auto('{source_path}')) "
            f"TO '{target_path}' (FORMAT 'PARQUET')"
        )
        logger.info("📥 Data moved to quarantine: %s", target_path)
    except Exception as err:  # pylint: disable=broad-exception-caught
        logger.error("Error during quarantine move: %s", err)


if __name__ == "__main__":
    validate_landing_data()
