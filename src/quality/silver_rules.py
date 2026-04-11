"""Domain-specific Validation Rules for Silver (Great Expectations)."""
import sys

from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context
import great_expectations as gx

def validate_silver_quality():
    """Validates Silver data post-business rules application."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource_name = "silver_quality_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except (ValueError, KeyError):
        datasource = context.sources.add_duckdb(name=datasource_name)

    # pylint: disable=protected-access
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ [Silver Gatekeeper] Running Great Expectations...")

    suite_name = "silver_quality_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except (ValueError, KeyError):
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)

    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="_quality_status", value_set=["CLEANED"]
    ))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="sale_value", min_value=0.01
        )
    )

    silver_vendas_path = paths['vendas_silver']
    asset_name = "sales_silver_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except (ValueError, KeyError):
        asset = datasource.add_query_asset(
            name=asset_name,
            query=f"SELECT * FROM read_parquet('{silver_vendas_path}/*.parquet')"
        )

    checkpoint = context.add_or_update_checkpoint(
        name="silver_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": suite_name
        }],
    )

    result = checkpoint.run()

    if not result.success:
        logger.error("❌ [Silver Quality] Validation failed! Stopping.")
        context.build_data_docs()
        sys.exit(1)

    logger.info("✅ [Silver Quality] All checks passed.")
    context.build_data_docs()

if __name__ == "__main__":
    validate_silver_quality()
