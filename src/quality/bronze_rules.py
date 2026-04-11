"""Domain-specific Validation Rules for Sales (Great Expectations)."""
import sys
import great_expectations as gx

from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context

# pylint: disable=too-many-locals, duplicate-code
def validate_bronze_quality():
    """Runs GX validation specifically for the Bronze domain."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource_name = "sales_quality_datasource"
    try:
        datasource = context.get_datasource(datasource_name)
    except (ValueError, KeyError):
        datasource = context.sources.add_duckdb(name=datasource_name)

    # pylint: disable=protected-access
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    logger.info("🛡️ [Sales Gatekeeper] Running Great Expectations...")

    suite_name = "sales_quality_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except (ValueError, KeyError):
        suite = context.add_expectation_suite(
            expectation_suite_name=suite_name
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="sale_value", min_value=0.0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
            column="sale_date", strftime_format="%Y-%m-%d"
        )
    )

    bronze_vendas_path = paths['vendas_bronze']
    asset_name = "sales_bronze_asset"
    try:
        asset = datasource.get_asset(asset_name)
    except (ValueError, KeyError):
        asset = datasource.add_query_asset(
            name=asset_name,
            query=f"SELECT * FROM read_parquet('{bronze_vendas_path}')"
        )

    checkpoint = context.add_or_update_checkpoint(
        name="sales_quality_checkpoint",
        validations=[{
            "batch_request": asset.build_batch_request(),
            "expectation_suite_name": suite_name
        }],
    )

    result = checkpoint.run()

    if not result.success:
        logger.error("❌ [Sales Quality] Validation failed! Quarantining...")
        context.build_data_docs()
        quarantine_path = paths['vendas_quarantine']
        raw_conn.execute(
            f"COPY (SELECT * FROM read_parquet('{bronze_vendas_path}')) "
            f"TO '{quarantine_path}' (FORMAT 'PARQUET')"
        )
        logger.error("🛑 [Sales Quality] Circuit breaker engaged: %s",
                     quarantine_path)
        sys.exit(1)

    logger.info("✅ [Sales Quality] All checks passed.")
    context.build_data_docs()

if __name__ == "__main__":
    validate_bronze_quality()
