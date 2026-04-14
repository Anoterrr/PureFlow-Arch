"""Domain-specific Validation Rules for Sales (Great Expectations)."""
import sys
import great_expectations as gx
from great_expectations.core.validation_definition import ValidationDefinition

from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_bronze_quality():
    """Runs GX validation specifically for the Bronze domain."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, raw_conn = setup_gx_backend(
        context, "sales_quality_datasource", factory
    )

    logger.info("🛡️ [Sales Gatekeeper] Running Great Expectations...")

    suite = get_or_create_suite(context, "sales_quality_suite")
    _add_sales_expectations(suite)

    bronze_vendas_path = paths['vendas_bronze']
    try:
        asset = datasource.get_asset("sales_bronze_asset")
    except (ValueError, KeyError, LookupError):
        asset = datasource.add_query_asset(
            name="sales_bronze_asset",
            query=f"SELECT * FROM read_parquet('{bronze_vendas_path}')"
        )

    # In GX 1.x, we use ValidationDefinitions
    batch_def_name = "sales_bronze_batch_def"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except (ValueError, KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_table(batch_def_name)
    
    validation_def_name = "sales_bronze_validation"
    try:
        context.validation_definitions.delete(validation_def_name)
    except Exception:
        pass

    validation_def = context.validation_definitions.add(
        ValidationDefinition(
            name=validation_def_name,
            data=batch_definition,
            suite=suite
        )
    )

    result = validation_def.run()

    if not result.success:
        _handle_bronze_failure(context, raw_conn, bronze_vendas_path, paths)

    logger.info("✅ [Sales Quality] All checks passed.")
    context.build_data_docs()

def _add_sales_expectations(suite):
    """Adds specific expectations to the suite."""
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

def _handle_bronze_failure(context, conn, source_path, paths):
    """Logic for handling validation failure."""
    logger.error("❌ [Sales Quality] Validation failed! Quarantining...")
    context.build_data_docs()
    quarantine_path = paths['vendas_quarantine']
    conn.execute(
        f"COPY (SELECT * FROM read_parquet('{source_path}')) "
        f"TO '{quarantine_path}' (FORMAT 'PARQUET')"
    )
    logger.error("🛑 [Sales Quality] Circuit breaker engaged: %s",
                 quarantine_path)
    sys.exit(1)

if __name__ == "__main__":
    validate_bronze_quality()
