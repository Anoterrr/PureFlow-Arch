"""Domain-specific Validation Rules for Silver (Great Expectations)."""
import sys
import great_expectations as gx
from great_expectations.core.validation_definition import ValidationDefinition

from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_silver_quality():
    """Validates Silver data post-business rules application."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, _ = setup_gx_backend(
        context, "silver_quality_datasource", factory
    )

    logger.info("🛡️ [Silver Gatekeeper] Running Great Expectations...")

    suite = get_or_create_suite(context, "silver_quality_suite")
    _add_silver_expectations(suite)

    silver_vendas_path = paths['vendas_silver']
    try:
        asset = datasource.get_asset("sales_silver_asset")
    except (ValueError, KeyError, LookupError):
        asset = datasource.add_query_asset(
            name="sales_silver_asset",
            query=f"SELECT * FROM read_parquet('{silver_vendas_path}/*.parquet')"
        )

    # In GX 1.x, we use ValidationDefinitions
    batch_def_name = "sales_silver_batch_def"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except (ValueError, KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_table(batch_def_name)
    
    validation_def_name = "sales_silver_validation"
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
        logger.error("❌ [Silver Quality] Validation failed! Stopping.")
        context.build_data_docs()
        sys.exit(1)

    logger.info("✅ [Silver Quality] All checks passed.")
    context.build_data_docs()

def _add_silver_expectations(suite):
    """Business logic expectations for Silver layer."""
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

if __name__ == "__main__":
    validate_silver_quality()
