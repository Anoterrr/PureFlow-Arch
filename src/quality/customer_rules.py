"""Domain-specific Validation Rules for Customers (Great Expectations)."""
import sys
import great_expectations as gx
from great_expectations.core.validation_definition import ValidationDefinition

from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_customer_bronze_quality():
    """Runs GX validation specifically for the Customer Bronze domain."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, raw_conn = setup_gx_backend(
        context, "customer_quality_datasource", factory
    )

    logger.info("🛡️ [Customer Gatekeeper] Running Great Expectations (Bronze)...")

    suite = get_or_create_suite(context, "customer_bronze_suite")
    _add_customer_bronze_expectations(suite)

    bronze_clientes_path = paths['clientes_bronze']
    try:
        asset = datasource.get_asset("customer_bronze_asset")
    except (ValueError, KeyError, LookupError):
        asset = datasource.add_query_asset(
            name="customer_bronze_asset",
            query=f"SELECT * FROM read_parquet('{bronze_clientes_path}')"
        )

    # In GX 1.x, we use ValidationDefinitions
    batch_def_name = "customer_bronze_batch_def"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except (ValueError, KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_table(batch_def_name)
    
    validation_def_name = "customer_bronze_validation"
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
        _handle_customer_failure(context, raw_conn, bronze_clientes_path, paths)

    logger.info("✅ [Customer Bronze Quality] All checks passed.")
    context.build_data_docs()

def validate_customer_silver_quality():
    """Validates Customer Silver data."""
    paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, _ = setup_gx_backend(
        context, "customer_silver_datasource", factory
    )

    logger.info("🛡️ [Customer Gatekeeper] Running Great Expectations (Silver)...")

    suite = get_or_create_suite(context, "customer_silver_suite")
    _add_customer_silver_expectations(suite)

    silver_clientes_path = paths['clientes_silver']
    try:
        asset = datasource.get_asset("customer_silver_asset")
    except (ValueError, KeyError, LookupError):
        asset = datasource.add_query_asset(
            name="customer_silver_asset",
            query=f"SELECT * FROM read_parquet('{silver_clientes_path}/*.parquet')"
        )

    # In GX 1.x, we use ValidationDefinitions
    batch_def_name = "customer_silver_batch_def"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except (ValueError, KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_table(batch_def_name)
    
    validation_def_name = "customer_silver_validation"
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
        logger.error("❌ [Customer Silver Quality] Validation failed! Stopping.")
        context.build_data_docs()
        sys.exit(1)

    logger.info("✅ [Customer Silver Quality] All checks passed.")
    context.build_data_docs()

def _add_customer_bronze_expectations(suite):
    """Adds specific expectations for Customer Bronze."""
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="email")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email", regex=r"^[\w\.-]+@[\w\.-]+\.\w+$"
        )
    )

def _add_customer_silver_expectations(suite):
    """Adds specific expectations for Customer Silver."""
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="name")
    )

def _handle_customer_failure(context, conn, source_path, paths):
    """Logic for handling customer validation failure."""
    logger.error("❌ [Customer Quality] Validation failed! Quarantining...")
    context.build_data_docs()
    quarantine_path = paths['clientes_quarantine']
    conn.execute(
        f"COPY (SELECT * FROM read_parquet('{source_path}')) "
        f"TO '{quarantine_path}' (FORMAT 'PARQUET')"
    )
    logger.error("🛑 [Customer Quality] Circuit breaker engaged: %s",
                 quarantine_path)
    sys.exit(1)
