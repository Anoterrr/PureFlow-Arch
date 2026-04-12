"""Module for validating raw data in Landing Zone using Great Expectations (GX)."""
import sys
import os
import duckdb

import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths, get_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_landing_data():
    """Validates raw data in the Landing Zone and moves to Quarantine."""
    s3_paths = get_s3_paths()
    local_paths = get_paths() # Get local filesystem paths
    factory = ConnectionFactory()
    context = get_gx_context()

    datasource, raw_conn = setup_gx_backend(
        context, "landing_datasource", factory
    )

    logger.info("🛡️ Starting Data Gatekeeper (Great Expectations)...")

    suite = get_or_create_suite(context, "vendas_landing_suite")
    _add_landing_expectations(suite)

    # Use local path for validation to avoid S3 credential issues in the container
    # The container mounts the project root at /app, so path is relative to /app
    vendas_landing_path = f"{local_paths[0]}/vendas.csv"
    
    logger.info("🔍 Local path for validation: %s", vendas_landing_path)

    asset_name = "vendas_landing_asset"
    # Delete old asset if it exists to force update the query
    try:
        datasource.delete_asset(asset_name)
    except Exception:
        pass

    asset = datasource.add_query_asset(
        name=asset_name,
        query=f"SELECT * FROM read_csv_auto('{vendas_landing_path}')"
    )

    # In GX 1.x, we use ValidationDefinitions
    batch_def_name = "vendas_batch_def"
    try:
        batch_definition = asset.get_batch_definition(batch_def_name)
    except (ValueError, KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_table(batch_def_name)
    
    validation_def_name = "vendas_landing_validation"
    # Delete old validation definition to ensure it uses the new batch definition
    try:
        context.validation_definitions.delete(validation_def_name)
    except Exception:
        pass

    from great_expectations.core.validation_definition import ValidationDefinition
    validation_def = context.validation_definitions.add(
        ValidationDefinition(
            name=validation_def_name,
            data=batch_definition,
            suite=suite
        )
    )

    logger.info("🔍 Running validation for: %s", vendas_landing_path)
    # Run validation directly using the validation definition
    validation_result = validation_def.run()

    if not validation_result.success:
        _handle_landing_failure(context, raw_conn, vendas_landing_path, s3_paths)

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
