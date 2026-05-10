"""Generic validation engine using Great Expectations (GX)."""

import os
from typing import Any, Dict, List, Optional, Tuple

import great_expectations as gx
from great_expectations.core.validation_definition import ValidationDefinition

from core.connection import ConnectionFactory
from core.logger import logger
from core.quality import get_gx_context, get_or_create_suite, setup_gx_backend


def validate_data(
    path: str,
    expectations: List[Dict[str, Any]],
    data_format: str = "parquet",
    suite_name: str = "dynamic_suite",
    context: Optional[Any] = None,
) -> Tuple[bool, str, Optional[str]]:
    """
    Generic execution engine for validation.
    Returns (success: bool, report_url: str, error_message: Optional[str]).
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    factory = ConnectionFactory()
    
    # Use provided context or get/create a new one
    if context is None:
        context = get_gx_context()

    # Setup backend (DuckDB-based for GX)
    datasource_name = f"ds_{suite_name}"
    datasource, raw_conn, db_path = setup_gx_backend(context, datasource_name, factory)

    logger.info("🛡️ [Validator] Validating data at %s...", path)

    success = True
    error_msg = None
    
    # Default report URL (index page)
    web_report_url = "http://localhost:8082/index.html"
    base_docs_path = os.path.abspath("gx/uncommitted/data_docs/local_site")

    try:
        # 1. Setup Suite and Add Expectations
        suite = get_or_create_suite(context, suite_name)
        
        # Clear existing expectations to ensure only the ones defined in the asset are used
        for expectation in list(suite.expectations):
            suite.delete_expectation(expectation)

        for check in expectations:
            exp_name = check.get("expectation")
            kwargs = check.get("kwargs", {})

            try:
                exp_class = getattr(gx.expectations, exp_name)
                suite.add_expectation(exp_class(**kwargs))
            except (AttributeError, TypeError) as e:
                logger.warning(
                    "⚠️ [Validator] Could not add expectation %s: %s", exp_name, str(e)
                )

        # 2. Setup Data Asset via VIEW
        # We create a VIEW in the shared DuckDB instance and point GX to it using add_table_asset.
        # This is more stable than add_query_asset as it avoids complex subquery nesting.
        asset_name = f"view_{suite_name}"
        
        # Read function for GX query
        fmt = data_format.lower()
        if fmt == "delta":
            read_func = "delta_scan"
        else:
            read_func = (
                "read_csv_auto"
                if fmt == "csv"
                else ("read_parquet" if fmt == "parquet" else "read_json_auto")
            )

        # Create/Replace the view in the shared DB
        try:
            raw_conn.execute(f"DROP VIEW IF EXISTS {asset_name}")
            raw_conn.execute(
                f"CREATE VIEW {asset_name} AS SELECT * FROM {read_func}(?)", 
                [path]
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create validation view {asset_name}: {str(e)}") from e

        # Register the Table Asset in GX
        try:
            # Clean up existing asset if it exists in the metadata
            existing_assets = [a.name for a in datasource.assets]
            if asset_name in existing_assets:
                datasource.delete_asset(asset_name)
        except Exception:
            pass

        asset = datasource.add_table_asset(name=asset_name, table_name=asset_name)
        batch_def = asset.add_batch_definition_whole_table(f"batch_{suite_name}")

        # 3. Run Validation
        val_name = f"val_{suite_name}"
        try:
            try:
                context.validation_definitions.delete(val_name)
            except Exception:
                pass

            val_def = context.validation_definitions.add(
                ValidationDefinition(name=val_name, data=batch_def, suite=suite)
            )
        except Exception as e:
            raise RuntimeError(f"Failed to manage GX ValidationDefinition: {str(e)}") from e

        logger.info("⚡ [Validator] Running GX validation execution...")
        result = val_def.run()
        success = result.success
        if not success:
            error_msg = "Data quality validation failed (check GX report for details)."

        # Ensure Data Docs are built
        context.build_data_docs()

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("❌ [Validator] Validation failed: %s", str(e))
        success = False
        error_msg = f"Technical validation failure: {str(e)}"
    finally:
        # 1. Close the raw connection
        raw_conn.close()
        
        # 2. Attempt to cleanup the temporary DB file
        # We need to ensure GX has also released its connections. 
        # Since datasource is ephemeral in this context, the engine should close.
        try:
            if os.path.exists(db_path):
                os.remove(db_path)
                # Also remove WAL files if they exist
                for ext in [".wal", ".tmp"]:
                    if os.path.exists(db_path + ext):
                        os.remove(db_path + ext)
        except Exception as e:
            logger.debug("Non-critical: Could not delete temporary DB %s: %s", db_path, str(e))

    # Dynamic path to the generated Data Docs for this specific suite
    report_path = os.path.join(base_docs_path, "validations", suite_name)

    # Try to find the most recent HTML report
    latest_report_file = os.path.join(base_docs_path, "index.html")
    try:
        if os.path.exists(report_path):
            html_files = []
            for root, _, files in os.walk(report_path):
                for file in files:
                    if file.endswith(".html"):
                        html_files.append(os.path.join(root, file))

            if html_files:
                latest_report_file = max(html_files, key=os.path.getmtime)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Could not find latest report: %s", str(e))

    # Convert local file path to HTTP URL
    if os.path.exists(latest_report_file):
        relative_path = os.path.relpath(latest_report_file, base_docs_path)
        web_report_url = f"http://localhost:8082/{relative_path}"

    return success, web_report_url, error_msg
