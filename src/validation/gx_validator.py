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
) -> Tuple[bool, str, Optional[str]]:
    """
    Generic execution engine for validation.
    Returns (success: bool, report_url: str, error_message: Optional[str]).
    """
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    factory = ConnectionFactory()
    context = get_gx_context()

    # Setup backend (DuckDB-based for GX)
    datasource_name = f"ds_{suite_name}"
    datasource, raw_conn = setup_gx_backend(context, datasource_name, factory)

    logger.info("🛡️ [Validator] Validating data at %s...", path)

    success = True
    error_msg = None
    try:
        # 1. Setup Suite and Add Expectations
        suite = get_or_create_suite(context, suite_name)
        
        # Clear existing expectations to ensure only the ones defined in the asset are used
        # This prevents old/deleted expectations from persisting in the JSON suites
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

        # 2. Setup Data Asset
        asset_name = f"asset_{suite_name}"
        try:
            # Check if asset exists before deleting
            existing_assets = [a.name for a in datasource.assets]
            if asset_name in existing_assets:
                datasource.delete_asset(asset_name)
        except Exception as e:
            logger.debug("Non-critical error deleting asset %s: %s", asset_name, str(e))

        # Read function for GX query
        fmt = data_format.lower()
        read_func = (
            "read_csv_auto"
            if fmt == "csv"
            else ("read_parquet" if fmt == "parquet" else "read_json_auto")
        )

        # Check if file exists before running GX to provide better error messages
        try:
            # We use f-string for function name, but path is parameterized
            raw_conn.execute(f"SELECT 1 FROM {read_func}(?) LIMIT 1", [path])  # nosec B608
        except Exception as e:
            raise FileNotFoundError(f"Data not accessible at {path}. Error: {str(e)}") from e

        asset = datasource.add_query_asset(
            name=asset_name,
            query=f"SELECT * FROM {read_func}('{path}')"  # nosec B608
        )
        batch_def = asset.add_batch_definition_whole_table(f"batch_{suite_name}")

        # 3. Run Validation
        val_name = f"val_{suite_name}"
        try:
            # Try to get existing or create new one to avoid 'already exists' errors
            try:
                val_def = context.validation_definitions.get(val_name)
                # Update with new data and suite
                val_def.data = batch_def
                val_def.suite = suite
                # In some GX versions we might need to save/persist changes
            except (ValueError, KeyError, LookupError):
                val_def = context.validation_definitions.add(
                    ValidationDefinition(name=val_name, data=batch_def, suite=suite)
                )
        except Exception as e:
            # Fallback: if get/add fails, try one last time with a clean delete attempt
            logger.warning("🔄 [Validator] Conflict with ValidationDefinition %s, attempting fallback...", val_name)
            try:
                # Force delete and re-add
                for v in list(context.validation_definitions):
                    if v.name == val_name:
                        context.validation_definitions.delete(val_name)
                val_def = context.validation_definitions.add(
                    ValidationDefinition(name=val_name, data=batch_def, suite=suite)
                )
            except Exception as final_e:
                raise RuntimeError(f"Failed to manage GX ValidationDefinition: {str(final_e)}") from final_e

        result = val_def.run()
        success = result.success
        if not success:
            error_msg = "Data quality validation failed (check GX report for details)."

        context.build_data_docs()

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("❌ [Validator] Validation failed: %s", str(e))
        success = False
        error_msg = f"Technical validation failure: {str(e)}"
    finally:
        raw_conn.close()

    # Dynamic path to the generated Data Docs for this specific suite
    report_path = os.path.abspath(
        f"gx/uncommitted/data_docs/local_site/validations/{suite_name}"
    )

    # Try to find the most recent HTML report
    latest_report = (
        f"file://{os.path.abspath('gx/uncommitted/data_docs/local_site/index.html')}"
    )
    try:
        if os.path.exists(report_path):
            html_files = []
            for root, _, files in os.walk(report_path):
                for file in files:
                    if file.endswith(".html"):
                        html_files.append(os.path.join(root, file))

            if html_files:
                latest_report = f"file://{max(html_files, key=os.path.getmtime)}"
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Could not find latest report: %s", str(e))

    return success, latest_report, error_msg
