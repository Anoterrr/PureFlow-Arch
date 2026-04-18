"""Generic validation engine using Great Expectations (GX)."""
import os
import great_expectations as gx
from typing import List, Dict, Any, Optional

from core.connection import ConnectionFactory
from core.logger import logger
from core.quality import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_data(
    path: str, 
    expectations: List[Dict[str, Any]], 
    format: str = "parquet",
    suite_name: str = "dynamic_suite"
) -> (bool, str):
    """
    Generic execution engine for validation.
    Returns (success: bool, report_url: str).
    """
    factory = ConnectionFactory()
    context = get_gx_context()
    
    # Setup backend (DuckDB-based for GX)
    datasource_name = f"ds_{suite_name}"
    datasource, raw_conn = setup_gx_backend(context, datasource_name, factory)
    
    logger.info(f"🛡️ [Validator] Validating data at {path}...")
    
    success = True
    try:
        # 1. Setup Suite and Add Expectations
        suite = get_or_create_suite(context, suite_name)
        
        for check in expectations:
            # Map simple dictionaries to GX expectations if needed, 
            # or assume they are already GX expectation objects.
            # Here we support a simple dict format: {"expectation": "name", "kwargs": {...}}
            exp_name = check.get("expectation")
            kwargs = check.get("kwargs", {})
            
            # Use getattr to find the expectation class in gx.expectations
            try:
                exp_class = getattr(gx.expectations, exp_name)
                suite.add_expectation(exp_class(**kwargs))
            except Exception as e:
                logger.warning(f"⚠️ [Validator] Could not add expectation {exp_name}: {str(e)}")

        # 2. Setup Data Asset
        asset_name = f"asset_{suite_name}"
        try: datasource.delete_asset(asset_name)
        except: pass
        
        # Read function for GX query
        read_func = "read_csv_auto" if format.lower() == "csv" else ("read_parquet" if format.lower() == "parquet" else "read_json_auto")
        asset = datasource.add_query_asset(
            name=asset_name, 
            query=f"SELECT * FROM {read_func}('{path}')"
        )
        batch_def = asset.add_batch_definition_whole_table(f"batch_{suite_name}")
        
        # 3. Run Validation
        from great_expectations.core.validation_definition import ValidationDefinition
        val_name = f"val_{suite_name}"
        try: context.validation_definitions.delete(val_name)
        except: pass
            
        val_def = context.validation_definitions.add(
            ValidationDefinition(name=val_name, data=batch_def, suite=suite)
        )
        
        result = val_def.run()
        success = result.success
        
        context.build_data_docs()
        
    except Exception as e:
        logger.error(f"❌ [Validator] Validation failed: {str(e)}")
        success = False
    finally:
        raw_conn.close()

    # Path to the generated Data Docs
    report_path = os.path.abspath("gx/uncommitted/data_docs/local_site/index.html")
    report_url = f"file://{report_path}"
    
    return success, report_url
