"""Module for multi-purpose validation using Great Expectations (GX)."""
import os
import great_expectations as gx
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger
from quality.utils import get_gx_context, setup_gx_backend, get_or_create_suite

def validate_layer(layer_name: str):
    """Execution engine for layer-specific validation. Returns (success, report_url)."""
    s3_paths = get_s3_paths()
    factory = ConnectionFactory()
    context = get_gx_context()
    datasource, raw_conn = setup_gx_backend(context, f"{layer_name}_datasource", factory)
    
    logger.info("🛡️ [%s Gate] Purpose-driven Validation starting...", layer_name.upper())
    
    success = True
    try:
        if layer_name == "landing":
            success = _run_gx(context, datasource, s3_paths['vendas_landing'], "vendas_landing_tech", _tech_landing_expectations, is_csv=True)
        elif layer_name == "bronze":
            success = _run_gx(context, datasource, s3_paths['vendas_bronze'], "vendas_bronze_tech", _tech_bronze_expectations)
        elif layer_name == "silver":
            gold_path = f"{s3_paths['sales_summary']}sales_summary.parquet"
            success = _run_gx(context, datasource, gold_path, "sales_gold_business_rules", _business_rules_gold_expectations)
        
        context.build_data_docs()
    finally:
        raw_conn.close()

    # Path to the generated Data Docs
    # Em Docker, o caminho absoluto no host pode variar, mas passamos o caminho relativo ao projeto
    report_path = os.path.abspath("gx/uncommitted/data_docs/local_site/index.html")
    report_url = f"file://{report_path}"
    
    return success, report_url

def _run_gx(context, datasource, path, suite_name, expectation_func, is_csv=False):
    suite = get_or_create_suite(context, suite_name)
    expectation_func(suite)
    
    asset_name = f"asset_{suite_name}"
    try: datasource.delete_asset(asset_name)
    except: pass
    
    read_func = "read_csv_auto" if is_csv else ("read_parquet" if path.endswith(".parquet") else "read_json_auto")
    asset = datasource.add_query_asset(name=asset_name, query=f"SELECT * FROM {read_func}('{path}')")
    batch_def = asset.add_batch_definition_whole_table(f"batch_{suite_name}")
    
    val_name = f"val_{suite_name}"
    try: context.validation_definitions.delete(val_name)
    except: pass
        
    from great_expectations.core.validation_definition import ValidationDefinition
    val_def = context.validation_definitions.add(ValidationDefinition(name=val_name, data=batch_def, suite=suite))
    
    result = val_def.run()
    return result.success

# --- Expectations Definitions ---
def _tech_landing_expectations(suite):
    suite.add_expectation(gx.expectations.ExpectTableColumnCountToEqual(value=5))

def _tech_bronze_expectations(suite):
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))

def _business_rules_gold_expectations(suite):
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="avg_ticket", min_value=10.0, max_value=10000.0
    ))
