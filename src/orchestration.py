"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""
import os
from dagster import (
    AssetExecutionContext, 
    Definitions, 
    asset, 
    AssetCheckResult, 
    asset_check,
    define_asset_job,
    ScheduleDefinition,
    MetadataValue,
    AssetKey,
    AssetSelection,
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from pathlib import Path

from utils.generate_clean_data import generate_clean_big_data
from utils.generate_dirty_data import generate_dirty_big_data
from validation.gx_validator import validate_layer

# --- 1. dbt Configuration ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        node_type = dbt_resource_props.get("resource_type")
        if node_type == "source":
            return AssetKey(["landing", dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)

@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def pureflow_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
    context.log.info("Generating dbt documentation (catalog.json)...")
    dbt.cli(["docs", "generate"], context=context).wait()

# --- 2. Landing Zone Generation (Python) ---

@asset(group_name="landing", compute_kind="python", key_prefix=["landing"])
def generate_clean_data(context):
    """Generates CLEAN synthetic data in the Landing Zone."""
    generate_clean_big_data()
    context.log.info("✅ CLEAN data generated in Landing Zone.")

@asset(group_name="landing", compute_kind="python", key_prefix=["landing"])
def generate_dirty_data(context):
    """Generates DIRTY synthetic data in the Landing Zone (for testing)."""
    generate_dirty_big_data()
    context.log.info("⚠️ DIRTY data generated in Landing Zone.")

# --- 3. Great Expectations Gates (Asset Checks) ---

def _handle_gx_result(success, report_url, check_name):
    return AssetCheckResult(
        passed=success,
        metadata={
            "status": "Quality Passed" if success else "Quality FAILED",
            "gx_report": MetadataValue.url(report_url),
        }
    )

# Attaching check to both landing assets
@asset_check(asset=generate_clean_data, name="gx_landing_clean_gate")
def gx_landing_clean_gate(context):
    success, report_url = validate_layer("landing")
    return _handle_gx_result(success, report_url, "Landing Clean")

@asset_check(asset=generate_dirty_data, name="gx_landing_dirty_gate")
def gx_landing_dirty_gate(context):
    success, report_url = validate_layer("landing")
    return _handle_gx_result(success, report_url, "Landing Dirty")

@asset_check(asset=AssetKey(["stg_vendas_bronze"]), name="gx_bronze_gate")
def gx_bronze_gate(context):
    success, report_url = validate_layer("bronze")
    return _handle_gx_result(success, report_url, "Bronze")

@asset_check(asset=AssetKey(["vendas_silver"]), name="gx_silver_gate")
def gx_silver_gate(context):
    success, report_url = validate_layer("silver")
    return _handle_gx_result(success, report_url, "Silver")

# --- 4. Definitions & Jobs ---

# Job 1: Clean Pipeline (Standard Flow)
clean_pipeline_job = define_asset_job(
    name="clean_pipeline_job",
    selection=AssetSelection.assets(generate_clean_data) | AssetSelection.assets(pureflow_dbt_assets)
)

# Job 2: Dirty Pipeline (Test Validation Failure)
dirty_pipeline_job = define_asset_job(
    name="dirty_pipeline_job",
    selection=AssetSelection.assets(generate_dirty_data) | AssetSelection.assets(pureflow_dbt_assets)
)

defs = Definitions(
    assets=[generate_clean_data, generate_dirty_data, pureflow_dbt_assets],
    asset_checks=[gx_landing_clean_gate, gx_landing_dirty_gate, gx_bronze_gate, gx_silver_gate],
    resources={"dbt": dbt},
    jobs=[clean_pipeline_job, dirty_pipeline_job],
    schedules=[
        ScheduleDefinition(
            name="daily_clean_schedule", job_name="clean_pipeline_job", cron_schedule="0 0 * * *"
        )
    ],
)
