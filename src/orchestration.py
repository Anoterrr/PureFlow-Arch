"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""

import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    asset,
    define_asset_job,
    load_assets_from_current_module,
    load_assets_from_package_name,
    MetadataValue,
    RunConfig,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

from core.engine import PureFlowEngine
from validation.gx_validator import validate_data

# Import data generators and corruptors
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_corrupt_data import corrupt_bronze_layer, corrupt_landing_zone
from utils.generate_dirty_data import generate_dirty_big_data

# --- 1. dbt Configuration with Lineage Mapping ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt_resource = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


# This translator connects dbt sources to our Factory assets
class PureFlowDbtTranslator(DagsterDbtTranslator):
    """Custom translator for PureFlow dbt assets to map sources and groups."""

    def get_asset_key(self, dbt_resource_props):
        """Maps dbt source names to Dagster AssetKeys."""
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            # Map dbt source to the GX validation asset of the silver layer
            # source_name is the table name in dbt
            source_name = dbt_resource_props["name"]
            return AssetKey(f"gx_{source_name}")
        return super().get_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props):
        """Assigns 'gold' group to dbt models."""
        # Only put actual models in the 'gold' group
        if dbt_resource_props.get("resource_type") == "model":
            return "gold"
        return super().get_group_name(dbt_resource_props)


@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=PureFlowDbtTranslator(),
)
def pureflow_dbt_assets(context, dbt: DbtCliResource):
    """Assets representing dbt models in the transformation pipeline."""
    yield from dbt.cli(["run"], context=context).stream()


@asset(
    name="gx_sales_summary",
    group_name="gold",
    deps=[AssetKey("sales_summary")],
    compute_kind="gx",
    config_schema={"execution_date": str},
)
def gx_sales_summary(context):
    """Quality gate for the final Gold Layer asset."""
    execution_date = context.op_config.get("execution_date")
    engine = PureFlowEngine(execution_date=execution_date)
    
    # Path is defined in dbt_project.yml / sales_summary.sql
    target_path = f"s3://gold/sales_summary/dt={execution_date}/sales_summary.parquet"
    
    success, report_url, error_msg = validate_data(
        path=target_path,
        expectations=[
            {"expectation": "ExpectColumnValuesToNotBeNull", "kwargs": {"column": "total_revenue"}},
            {"expectation": "ExpectColumnValuesToBeGreaterThan", "kwargs": {"column": "total_orders", "value": 0}},
        ],
        data_format="parquet",
        suite_name="suite_gx_sales_summary",
    )
    if not success:
        raise ValueError(
            f"Gold validation failed. \n"
            f"Reason: {error_msg} \n"
            f"Report: {report_url}"
        )
    return MetadataValue.url(report_url)


# --- 2. Data State Management Assets (Separated from main pipeline) ---


@asset(group_name="data_generators", compute_kind="python", config_schema={"execution_date": str})
def generate_clean_data(context):
    """Generates CLEAN synthetic data in the Landing Zone."""
    execution_date = context.op_config.get("execution_date")
    generate_clean_big_data(execution_date=execution_date)
    context.log.info(f"✅ CLEAN data generated successfully for {execution_date}.")


@asset(group_name="data_generators", compute_kind="python", config_schema={"execution_date": str})
def generate_dirty_data(context):
    """Generates DIRTY synthetic data to test Quality Gates."""
    execution_date = context.op_config.get("execution_date")
    generate_dirty_big_data(execution_date=execution_date)
    context.log.info(f"⚠️ DIRTY data generated for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python", config_schema={"execution_date": str})
def inject_corrupt_landing(context):
    """Intentional data corruption at Landing Zone."""
    execution_date = context.op_config.get("execution_date")
    corrupt_landing_zone(execution_date=execution_date)
    context.log.warning(f"🧨 Landing Zone data corrupted for {execution_date}.")


@asset(group_name="test_quality", compute_kind="python", config_schema={"execution_date": str})
def inject_corrupt_bronze(context):
    """Intentional data corruption at Bronze Layer."""
    execution_date = context.op_config.get("execution_date")
    corrupt_bronze_layer(execution_date=execution_date)
    context.log.warning(f"🧨 Bronze Layer data corrupted for {execution_date}.")


# --- 3. Jobs & Definitions ---

# Default configuration to avoid 404s for today's date if data isn't generated
DEFAULT_DATE = "2026-04-19"

# Config for core business logic assets (Bronze, Silver, Gold GX)
core_pipeline_ops_config = {
    "stg_customers_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_stg_customers_bronze_source": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_stg_customers_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "customers_silver": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_customers_silver": {"config": {"execution_date": DEFAULT_DATE}},
    "stg_sales_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_stg_sales_bronze_source": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_stg_sales_bronze": {"config": {"execution_date": DEFAULT_DATE}},
    "sales_silver": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_sales_silver": {"config": {"execution_date": DEFAULT_DATE}},
    "gx_sales_summary": {"config": {"execution_date": DEFAULT_DATE}},
}

# Config for data corruption tools
test_quality_ops_config = {
    "inject_corrupt_landing": {"config": {"execution_date": DEFAULT_DATE}},
    "inject_corrupt_bronze": {"config": {"execution_date": DEFAULT_DATE}},
}

# Main Transformation Pipeline (Excludes generators and corruption tools)
pureflow_pipeline_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="pureflow_pipeline_job",
    selection=AssetSelection.all() - AssetSelection.groups("data_generators", "test_quality"),
    config={"ops": core_pipeline_ops_config},
)

# Specific job to test Quality Gates by corrupting and then running the pipeline
quality_test_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="quality_test_job",
    selection=(
        AssetSelection.groups("test_quality")
        | AssetSelection.groups("bronze", "silver", "gold")
    ),
    config={"ops": {**core_pipeline_ops_config, **test_quality_ops_config}},
)

# Dynamic asset discovery:
# 1. Scans the 'pipelines' package for assets created via Factory
# 2. Scans the current module for dbt assets and local generator assets
all_assets = [
    *load_assets_from_package_name("pipelines"),
    *load_assets_from_current_module(),
]

defs = Definitions(
    assets=all_assets,
    resources={"dbt": dbt_resource},
    jobs=[pureflow_pipeline_job, quality_test_job],
)
