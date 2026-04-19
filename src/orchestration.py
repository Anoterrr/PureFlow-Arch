"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""

import os
from pathlib import Path

from dagster import (
    AssetKey,
    AssetSelection,
    Definitions,
    asset,
    define_asset_job,
    load_assets_from_package_name,
    load_assets_from_current_module,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

# Import data generators for the landing assets
from utils.generate_clean_data import generate_clean_big_data
from utils.generate_dirty_data import generate_dirty_big_data

# --- 1. dbt Configuration with Lineage Mapping ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))


# This translator connects dbt sources to our Factory assets
class PureFlowDbtTranslator(DagsterDbtTranslator):
    """Custom translator for PureFlow dbt assets to map sources and groups."""

    def get_asset_key(self, dbt_resource_props):
        """Maps dbt source names to Dagster AssetKeys."""
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            return AssetKey(dbt_resource_props["name"])
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
def pureflow_dbt_assets(context, dbt_resource: DbtCliResource):
    """Assets representing dbt models in the transformation pipeline."""
    yield from dbt_resource.cli(["run"], context=context).stream()


# --- 2. Landing Zone Generation Assets ---


@asset(group_name="landing", compute_kind="python")
def generate_clean_data(context):
    """Generates CLEAN synthetic data in the Landing Zone."""
    from utils.generate_clean_data import generate_clean_big_data
    generate_clean_big_data()
    context.log.info("✅ CLEAN data generated successfully.")


@asset(group_name="landing", compute_kind="python")
def generate_dirty_data(context):
    """Generates DIRTY synthetic data to test Quality Gates."""
    from utils.generate_dirty_data import generate_dirty_big_data
    generate_dirty_big_data()
    context.log.info("⚠️ DIRTY data generated. Quality Gates should trigger.")


# --- 3. Jobs & Definitions ---

# Main Transformation Pipeline (Now includes generation and transformation)
pureflow_pipeline_job = define_asset_job(  # pylint: disable=assignment-from-no-return
    name="pureflow_pipeline_job", selection=AssetSelection.all()
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
    resources={"dbt": dbt},
    jobs=[pureflow_pipeline_job]
)
