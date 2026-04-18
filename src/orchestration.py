"""Modern Orchestration for PureFlow-Arch using Dagster (Asset-Based)."""
from dagster import (
    Definitions, 
    asset, 
    define_asset_job,
    AssetSelection,
)
from dagster_dbt import DbtCliResource, dbt_assets
import os
from pathlib import Path

# Import our new Factory-based assets
from pipelines.sales import stg_vendas_bronze, vendas_silver
from pipelines.customers import stg_clientes_bronze, clientes_silver

# --- 1. dbt Configuration ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def pureflow_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

# --- 2. Jobs ---
all_assets = [
    stg_vendas_bronze, 
    vendas_silver, 
    stg_clientes_bronze, 
    clientes_silver, 
    pureflow_dbt_assets
]

pureflow_pipeline_job = define_asset_job(
    name="pureflow_pipeline_job",
    selection=AssetSelection.all()
)

# --- 3. Definitions ---
defs = Definitions(
    assets=all_assets,
    resources={"dbt": dbt},
    jobs=[pureflow_pipeline_job]
)
