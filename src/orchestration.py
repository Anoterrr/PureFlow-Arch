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
    MetadataValue
)
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

from ingestion.sales_ingest import SalesIngestor
from core.config import BASE_DATE
from core.logger import logger
from validation.gx_validator import validate_landing_data

# --- 1. dbt Configuration ---
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt").resolve()
dbt = DbtCliResource(project_dir=os.fspath(DBT_PROJECT_DIR))

# Load all dbt assets (Bronze, Silver, Gold)
@dbt_assets(manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"))
def pureflow_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()

# --- 2. Ingestion Asset (Landing -> Bronze) ---
@asset(group_name="ingestion", compute_kind="python")
def raw_sales_ingest(context):
    """Ingests raw Sales CSV from Landing to Bronze via DuckDB."""
    ingestor = SalesIngestor()
    ingestor.ingest()
    context.log.info("✅ Sales data successfully moved to Bronze layer.")

# --- 3. Great Expectations Integration (Replacing Asset Checks with GX reports) ---
@asset_check(asset=raw_sales_ingest, name="gx_landing_validation")
def gx_landing_validation(context):
    """Runs Great Expectations validation and generates Data Docs."""
    try:
        # Triggering the original GX validation logic
        validate_landing_data()
        
        # Path to the generated Data Docs (for the UI)
        docs_path = os.path.abspath("gx/uncommitted/data_docs/local_site/index.html")
        
        return AssetCheckResult(
            passed=True, 
            metadata={
                "report_url": MetadataValue.url(f"file://{docs_path}"),
                "status": "Quality Gates Passed (GX)"
            }
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False, 
            metadata={"error": str(e)}
        )

# --- 4. Definitions ---
defs = Definitions(
    assets=[raw_sales_ingest, pureflow_dbt_assets],
    asset_checks=[gx_landing_validation],
    resources={
        "dbt": dbt,
    },
    jobs=[define_asset_job(name="full_pipeline_job")],
    schedules=[
        ScheduleDefinition(
            name="daily_pipeline_schedule",
            job_name="full_pipeline_job",
            cron_schedule="0 0 * * *",
        )
    ],
)
