"""Factory Pattern for creating Data Pipelines as Dagster Assets."""
from typing import List, Dict, Any, Optional
from dagster import asset, AssetKey, AssetCheckResult, MetadataValue, AssetsDefinition
import os

from core.engine import PureFlowEngine
from validation.gx_validator import validate_data
from core.logger import logger

class DataPipelineFactory:
    """Creates standardized Dagster Assets for data ingestion and transformation."""

    @staticmethod
    def load_sql(path: str) -> str:
        """Helper to read SQL files from a given path."""
        with open(path, "r") as f:
            return f.read()

    @staticmethod
    def create_asset(
        name: str,
        source: Dict[str, str],
        target: Dict[str, str],
        group_name: str = "default",
        sql_transform: Optional[str] = None,
        expectations: Optional[List[Dict[str, Any]]] = None,
        depends_on: Optional[List[str]] = None
    ) -> AssetsDefinition:
        """
        Generates a Dagster Asset that moves data, transforms it, and validates it.
        """
        
        # Define dependencies (Lineage)
        deps = [AssetKey([dep]) for dep in depends_on] if depends_on else []

        @asset(
            name=name,
            group_name=group_name,
            deps=deps,
            compute_kind="duckdb"
        )
        def generated_asset(context):
            # 1. Initialize Engine
            # We can get execution_date from Dagster's context partition if needed
            engine = PureFlowEngine()
            
            # 2. Execute Data Movement & Transformation
            result = engine.execute_move_and_transform(
                source_path=source["path"],
                source_format=source["format"],
                target_path=target["path"],
                target_format=target["format"],
                sql_transform=sql_transform
            )
            
            # 3. Add Metadata to Dagster
            context.add_output_metadata({
                "rows_processed": MetadataValue.int(result["row_count"]),
                "target_path": MetadataValue.path(result["target_path"]),
                "status": "Success"
            })
            
            # 4. Optional Validation (Great Expectations)
            if expectations:
                success, report_url = validate_data(
                    path=result["target_path"],
                    expectations=expectations,
                    format=target["format"],
                    suite_name=f"suite_{name}"
                )
                
                # We can't return AssetCheckResult directly from an asset function without @asset_check,
                # but we can log and add metadata.
                context.log.info(f"🛡️ Validation {'Passed' if success else 'FAILED'}! Report: {report_url}")
                
                context.add_output_metadata({
                    "quality_check": "Passed" if success else "FAILED",
                    "gx_report": MetadataValue.url(report_url)
                })
                
                if not success:
                    raise ValueError(f"❌ Data Quality Check FAILED for {name}. Check report at {report_url}")

            return result

        return generated_asset
