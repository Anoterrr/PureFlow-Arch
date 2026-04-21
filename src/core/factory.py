"""Factory Pattern for creating Data Pipelines as Dagster Assets."""

from typing import Any, Dict, List, Optional, Tuple

from dagster import (
    AssetCheckResult,
    AssetKey,
    AssetsDefinition,
    AssetChecksDefinition,
    MetadataValue,
    asset,
    asset_check,
)

from core.engine import PureFlowEngine
from validation.gx_validator import validate_data


class DataPipelineFactory:
    """Creates standardized Dagster Assets for data ingestion and transformation."""

    @staticmethod
    def load_sql(path: str) -> str:
        """Helper to read SQL files from a given path."""
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def create_asset(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        name: str,
        source: Dict[str, str],
        target: Dict[str, str],
        group_name: str = "default",
        sql_transform: Optional[str] = None,
        expectations: Optional[List[Dict[str, Any]]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> Tuple[AssetsDefinition, Optional[AssetChecksDefinition]]:
        """
        Generates a Dagster Asset and an optional Asset Check for quality.
        Returns (Asset, Check).
        """

        # Define dependencies (Lineage)
        deps = [AssetKey([dep]) for dep in depends_on] if depends_on else []

        @asset(name=name, group_name=group_name, deps=deps, compute_kind="duckdb")
        def generated_asset(context):
            # 1. Initialize Engine
            engine = PureFlowEngine()

            # 2. Execute Data Movement & Transformation
            result = engine.execute_move_and_transform(
                source_path=source["path"],
                source_format=source["format"],
                target_path=target["path"],
                target_format=target["format"],
                sql_transform=sql_transform,
            )

            # 3. Add Metadata to Dagster
            context.add_output_metadata(
                {
                    "rows_processed": MetadataValue.int(result["row_count"]),
                    "target_path": MetadataValue.path(result["target_path"]),
                    "status": "Success",
                }
            )

            return result

        # 4. Separate Quality Check Step (Asset Check)
        generated_check = None
        if expectations:

            @asset_check(asset=generated_asset, name=f"check_{name}")
            def quality_gate(_context):
                engine = PureFlowEngine()
                # Re-render path for the check to validate the materialized target
                rendered_path = engine.render_path(target["path"])

                success, report_url = validate_data(
                    path=rendered_path,
                    expectations=expectations,
                    data_format=target["format"],
                    suite_name=f"suite_{name}",
                )

                return AssetCheckResult(
                    passed=success,
                    metadata={
                        "gx_report": MetadataValue.url(report_url),
                        "description": "Validation powered by Great Expectations",
                    },
                )

            generated_check = quality_gate

        return generated_asset, generated_check
