"""Factory Pattern for creating Data Pipelines as Dagster Assets."""

from typing import Any, Dict, List, Optional

from dagster import (
    AssetKey,
    AssetsDefinition,
    MetadataValue,
    asset,
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
        source_expectations: Optional[List[Dict[str, Any]]] = None,
        target_expectations: Optional[List[Dict[str, Any]]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> List[AssetsDefinition]:
        """
        Generates a sequence of Dagster Assets:
        [Source Validation (GX)] -> [Transformation (DuckDB)] -> [Target Validation (GX)].
        Returns a list of AssetDefinitions to be registered.
        """
        assets = []
        current_deps = [AssetKey([dep]) for dep in depends_on] if depends_on else []

        # 1. Source Validation Asset (gx_landing or gx_bronze, etc.)
        if source_expectations:
            gx_source_name = f"gx_{name}_source"

            @asset(
                name=gx_source_name,
                group_name=group_name,
                deps=current_deps,
                compute_kind="gx",
                config_schema={"execution_date": str},
            )
            def source_validation(context):
                execution_date = context.op_config.get("execution_date")
                engine = PureFlowEngine(execution_date=execution_date)
                
                rendered_path = engine.render_path(source["path"])
                success, report_url = validate_data(
                    path=rendered_path,
                    expectations=source_expectations,
                    data_format=source["format"],
                    suite_name=f"suite_{gx_source_name}",
                )
                if not success:
                    raise ValueError(f"Source validation failed for {name}. Report: {report_url}")
                return MetadataValue.url(report_url)

            assets.append(source_validation)
            current_deps = [AssetKey([gx_source_name])]

        # 2. Main Transformation Asset (bronze or silver, etc.)
        @asset(
            name=name, 
            group_name=group_name, 
            deps=current_deps, 
            compute_kind="duckdb",
            config_schema={"execution_date": str},
        )
        def main_transformation(context):
            execution_date = context.op_config.get("execution_date")
            engine = PureFlowEngine(execution_date=execution_date)
            
            result = engine.execute_move_and_transform(
                source_path=source["path"],
                source_format=source["format"],
                target_path=target["path"],
                target_format=target["format"],
                sql_transform=sql_transform,
            )
            context.add_output_metadata(
                {
                    "rows_processed": MetadataValue.int(result["row_count"]),
                    "target_path": MetadataValue.path(result["target_path"]),
                    "status": "Success",
                }
            )
            return result

        assets.append(main_transformation)
        current_deps = [AssetKey([name])]

        # 3. Target Validation Asset (gx_bronze or gx_silver, etc.)
        if target_expectations:
            gx_target_name = f"gx_{name}"

            @asset(
                name=gx_target_name,
                group_name=group_name,
                deps=current_deps,
                compute_kind="gx",
                config_schema={"execution_date": str},
            )
            def target_validation(context):
                execution_date = context.op_config.get("execution_date")
                engine = PureFlowEngine(execution_date=execution_date)
                
                rendered_path = engine.render_path(target["path"])
                success, report_url = validate_data(
                    path=rendered_path,
                    expectations=target_expectations,
                    data_format=target["format"],
                    suite_name=f"suite_{gx_target_name}",
                )
                if not success:
                    raise ValueError(f"Target validation failed for {name}. Report: {report_url}")
                return MetadataValue.url(report_url)

            assets.append(target_validation)

        return assets
