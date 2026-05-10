"""Factory Pattern for creating Data Pipelines as Dagster Assets."""

from typing import Any, Dict, List, Optional, Union

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AssetObservation,
    AssetsDefinition,
    AssetChecksDefinition,
    MetadataValue,
    MaterializeResult,
    asset,
    asset_check,
)

from core.engine import PureFlowEngine
from core.logger import logger
from core.quality import GreatExpectationsResource


class DataPipelineFactory:
    """Creates standardized Dagster Assets and Checks for data ingestion and transformation."""

    @staticmethod
    def load_sql(path: str) -> str:
        """Helper to read SQL files from a given path."""
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    @staticmethod
    def create_asset(  # pylint: disable=too-many-arguments, too-many-positional-arguments, too-many-locals
        name: str,
        source: Dict[str, str],
        target: Dict[str, str],
        group_name: str = "default",
        sql_transform: Optional[str] = None,
        source_expectations: Optional[List[Dict[str, Any]]] = None,
        target_expectations: Optional[List[Dict[str, Any]]] = None,
        depends_on: Optional[List[str]] = None,
    ) -> List[Union[AssetsDefinition, AssetChecksDefinition]]:
        """
        Generates a sequence of Dagster Assets and Checks:
        [Transformation (DuckDB)] -> [Source Check (GX)] & [Target Check (GX)].
        Returns a list of AssetDefinitions and AssetChecksDefinitions to be registered.
        """
        assets_and_checks = []
        current_deps = [AssetKey([dep]) for dep in depends_on] if depends_on else []

        # Context for path rendering
        source_context = {"name": name, "group": group_name, "format": source["format"]}
        target_context = {"name": name, "group": group_name, "format": target["format"]}

        # 1. Main Transformation Asset (bronze or silver, etc.)
        @asset(
            name=name,
            group_name=group_name,
            deps=current_deps,
            compute_kind="duckdb",
            description=f"DuckDB transformation for {name}.",
            config_schema={"execution_date": str},
        )
        def main_transformation(context, gx_resource: GreatExpectationsResource):
            execution_date = context.op_config.get("execution_date")
            engine = PureFlowEngine(execution_date=execution_date)

            rendered_source = engine.render_path(source["path"], context=source_context)
            rendered_target = engine.render_path(target["path"], context=target_context)
            
            # Tracking validation results
            validation_metadata = {}

            # --- GATEKEEPER: Source Validation ---
            if source_expectations:
                context.log.info(f"🛡️ [Gatekeeper] Validating source for {name}: {rendered_source}")
                from validation.gx_validator import validate_data
                success, report_url, error_msg = validate_data(
                    path=rendered_source,
                    expectations=source_expectations,
                    data_format=source["format"],
                    suite_name=f"check_{name}_source",
                    context=gx_resource.get_context()
                )
                
                validation_metadata["Source Validation Report"] = MetadataValue.url(report_url)
                
                if not success:
                    context.log.error(f"❌ [Gatekeeper] Source validation FAILED for {name}. Quarantining...")
                    q_path = engine.quarantine_data(rendered_source, reason=f"source_fail_{name}", source_format=source["format"])
                    
                    # Yield observation for visibility before crashing
                    context.log_event(
                        AssetObservation(
                            asset_key=name,
                            metadata={
                                "validation_status": "FAILED",
                                "GX Source Report": MetadataValue.url(report_url),
                                "quarantine_path": MetadataValue.path(q_path),
                                "error": MetadataValue.text(error_msg)
                            }
                        )
                    )
                    raise ValueError(f"Circuit Breaker: Source validation failed. Data quarantined. Report: {report_url}")
                
                context.log.info(f"✅ [Gatekeeper] Source validation passed for {name}.")

            # --- EXECUTION: Transformation ---
            result = engine.execute_move_and_transform(
                source_path=rendered_source,
                source_format=source["format"],
                target_path=rendered_target,
                target_format=target["format"],
                sql_transform=sql_transform,
            )

            # --- GATEKEEPER: Target Validation ---
            if target_expectations:
                context.log.info(f"🛡️ [Gatekeeper] Validating target for {name}: {rendered_target}")
                from validation.gx_validator import validate_data
                success, report_url, error_msg = validate_data(
                    path=rendered_target,
                    expectations=target_expectations,
                    data_format=target["format"],
                    suite_name=f"check_{name}_target",
                    context=gx_resource.get_context()
                )
                
                validation_metadata["Target Validation Report"] = MetadataValue.url(report_url)
                
                if not success:
                    context.log.error(f"❌ [Gatekeeper] Target validation FAILED for {name}. Quarantining...")
                    q_path = engine.quarantine_data(rendered_target, reason=f"target_fail_{name}", source_format=target["format"])
                    
                    context.log_event(
                        AssetObservation(
                            asset_key=name,
                            metadata={
                                "validation_status": "FAILED",
                                "GX Target Report": MetadataValue.url(report_url),
                                "quarantine_path": MetadataValue.path(q_path),
                                "error": MetadataValue.text(error_msg)
                            }
                        )
                    )
                    raise ValueError(f"Circuit Breaker: Target validation failed. Data quarantined. Report: {report_url}")

                context.log.info(f"✅ [Gatekeeper] Target validation passed for {name}.")

            return MaterializeResult(
                metadata={
                    "rows_processed": MetadataValue.int(result["row_count"]),
                    "target_path": MetadataValue.path(result["target_path"]),
                    "status": "Success",
                    **validation_metadata
                }
            )

        assets_and_checks.append(main_transformation)

        return assets_and_checks
