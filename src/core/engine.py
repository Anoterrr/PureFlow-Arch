"""Core Engine for executing data engineering tasks using DuckDB."""

from datetime import datetime
from typing import Any, Dict, Optional

from core.connection import ConnectionFactory
from core.logger import logger


class PureFlowEngine:
    """Technically executes data movement, transformation and validation tasks."""

    def __init__(self, execution_date: str = None):
        self.execution_date = execution_date or datetime.now().strftime("%Y-%m-%d")
        self.factory = ConnectionFactory()

    def render_path(self, path: str) -> str:
        """Renders dynamic variables in paths (e.g., {{ execution_date }})."""
        return path.replace("{{ execution_date }}", self.execution_date)

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def execute_move_and_transform(
        self,
        source_path: str,
        source_format: str,
        target_path: str,
        target_format: str,
        sql_transform: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes a move from source to target with an optional SQL transformation.
        Uses DuckDB for high performance.
        """
        source_path = self.render_path(source_path)
        target_path = self.render_path(target_path)

        conn = self.factory.get_duckdb_conn()
        self.factory.setup_s3_auth(conn)

        try:
            logger.info(
                "🚀 [Engine] Moving: %s (%s) -> %s (%s)",
                source_path,
                source_format,
                target_path,
                target_format,
            )

            # 1. Define the source read function
            fmt = source_format.lower()
            read_func = f"read_{fmt}_auto" if fmt in ["csv", "json"] else "read_parquet"

            # 2. Build the query
            # We create a temporary view 'source_data' to allow SQL transformations
            # DuckDB-specific: we use f-strings for paths as they can't be parameterized in standard way
            conn.execute(
                f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM {read_func}('{source_path}')"  # nosec B608
            )

            final_query = (
                sql_transform if sql_transform else "SELECT * FROM source_data"
            )

            # 3. Execute and Write
            # DuckDB allows COPY from a query directly
            copy_query = f"""
                COPY ({final_query})
                TO '{target_path}' (FORMAT '{target_format.upper()}')
            """
            conn.execute(copy_query)

            # Get count for metadata
            row_count = conn.execute("SELECT count(*) FROM source_data").fetchone()[0]

            logger.info("✅ [Engine] Success! Processed %d rows.", row_count)

            return {
                "status": "success",
                "row_count": row_count,
                "target_path": target_path,
            }

        except Exception as e:
            logger.error("❌ [Engine] Failed execution: %s", str(e))
            raise e
        finally:
            conn.close()
