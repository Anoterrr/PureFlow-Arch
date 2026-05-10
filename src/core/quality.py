"""Core Quality Engine: Integrates Great Expectations with DuckDB and S3."""

import os
import duckdb
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
import yaml
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dagster import ConfigurableResource
import dagster_ge
from core.logger import logger
from core.config import get_s3_connection_config

# --- Custom Great Expectations Resource ---
class GreatExpectationsResource(ConfigurableResource):
    """Custom resource to manage Great Expectations context."""
    ge_root_dir: str

    def get_context(self):
        """Returns the GX Data Context."""
        import great_expectations as gx
        return gx.get_context(context_root_dir=self.ge_root_dir)

# --- Global SQLAlchemy Listener ---
# This ensures that ANY DuckDB connection created via SQLAlchemy (by GX or others)
# is correctly configured for S3 access using the DuckDB Secrets Manager.
@event.listens_for(Engine, "connect")
def set_duckdb_s3_config(dbapi_connection, _connection_record):
    """Global hook to configure DuckDB S3 settings on every new connection."""
    conn_type = str(type(dbapi_connection)).lower()
    if "duckdb" in conn_type:
        logger.debug("🔗 [Hook] SQLAlchemy Hook triggered for DuckDB connection.")
        cursor = dbapi_connection.cursor()
        try:
            # 1. Load necessary extensions
            cursor.execute("INSTALL httpfs; LOAD httpfs;")
            cursor.execute("INSTALL json; LOAD json;")
            
            # 2. Apply HYPER-REDUNDANT settings (Session + Global)
            # This ensures any DuckDB version/session combination respects path-style
            s3_cfg = get_s3_connection_config()
            
            logger.debug("⚙️ [Hook] Applying S3 Config: Endpoint=%s, Style=%s", 
                         s3_cfg['s3_endpoint'], s3_cfg['s3_url_style'])
            
            # Session-level
            cursor.execute("SET s3_url_style = 'path';")
            cursor.execute(f"SET s3_use_ssl = {s3_cfg['s3_use_ssl']};")
            cursor.execute(f"SET s3_endpoint = '{s3_cfg['s3_endpoint']}';")
            
            # Global-level
            cursor.execute("SET GLOBAL s3_url_style = 'path';")
            cursor.execute("SET GLOBAL s3_use_ssl = false;")
            cursor.execute(f"SET GLOBAL s3_endpoint = '{s3_cfg['s3_endpoint']}';")
            
            # 3. Create a default secret with EXPLICIT ENDPOINT and PATH style
            # Using DuckDB 1.1.0+ Secret type with broad SCOPE
            cursor.execute(f"""
                CREATE OR REPLACE SECRET (
                    TYPE S3,
                    KEY_ID '{s3_cfg['s3_access_key_id']}',
                    SECRET '{s3_cfg['s3_secret_access_key']}',
                    ENDPOINT '{s3_cfg['s3_endpoint']}',
                    URL_STYLE 'path',
                    USE_SSL false,
                    REGION 'us-east-1',
                    SCOPE 's3://'
                );
            """)
            
            logger.info("✅ [Hook] Hyper-Redundant S3 config applied successfully (%s).", s3_cfg['s3_endpoint'])
        except Exception as e:
            logger.warning("⚠️ [Hook] Failed to configure DuckDB S3: %s", str(e))
        finally:
            cursor.close()

# --- Process-level S3 Reinforcement ---
def reinforce_global_s3_config():
    """Reinforces S3 configuration at the process level for all DuckDB connections."""
    try:
        s3_cfg = get_s3_connection_config()
        logger.debug("🌍 [Global-Reinforce] Starting reinforcement with endpoint: %s", s3_cfg['s3_endpoint'])
        with duckdb.connect() as global_conn:
            global_conn.execute("INSTALL httpfs; LOAD httpfs;")
            
            # Global-level reinforcement
            global_conn.execute("SET GLOBAL s3_url_style = 'path';")
            global_conn.execute("SET GLOBAL s3_use_ssl = false;")
            global_conn.execute(f"SET GLOBAL s3_endpoint = '{s3_cfg['s3_endpoint']}';")
            
            # Also create a named secret for extra redundancy with explicit endpoint
            global_conn.execute(f"""
                CREATE OR REPLACE SECRET minio_global (
                    TYPE S3,
                    KEY_ID '{s3_cfg['s3_access_key_id']}',
                    SECRET '{s3_cfg['s3_secret_access_key']}',
                    ENDPOINT '{s3_cfg['s3_endpoint']}',
                    URL_STYLE 'path',
                    USE_SSL false,
                    REGION 'us-east-1',
                    SCOPE 's3://'
                );
            """)
        logger.info("🌍 [Global-Reinforce] DuckDB environment reinforced.")
    except Exception as e:
        logger.warning("⚠️ [Global-Reinforce] Initial reinforcement failed: %s", str(e))

# Run reinforcement immediately upon module load
reinforce_global_s3_config()


def get_gx_context():
    """
    Initializes and returns an Ephemeral GX context.
    """
    context_root_dir = os.path.abspath("gx")
    config_path = os.path.join(context_root_dir, "great_expectations.yml")

    # Ensure necessary directories exist
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)

    with open(config_path, "r", encoding="utf-8") as f:
        project_config_dict = yaml.safe_load(f) or {}

    # Path normalization for local vs docker environments
    for key in ["plugins_directory", "config_variables_file_path"]:
        if key in project_config_dict:
            val = project_config_dict[key]
            if val and not os.path.isabs(val):
                project_config_dict[key] = os.path.abspath(os.path.join(context_root_dir, val))
            elif val and os.path.isabs(val) and val.startswith("/app/gx/"):
                # Force local path if /app/ prefix found but we are not in docker
                if not os.path.exists("/.dockerenv"):
                    local_val = val.replace("/app/gx/", "./gx/")
                    project_config_dict[key] = os.path.abspath(os.path.join(os.getcwd(), local_val))

    return gx.get_context(project_config=project_config_dict)


def setup_gx_backend(context, datasource_name, factory):
    """
    Standardizes GX Backend setup using official add_sql pattern.
    The global SQLAlchemy listener handles the S3 configuration via Secrets Manager.
    We use a unique file-based DB per suite to ensure consistency and avoid locking/mismatch.
    """
    # Use a unique persistent file-based DB for this specific validation run
    # This ensures that multiple connections (GX Pool + Raw) share the same state
    db_path = os.path.abspath(f"data/gx_val_{datasource_name}.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # We NO LONGER pass s3 params in the connection string to avoid "different configuration" errors.
    # The @event.listens_for(Engine, "connect") hook handles this for GX.
    conn_str = f"duckdb:///{db_path}"

    try:
        try:
            context.data_sources.delete(datasource_name)
        except Exception:
            pass
            
        datasource = context.data_sources.add_sql(
            name=datasource_name, connection_string=conn_str
        )
    except Exception:
        datasource = context.data_sources.get(datasource_name)

    # Use the SAME database file for the raw connection
    raw_conn = factory.get_duckdb_conn(db_path=db_path)
    factory.setup_s3_auth(raw_conn)

    return datasource, raw_conn, db_path


def get_or_create_suite(context, suite_name):
    """Abstraction for GX suite management."""
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        try:
            suite = ExpectationSuite(name=suite_name)
            return context.suites.add(suite)
        except Exception:
            return context.suites.get(name=suite_name)
