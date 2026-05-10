"""Core Quality Engine: Integrates Great Expectations with DuckDB and S3."""

import os
import duckdb
from sqlalchemy import event, text
import yaml
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from core.logger import logger

# --- Process-level S3 Configuration ---
# Set global DuckDB settings for the entire process
try:
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    clean_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")
    storage_user = os.getenv("STORAGE_USER", "admin")
    storage_password = os.getenv("STORAGE_PASSWORD", "strongpassword123")

    duckdb.execute(f"SET GLOBAL s3_endpoint = '{clean_endpoint}';")
    duckdb.execute(f"SET GLOBAL s3_access_key_id = '{storage_user}';")
    duckdb.execute(f"SET GLOBAL s3_secret_access_key = '{storage_password}';")
    duckdb.execute("SET GLOBAL s3_use_ssl = false;")
    duckdb.execute("SET GLOBAL s3_url_style = 'path';")
    duckdb.execute("SET GLOBAL s3_region = 'us-east-1';")
    logger.info("🌍 [GX-Quality] Global DuckDB S3 configuration applied.")
except Exception as e:
    logger.warning("⚠️ [GX-Quality] Failed to set global DuckDB settings: %s", str(e))


def get_gx_context():
    """
    Initializes and returns an Ephemeral GX context.
    """
    context_root_dir = os.path.abspath("gx")
    config_path = os.path.join(context_root_dir, "great_expectations.yml")

    # Ensure necessary directories exist
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)

    with open(config_path, "r", encoding="utf-8") as f:
        project_config_dict = yaml.safe_load(f)

    # Path normalization
    for key in ["plugins_directory", "config_variables_file_path"]:
        if key in project_config_dict:
            val = project_config_dict[key]
            if val and not os.path.isabs(val):
                project_config_dict[key] = os.path.abspath(os.path.join(context_root_dir, val))

    return gx.get_context(project_config=project_config_dict)


def setup_gx_backend(context, datasource_name, factory):
    """
    Standardizes GX Backend setup using official add_sql pattern.
    """
    # Use standard memory connection string
    conn_str = "duckdb:///:memory:"

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

    engine = datasource.get_execution_engine().engine

    # Secondary reinforcement on every connection
    if not event.contains(engine, "connect", None):
        @event.listens_for(engine, "connect")
        def receive_connect(dbapi_connection, _connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("INSTALL httpfs; LOAD httpfs;")
            cursor.execute("INSTALL json; LOAD json;")
            cursor.execute("SET s3_use_ssl = false;")
            cursor.execute("SET s3_url_style = 'path';")
            cursor.close()

    raw_conn = factory.get_duckdb_conn(db_path=":memory:")
    factory.setup_s3_auth(raw_conn)

    return datasource, raw_conn


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
