"""Core Quality Engine: Integrates Great Expectations with DuckDB and S3."""

import os

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from sqlalchemy import event


def get_gx_context():
    """Initializes and returns the GX context in the 'gx' directory."""
    context_root_dir = os.path.abspath("gx")
    # Ensure necessary directories exist for GX Data Docs
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"), exist_ok=True)
    return gx.get_context(context_root_dir=context_root_dir)


def setup_gx_backend(context, datasource_name, factory):
    """
    Standardizes GX Backend setup to avoid duplication.
    Injects S3 configuration into the DuckDB connection used by Great Expectations.
    """
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")
    storage_user = os.getenv("STORAGE_USER", "admin")
    storage_password = os.getenv("STORAGE_PASSWORD", "strongpassword123")

    try:
        datasource = context.data_sources.get(datasource_name)
    except (ValueError, KeyError):
        # We use duckdb:///:memory: because GX only needs a processing engine
        datasource = context.data_sources.add_sql(
            name=datasource_name, connection_string="duckdb:///:memory:"
        )

    engine = datasource.get_execution_engine().engine

    # IMPORTANT: We use SQLAlchemy events to ensure S3 is configured
    # every time GX/SQLAlchemy opens a new connection to DuckDB.
    @event.listens_for(engine, "connect")
    def receive_connect(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("INSTALL httpfs; LOAD httpfs;")
        cursor.execute("INSTALL json; LOAD json;")
        cursor.execute(f"SET s3_endpoint = '{s3_endpoint}';")
        cursor.execute(f"SET s3_access_key_id = '{storage_user}';")
        cursor.execute(f"SET s3_secret_access_key = '{storage_password}';")
        cursor.execute("SET s3_use_ssl = false;")
        cursor.execute("SET s3_url_style = 'path';")
        cursor.close()

    # Need a direct connection for helper operations
    raw_conn = factory.get_duckdb_conn()
    factory.setup_s3_auth(raw_conn)

    return datasource, raw_conn


def get_or_create_suite(context, suite_name):
    """Abstraction for GX suite management."""
    try:
        return context.suites.get(name=suite_name)
    except Exception:  # pylint: disable=broad-exception-caught
        # Create new suite if it doesn't exist
        suite = ExpectationSuite(expectation_suite_name=suite_name)
        return context.suites.add(suite)
