"""Utility functions for Great Expectations validation."""
import os
import great_expectations as gx
from sqlalchemy import text, event

def get_gx_context():
    """Initializes and returns the GX context in the 'gx' directory."""
    context_root_dir = os.path.abspath("gx")
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"),
                exist_ok=True)
    return gx.get_context(context_root_dir=context_root_dir)

def setup_gx_backend(context, datasource_name, factory):
    """
    Standardizes GX Backend setup to avoid duplication.
    Wraps protected member access in one controlled place.
    """
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000").replace("http://", "")
    storage_user = os.getenv("STORAGE_USER", "admin")
    storage_password = os.getenv("STORAGE_PASSWORD", "password123")

    try:
        datasource = context.data_sources.get(datasource_name)
    except (ValueError, KeyError):
        datasource = context.data_sources.add_sql(
            name=datasource_name, 
            connection_string="duckdb:///:memory:"
        )

    engine = datasource.get_execution_engine().engine
    
    # Use SQLAlchemy events to ensure every connection has S3 configured
    @event.listens_for(engine, "connect")
    def receive_connect(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("INSTALL httpfs; LOAD httpfs;")
        cursor.execute(f"SET s3_endpoint = '{s3_endpoint}';")
        cursor.execute(f"SET s3_access_key_id = '{storage_user}';")
        cursor.execute(f"SET s3_secret_access_key = '{storage_password}';")
        cursor.execute("SET s3_use_ssl = false;")
        cursor.execute("SET s3_url_style = 'path';")
        cursor.close()

    # Apply once to current connections if any
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        conn.commit()

    raw_conn = engine.raw_connection().dbapi_connection
    return datasource, raw_conn

def get_or_create_suite(context, suite_name):
    """Abstraction for suite management."""
    try:
        return context.suites.get(name=suite_name)
    except Exception:
        # In GX 1.x, we create it via context.suites.add
        from great_expectations.core.expectation_suite import ExpectationSuite
        suite = ExpectationSuite(name=suite_name)
        return context.suites.add(suite)
