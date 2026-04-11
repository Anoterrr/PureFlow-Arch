"""Utility functions for Great Expectations validation."""
import os
import great_expectations as gx

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
    try:
        datasource = context.get_datasource(datasource_name)
    except (ValueError, KeyError):
        datasource = context.sources.add_duckdb(name=datasource_name)

    # This is the only place in the project where we allow this access
    raw_conn = datasource.get_execution_engine()._connection
    factory.setup_s3_auth(raw_conn)

    return datasource, raw_conn

def get_or_create_suite(context, suite_name):
    """Abstraction for suite management."""
    try:
        return context.get_expectation_suite(suite_name)
    except (ValueError, KeyError):
        return context.add_expectation_suite(expectation_suite_name=suite_name)
