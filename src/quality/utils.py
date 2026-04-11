"""Utility functions for Great Expectations validation."""
import os
import great_expectations as gx

def get_gx_context():
    """Initializes and returns the GX context in the 'gx' directory."""
    context_root_dir = os.path.abspath("gx")
    os.makedirs(os.path.join(context_root_dir, "uncommitted/data_docs"),
                exist_ok=True)
    return gx.get_context(context_root_dir=context_root_dir)
