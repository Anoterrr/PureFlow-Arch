import os
import duckdb
from dotenv import load_dotenv

load_dotenv()


class ConnectionFactory:
    """Manages connections to Storage (MinIO) and the Processing Engine (DuckDB)."""

    @staticmethod
    def get_duckdb_conn():
        """Returns a DuckDB connection configured for the local environment."""
        db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")
        conn = duckdb.connect(db_path)

        # Configurations for Arch WSL / 32GB RAM
        conn.execute("SET memory_limit = '16GB'")  # Reserve half for DuckDB
        conn.execute("SET threads = 4")  # Adjust according to your processor

        # Install extensions to read from MinIO (S3)
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        return conn

    @staticmethod
    def setup_s3_auth(conn):
        """Configures credentials for DuckDB to see MinIO."""
        conn.execute(
            f"SET s3_endpoint = '{os.getenv('S3_ENDPOINT').replace('http://', '')}'"
        )
        conn.execute(f"SET s3_access_key_id = '{os.getenv('STORAGE_USER')}'")
        conn.execute(f"SET s3_secret_access_key = '{os.getenv('STORAGE_PASSWORD')}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET s3_url_style = 'path'")
