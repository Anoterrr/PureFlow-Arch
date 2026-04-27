"""Module to manage connections to Storage (MinIO) and the Processing Engine (DuckDB)."""

import os

import duckdb
from dotenv import load_dotenv
from core.logger import logger

# Only load .env if variables are not already set (prevents overriding Docker env with localhost)
load_dotenv(override=False)


class ConnectionFactory:
    """Manages connections to Storage (MinIO) and the Processing Engine (DuckDB)."""

    @staticmethod
    def get_duckdb_conn(db_path: str = None):
        """Returns a DuckDB connection. Use :memory: for non-persistent tasks to avoid locks."""
        if db_path is None:
            db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")

        # Ensure parent directory exists for file-based DBs
        if db_path != ":memory:":
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        conn = duckdb.connect(db_path)

        # Configurations for Arch WSL / 32GB RAM
        conn.execute("SET memory_limit = '16GB'")  # Reserve half for DuckDB
        conn.execute("SET threads = 4")  # Adjust according to your processor

        # Install extensions to read from MinIO (S3) and Delta Lake
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("INSTALL delta;")
        conn.execute("LOAD delta;")
        conn.execute("INSTALL json;")
        conn.execute("LOAD json;")

        return conn

    @staticmethod
    def setup_s3_auth(conn):
        """Configures credentials for DuckDB to see MinIO."""
        # Detect if we are running inside Docker
        is_docker = os.path.exists("/.dockerenv")

        # Prioritize the environment variable
        s3_endpoint = os.getenv("S3_ENDPOINT")

        if not s3_endpoint:
            s3_endpoint = "http://minio:9000" if is_docker else "http://localhost:9000"

        # FORCED FIX: If we are in docker and somehow it got 'localhost', force 'minio'
        if is_docker and "localhost" in s3_endpoint:
            logger.warning("⚠️ [Conn] S3_ENDPOINT was 'localhost' inside Docker. Forcing 'http://minio:9000'")
            s3_endpoint = "http://minio:9000"

        storage_user = os.getenv("STORAGE_USER", "admin")
        storage_password = os.getenv("STORAGE_PASSWORD", "strongpassword123")

        # Clean endpoint for DuckDB (remove http:// or https://)
        clean_endpoint = s3_endpoint.replace("http://", "").replace("https://", "")

        logger.info("🔌 [Conn] Configuring S3 access with endpoint: %s", clean_endpoint)

        conn.execute(f"SET s3_endpoint = '{clean_endpoint}'")
        conn.execute(f"SET s3_access_key_id = '{storage_user}'")
        conn.execute(f"SET s3_secret_access_key = '{storage_password}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET s3_url_style = 'path'")
