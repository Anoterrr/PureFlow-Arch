import os
import duckdb
from dotenv import load_dotenv

load_dotenv()


class ConnectionFactory:
    """Gerencia conexões com o Storage (MinIO) e o Motor de Processamento (DuckDB)."""

    @staticmethod
    def get_duckdb_conn():
        """Retorna uma conexão DuckDB configurada para o ambiente local."""
        db_path = os.getenv("DUCKDB_PATH", "data/datagate_local.db")
        conn = duckdb.connect(db_path)

        # Configurações para Arch WSL / 32GB RAM
        conn.execute("SET memory_limit = '16GB'")  # Reserva metade para o DuckDB
        conn.execute("SET threads = 4")  # Ajuste conforme seu processador

        # Instala extensões para ler do MinIO (S3)
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")

        return conn

    @staticmethod
    def setup_s3_auth(conn):
        """Configura as credenciais para o DuckDB enxergar o MinIO."""
        conn.execute(
            f"SET s3_endpoint = '{os.getenv('S3_ENDPOINT').replace('http://', '')}'"
        )
        conn.execute(f"SET s3_access_key_id = '{os.getenv('STORAGE_USER')}'")
        conn.execute(f"SET s3_secret_access_key = '{os.getenv('STORAGE_PASSWORD')}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET s3_url_style = 'path'")
