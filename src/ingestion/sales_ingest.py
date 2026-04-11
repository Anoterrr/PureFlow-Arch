"""Domain-specific Ingestion for Sales."""
import os
import duckdb
from abc import ABC, abstractmethod
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger

class BaseIngestor(ABC):
    """Abstract base class for domain-driven ingestion."""
    def __init__(self, domain: str):
        self.domain = domain
        self.factory = ConnectionFactory()
        self.paths = get_s3_paths()
        
    @abstractmethod
    def ingest(self):
        """Must be implemented by each domain."""
        pass

class SalesIngestor(BaseIngestor):
    """Handles CSV-to-Parquet conversion for Sales domain."""
    def __init__(self):
        super().__init__("sales")
        
    def ingest(self):
        """Converts raw CSV to Parquet and uploads to Landing Zone in MinIO."""
        conn = self.factory.get_duckdb_conn()
        self.factory.setup_s3_auth(conn)
        
        # Local CSV path (input) and Landing S3 path (output)
        # We assume for this modular refactor that raw data might be staged locally
        # or we read directly from the configured landing zone in config.py
        landing_csv = self.paths['vendas_landing']
        bronze_parquet = self.paths['vendas_bronze']
        
        logger.info(f"🚀 [Sales Ingest] Processing: {landing_csv} -> {bronze_parquet}")
        
        try:
            # Atomic operation: read CSV, add metadata, write Parquet to Bronze
            # DuckDB handles the S3 upload directly via httpfs
            conn.execute(f"""
                COPY (
                    SELECT 
                        *, 
                        now() as _ingested_at,
                        'sales' as _domain,
                        '{os.path.basename(landing_csv)}' as _source_file
                    FROM read_csv_auto('{landing_csv}')
                ) TO '{bronze_parquet}' (FORMAT 'PARQUET')
            """)
            logger.info("✅ Sales data ingested to Bronze successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to ingest Sales data: {e}")
            raise e
        finally:
            conn.close()

if __name__ == "__main__":
    ingestor = SalesIngestor()
    ingestor.ingest()
