"""Domain-specific Ingestion for Customers."""

import os

from core.logger import logger
from ingestion.sales_ingest import BaseIngestor


class CustomerIngestor(BaseIngestor):
    """Handles JSON-to-Parquet conversion for Customers domain."""

    def __init__(self):
        super().__init__("customers")

    def ingest(self):
        """Converts raw JSON from Landing Zone to Parquet in Bronze Layer."""
        conn = self.factory.get_duckdb_conn()
        self.factory.setup_s3_auth(conn)

        # S3 Landing path (input) and Bronze S3 path (output)
        landing_json = self.paths["clientes_landing"]
        bronze_parquet = self.paths["clientes_bronze"]

        logger.info(
            "🚀 [Customer Ingest] Processing: %s -> %s", landing_json, bronze_parquet
        )

        try:
            # Atomic operation: read JSON, add metadata, write Parquet to Bronze
            conn.execute(f"""
                COPY (
                    SELECT
                        *,
                        now() as _ingested_at,
                        'customers' as _domain,
                        '{os.path.basename(landing_json)}' as _source_file
                    FROM read_json_auto('{landing_json}')
                ) TO '{bronze_parquet}' (FORMAT 'PARQUET')
            """)
            logger.info("✅ Customer data ingested to Bronze successfully.")
        except Exception as err:
            logger.error("❌ Failed to ingest Customer data: %s", err)
            raise err
        finally:
            conn.close()


if __name__ == "__main__":
    CustomerIngestor().ingest()
