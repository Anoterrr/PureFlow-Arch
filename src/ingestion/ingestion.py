"""Module for ingesting raw data into the Bronze layer."""
import os
from core.connection import ConnectionFactory


def ingest_to_bronze():
    """Reads raw CSV data and saves it as Parquet in the Bronze layer using DuckDB."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()

    # paths
    input_csv = "data/raw_external_data.csv"
    output_bronze = "data/bronze/transactions_raw.parquet"

    os.makedirs("data/bronze", exist_ok=True)

    print("🚀 Ingesting data to Bronze layer...")

    # DuckDB reads the CSV and writes Parquet atomically
    conn.execute(
        f"""
        COPY (SELECT *, now() as ingested_at FROM read_csv_auto('{input_csv}'))
        TO '{output_bronze}' (FORMAT 'PARQUET')
    """
    )

    print(f"✔️ Bronze layer updated: {output_bronze}")
    conn.close()


if __name__ == "__main__":
    ingest_to_bronze()
