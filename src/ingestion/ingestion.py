"""Module for ingesting raw data into the Bronze layer."""
import os
from core.connection import ConnectionFactory


def ingest_to_bronze():
    """Reads raw CSV data and saves it as Parquet in the Bronze layer using DuckDB."""
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()

    # Base landing zone path (inside minio_data)
    # Using Hive partitioning: erp_vendas/dt=2024-03-29/vendas.csv
    base_date = "2024-03-29"
    input_csv = f"data/minio_data/landing-zone/erp_vendas/dt={base_date}/vendas.csv"
    output_bronze = f"data/minio_data/bronze/transactions_raw_{base_date}.parquet"

    os.makedirs("data/minio_data/bronze", exist_ok=True)

    print(f"🚀 Ingesting data from {input_csv} to Bronze layer...")

    if not os.path.exists(input_csv):
        print(f"❌ Error: Input file not found at {input_csv}")
        return

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
