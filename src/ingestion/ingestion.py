import duckdb
import os
from connection import ConnectionFactory


def ingest_to_bronze():
    factory = ConnectionFactory()
    conn = factory.get_duckdb_conn()

    # Caminhos
    input_csv = "data/raw_external_data.csv"
    output_bronze = "data/bronze/transactions_raw.parquet"

    os.makedirs("data/bronze", exist_ok=True)

    print("🚀 Ingerindo dados para a camada Bronze...")

    # DuckDB lê o CSV e escreve Parquet de forma atômica
    conn.execute(
        f"""
        COPY (SELECT *, now() as ingested_at FROM read_csv_auto('{input_csv}')) 
        TO '{output_bronze}' (FORMAT 'PARQUET')
    """
    )

    print(f"✔️ Camada Bronze atualizada: {output_bronze}")
    conn.close()


if __name__ == "__main__":
    ingest_to_bronze()
