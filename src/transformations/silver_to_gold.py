"""Module for transforming Silver data (Delta) into Gold layer (DuckDB)."""
import os
import duckdb
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def silver_to_gold_transformation():
    """Reads validated Silver Delta tables and materializes Gold summary."""
    gold_db_path = "data/pureflow_lakehouse.db"

    # Ensure directory exists
    os.makedirs(os.path.dirname(gold_db_path), exist_ok=True)

    # Initialize Connections
    factory = ConnectionFactory()

    # We use a persistent connection to the Gold DuckDB
    logger.info("🏆 Connecting to Gold Layer: %s", gold_db_path)
    gold_conn = duckdb.connect(gold_db_path)

    # Setup S3 and Extensions for the Gold connection
    gold_conn.execute("INSTALL httpfs; LOAD httpfs;")
    gold_conn.execute("INSTALL delta; LOAD delta;")
    factory.setup_s3_auth(gold_conn)

    paths = get_s3_paths()
    vendas_silver = paths['vendas_silver']
    clientes_silver = paths['clientes_silver']

    logger.info("🛠️ Starting Gold Transformation (dbt-style)...")

    # 1. Create or Replace Summary Table (dbt-style CTEs)
    transformation_sql = f"""
    CREATE OR REPLACE TABLE sales_summary AS
    WITH silver_vendas AS (
        SELECT * FROM delta_scan('{vendas_silver}')
    ),
    silver_clientes AS (
        SELECT * FROM delta_scan('{clientes_silver}')
    ),
    enriched_sales AS (
        SELECT
            v.id,
            v.sale_value,
            v.sale_date,
            c.name as customer_name,
            c.region,
            date_trunc('month', v.sale_date::DATE) as sale_month
        FROM silver_vendas v
        LEFT JOIN silver_clientes c ON v.customer_id = c.customer_id
    )
    SELECT
        region,
        sale_month,
        sum(sale_value) as total_revenue,
        count(id) as total_orders,
        round(avg(sale_value), 2) as avg_ticket
    FROM enriched_sales
    GROUP BY region, sale_month
    ORDER BY total_revenue DESC;
    """

    try:
        logger.info("📊 Materializing 'sales_summary' table...")
        gold_conn.execute(transformation_sql)

        # Verify results
        result = gold_conn.execute(
            "SELECT count(*) FROM sales_summary"
        ).fetchone()
        logger.info("✅ Gold transformation complete! Summary rows: %s",
                    result[0])

    except Exception as err:
        logger.error("❌ Error during Gold transformation: %s", err)
        raise err
    finally:
        gold_conn.close()


if __name__ == "__main__":
    silver_to_gold_transformation()
