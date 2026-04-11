"""Module for transforming Silver data (Delta) into Gold layer (DuckDB)."""
import os
import duckdb
from core.connection import ConnectionFactory
from core.config import get_s3_paths
from core.logger import logger


def silver_to_gold_transformation():
    """Reads validated Silver Delta tables and materializes Gold summary in DuckDB."""
    # Target Gold Database Path
    GOLD_DB_PATH = "data/pureflow_lakehouse.db"
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(GOLD_DB_PATH), exist_ok=True)

    # Initialize Connections
    factory = ConnectionFactory()
    
    # We use a persistent connection to the Gold DuckDB
    logger.info(f"🏆 Connecting to Gold Layer: {GOLD_DB_PATH}")
    gold_conn = duckdb.connect(GOLD_DB_PATH)
    
    # Setup S3 and Extensions for the Gold connection
    # Note: ConnectionFactory.get_duckdb_conn returns a new connection to DUCKDB_PATH
    # We'll manually setup this connection for flexibility
    gold_conn.execute("INSTALL httpfs; LOAD httpfs;")
    gold_conn.execute("INSTALL delta; LOAD delta;")
    factory.setup_s3_auth(gold_conn)

    paths = get_s3_paths()
    vendas_silver = paths['vendas_silver']
    clientes_silver = paths['clientes_silver']

    logger.info("🛠️ Starting Gold Transformation (dbt-style)...")

    # 1. Create or Replace Summary Table (dbt-style CTEs)
    # Task 2.1: Reads validated data from the Silver layer (Delta Lake format)
    # Task 2.2: Materializes a summary table in the Gold layer (DuckDB .db file)
    
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
        result = gold_conn.execute("SELECT count(*) FROM sales_summary").fetchone()
        logger.info(f"✅ Gold transformation complete! Summary rows: {result[0]}")
        
    except Exception as e:
        logger.error(f"❌ Error during Gold transformation: {e}")
        raise e
    finally:
        gold_conn.close()


if __name__ == "__main__":
    silver_to_gold_transformation()
