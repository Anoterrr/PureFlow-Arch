"""Modular Orchestrator for PureFlow-Arch (Sales Domain)."""
import os
from ingestion.sales_ingest import SalesIngestor
from quality.bronze_rules import validate_bronze_quality
from core.logger import logger


def run_sales_pipeline():
    """Triggers the modular ingestion, validation, and dbt transformation."""
    logger.info("🌊 [Sales Pipeline] Starting Modular Flow...")

    # 1. Ingestion (Modular)
    logger.info("Step 1: Ingesting Sales (Landing -> Bronze)...")
    ingestor = SalesIngestor()
    ingestor.ingest()

    # 2. Validation (Gatekeeper)
    logger.info("Step 2: Validating Sales (Bronze quality check)...")
    validate_bronze_quality()

    # 3. DBT Transformation (Materializing Gold)
    logger.info("Step 3: Triggering dbt-run (Silver -> Gold)...")
    # Note: Profiles directory set to our dbt project
    exit_code = os.system("cd dbt && dbt run --profiles-dir .")

    if exit_code != 0:
        logger.error("❌ dbt-run failed. Check logs for details.")
    else:
        logger.info("✨ Sales Pipeline completed successfully!")


if __name__ == "__main__":
    run_sales_pipeline()
