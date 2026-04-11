"""
Airflow DAG for the PureFlow-Arch Sales Pipeline.
Orchestrates the flow from Landing to Gold layer.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Ensure src is in PYTHONPATH for Airflow
sys.path.append('/opt/airflow/src')

# pylint: disable=wrong-import-position, import-error
from ingestion.sales_ingest import SalesIngestor
from quality.bronze_rules import validate_bronze_quality
from transformations.bronze_to_silver import transform_bronze_to_silver
from quality.silver_rules import validate_silver_quality

def run_ingestion():
    """Wrapper function to trigger Sales ingestion."""
    ingestor = SalesIngestor()
    ingestor.ingest()

DEFAULT_ARGS = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pureflow_sales_pipeline',
    default_args=DEFAULT_ARGS,
    description='Refined Medallion Pipeline (Bronze-Validation-Silver-Validation-Gold)',
    schedule_interval=None,
    catchup=False,
    tags=['sales', 'medallion', 'gx', 'dbt'],
) as dag:

    # 1. Ingestion: Landing -> Bronze
    task_ingestion = PythonOperator(
        task_id='sales_ingestion',
        python_callable=run_ingestion,
    )

    # 2. Bronze Validation: Gatekeeper on raw ingestion
    task_bronze_validation = PythonOperator(
        task_id='bronze_quality_validation',
        python_callable=validate_bronze_quality,
    )

    # 3. Silver Transformation: Apply Business Rules (Bronze -> Silver)
    task_silver_transformation = PythonOperator(
        task_id='silver_business_rules',
        python_callable=transform_bronze_to_silver,
    )

    # 4. Silver Validation: Gatekeeper after business transformations
    task_silver_validation = PythonOperator(
        task_id='silver_quality_validation',
        python_callable=validate_silver_quality,
    )

    # 5. Gold Transformation: Silver -> Gold (using dbt)
    # Wrap command to avoid line-too-long
    DBT_CMD = 'cd /opt/airflow/dbt && dbt run --profiles-dir .'
    task_dbt_run = BashOperator(
        task_id='dbt_transformation_gold',
        bash_command=DBT_CMD,
        env={
            **os.environ,
            'S3_ENDPOINT': os.getenv('S3_ENDPOINT', 'minio:9000'),
            'STORAGE_USER': os.getenv('STORAGE_USER', 'admin'),
            'STORAGE_PASSWORD': os.getenv('STORAGE_PASSWORD', 'password123'),
        }
    )

    # Execution Flow
    # pylint: disable=pointless-statement
    task_ingestion >> task_bronze_validation >> task_silver_transformation \
        >> task_silver_validation >> task_dbt_run
