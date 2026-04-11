"""
DAG to generate CLEAN synthetic data into the Landing Zone.
"""
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure src is in PYTHONPATH
sys.path.append('/opt/airflow/src')

# pylint: disable=wrong-import-position, import-error
from utils.generate_clean_data import generate_clean_big_data

DEFAULT_ARGS = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'retries': 0,
}

with DAG(
    'generate_clean_landing_data',
    default_args=DEFAULT_ARGS,
    description='Generates 1M rows of CLEAN data in Landing Zone',
    schedule_interval=None,
    catchup=False,
    tags=['utils', 'data_gen', 'clean'],
) as dag:

    task_gen_clean = PythonOperator(
        task_id='generate_clean_data',
        python_callable=generate_clean_big_data,
    )

    task_gen_clean
