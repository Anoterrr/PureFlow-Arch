"""
DAG to generate DIRTY synthetic data into the Landing Zone.
"""
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure src is in PYTHONPATH
sys.path.append('/opt/airflow/src')

from utils.generate_dirty_data import generate_dirty_big_data

DEFAULT_ARGS = {
    'owner': 'analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 29),
    'retries': 0,
}

with DAG(
    'generate_dirty_landing_data',
    default_args=DEFAULT_ARGS,
    description='Generates 1M rows with INTENTIONAL ANOMALIES in Landing Zone',
    schedule_interval=None,
    catchup=False,
    tags=['utils', 'data_gen', 'dirty'],
) as dag:

    task_gen_dirty = PythonOperator(
        task_id='generate_dirty_data',
        python_callable=generate_dirty_big_data,
    )

    task_gen_dirty
