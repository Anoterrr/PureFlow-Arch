import os
import yaml
import importlib
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def create_dag(config_path):
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    default_args = config.get("default_args", {})
    # Transform start_date string to datetime object
    if "start_date" in default_args:
        default_args["start_date"] = datetime.strptime(default_args["start_date"], "%Y-%m-%d")
    
    # Retry delay mapping
    if "retry_delay_minutes" in default_args:
        default_args["retry_delay"] = timedelta(minutes=default_args["retry_delay_minutes"])
        del default_args["retry_delay_minutes"]

    dag = DAG(
        dag_id=config["pipeline_name"],
        description=config.get("description", ""),
        schedule_interval=config.get("schedule"),
        default_args=default_args,
        catchup=False,
        tags=config.get("tags", []),
    )

    tasks_map = {}

    for task_cfg in config["tasks"]:
        task_id = task_cfg["id"]
        task_type = task_cfg["type"]

        if task_type == "python":
            # Dynamic import of python_callable
            module_name, func_name = task_cfg["python_callable"].rsplit(".", 1)
            
            # Special handling for class methods like SalesIngestor.ingest
            if ".SalesIngestor" in module_name:
                module_name = "ingestion.sales_ingest"
                def wrapper(**context):
                    from ingestion.sales_ingest import SalesIngestor
                    return SalesIngestor().ingest()
                python_callable = wrapper
            else:
                module = importlib.import_module(module_name)
                python_callable = getattr(module, func_name)

            tasks_map[task_id] = PythonOperator(
                task_id=task_id,
                python_callable=python_callable,
                dag=dag
            )

        elif task_type == "bash":
            tasks_map[task_id] = BashOperator(
                task_id=task_id,
                bash_command=task_cfg["command"],
                env=task_cfg.get("env", {}),
                dag=dag
            )

    # Define dependencies
    for dep_str in config.get("dependencies", []):
        upstream_id, downstream_id = [x.strip() for x in dep_str.split(">>")]
        tasks_map[upstream_id] >> tasks_map[downstream_id]

    return dag

# Scan for all YAML files in the pipelines directory
PIPELINES_DIR = os.path.join(os.path.dirname(__file__), "pipelines")
if os.path.exists(PIPELINES_DIR):
    for filename in os.listdir(PIPELINES_DIR):
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            path = os.path.join(PIPELINES_DIR, filename)
            dag_obj = create_dag(path)
            # Register the DAG globally for Airflow to discover it
            globals()[dag_obj.dag_id] = dag_obj
