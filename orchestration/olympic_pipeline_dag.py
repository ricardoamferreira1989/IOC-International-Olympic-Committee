from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "IOC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ioc_olympic_pipeline",
    default_args=default_args,
    description="IOC Olympic Batch Data Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="python /IOC/IOC/src/main.py",
    )