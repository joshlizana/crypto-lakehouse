"""
Airflow DAG: dbt Gold Layer

Runs dbt models to build Gold layer aggregations.
Schedule: Hourly (after bronze_to_silver)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/dbt"


def log_start(**context):
    print(f"Starting dbt Gold models at {context['ts']}")


def log_complete(**context):
    print(f"dbt Gold models completed at {datetime.now()}")


with DAG(
    dag_id="dbt_gold",
    default_args=default_args,
    description="Run dbt models to build Gold layer (OHLCV, metrics)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lakehouse", "dbt", "gold"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
    )

    # Wait for bronze_to_silver to complete
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="bronze_to_silver",
        external_task_id="log_complete",
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    complete = PythonOperator(
        task_id="log_complete",
        python_callable=log_complete,
    )

    start >> wait_for_silver >> dbt_run >> dbt_test >> complete
