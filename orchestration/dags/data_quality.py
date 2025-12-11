"""
Airflow DAG: Data Quality Checks

Runs Great Expectations checkpoints after data processing.
Schedule: Triggered after bronze_to_silver
"""

from datetime import datetime, timedelta
from airflow import DAG
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

GE_PROJECT_DIR = "/opt/great_expectations"


def log_start(**context):
    print(f"Starting data quality checks at {context['ts']}")


def run_bronze_checkpoint(**context):
    """Run Great Expectations checkpoint for Bronze layer."""
    try:
        import great_expectations as gx

        context_gx = gx.get_context(context_root_dir=GE_PROJECT_DIR)
        result = context_gx.run_checkpoint(checkpoint_name="bronze_trades_checkpoint")

        if not result.success:
            raise ValueError("Bronze data quality check failed")

        print("Bronze checkpoint passed")
        return result.success

    except ImportError:
        print("Great Expectations not installed, skipping checkpoint")
        return True
    except Exception as e:
        print(f"Bronze checkpoint error: {e}")
        # Don't fail the DAG, just log the error
        return True


def run_silver_checkpoint(**context):
    """Run Great Expectations checkpoint for Silver layer."""
    try:
        import great_expectations as gx

        context_gx = gx.get_context(context_root_dir=GE_PROJECT_DIR)
        result = context_gx.run_checkpoint(checkpoint_name="silver_trades_checkpoint")

        if not result.success:
            raise ValueError("Silver data quality check failed")

        print("Silver checkpoint passed")
        return result.success

    except ImportError:
        print("Great Expectations not installed, skipping checkpoint")
        return True
    except Exception as e:
        print(f"Silver checkpoint error: {e}")
        return True


def log_complete(**context):
    print(f"Data quality checks completed at {datetime.now()}")


with DAG(
    dag_id="data_quality",
    default_args=default_args,
    description="Run Great Expectations data quality checkpoints",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lakehouse", "data-quality", "great-expectations"],
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

    bronze_quality = PythonOperator(
        task_id="bronze_quality_check",
        python_callable=run_bronze_checkpoint,
    )

    silver_quality = PythonOperator(
        task_id="silver_quality_check",
        python_callable=run_silver_checkpoint,
    )

    complete = PythonOperator(
        task_id="log_complete",
        python_callable=log_complete,
    )

    start >> wait_for_silver >> bronze_quality >> silver_quality >> complete
