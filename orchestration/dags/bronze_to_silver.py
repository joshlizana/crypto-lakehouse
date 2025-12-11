"""
Airflow DAG: Bronze to Silver Processing

Runs the Spark batch job to process Bronze trades into Silver.
Schedule: Hourly
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.4.3",
])


def log_start(**context):
    print(f"Starting Bronze to Silver job at {context['ts']}")


def log_complete(**context):
    print(f"Bronze to Silver job completed at {datetime.now()}")


with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    description="Process Bronze trades into Silver (dedupe, type cast, merge)",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lakehouse", "spark", "bronze", "silver"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/spark-jobs/bronze_to_silver.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.type": "nessie",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.sql.catalog.nessie.ref": "main",
            "spark.sql.catalog.nessie.warehouse": "s3a://warehouse/",
            "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3:8333",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "admin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        env_vars={
            "BATCH_LOOKBACK_HOURS": "2",
        },
    )

    complete = PythonOperator(
        task_id="log_complete",
        python_callable=log_complete,
    )

    start >> bronze_to_silver >> complete
