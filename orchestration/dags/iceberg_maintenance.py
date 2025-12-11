"""
Airflow DAG: Iceberg Maintenance

Runs daily maintenance tasks for Iceberg tables:
- Compaction (rewrite small files)
- Snapshot expiration (remove old snapshots)

Schedule: Daily at 3 AM
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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.76.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.4.3",
])

# Inline compaction script
COMPACTION_SCRIPT = '''
from pyspark.sql import SparkSession
import sys
sys.path.insert(0, "/opt/spark-jobs")
from spark_config import get_spark_session

spark = get_spark_session("iceberg-maintenance")

tables = [
    "nessie.bronze.trades",
    "nessie.silver.trades",
]

for table in tables:
    try:
        print(f"Compacting {table}...")
        spark.sql(f"CALL nessie.system.rewrite_data_files(table => '{table}')")
        print(f"Compaction complete for {table}")
    except Exception as e:
        print(f"Compaction failed for {table}: {e}")

for table in tables:
    try:
        print(f"Expiring snapshots for {table}...")
        spark.sql(f"""
            CALL nessie.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")} 00:00:00'
            )
        """)
        print(f"Snapshot expiration complete for {table}")
    except Exception as e:
        print(f"Snapshot expiration failed for {table}: {e}")

spark.stop()
'''


def log_start(**context):
    print(f"Starting Iceberg maintenance at {context['ts']}")


def log_complete(**context):
    print(f"Iceberg maintenance completed at {datetime.now()}")


with DAG(
    dag_id="iceberg_maintenance",
    default_args=default_args,
    description="Daily Iceberg table maintenance (compaction, snapshot expiry)",
    schedule_interval="0 3 * * *",  # 3 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lakehouse", "iceberg", "maintenance"],
) as dag:

    start = PythonOperator(
        task_id="log_start",
        python_callable=log_start,
    )

    # Run compaction via Spark
    compaction = SparkSubmitOperator(
        task_id="iceberg_compaction",
        application="/opt/spark-jobs/compaction.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.type": "nessie",
            "spark.sql.catalog.nessie.uri": "http://nessie:19120/api/v1",
            "spark.hadoop.fs.s3a.endpoint": "http://seaweedfs-s3:8333",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "admin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )

    complete = PythonOperator(
        task_id="log_complete",
        python_callable=log_complete,
    )

    start >> compaction >> complete
