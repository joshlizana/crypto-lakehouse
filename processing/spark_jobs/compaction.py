#!/usr/bin/env python3
"""
Spark Job: Iceberg Table Maintenance

Performs compaction and snapshot expiration on Iceberg tables.
"""

import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/spark-jobs")

try:
    from spark_config import get_spark_session
except ImportError:
    # Fallback for local testing
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from spark_config import get_spark_session


TABLES = [
    "nessie.bronze.trades",
    "nessie.silver.trades",
]

SNAPSHOT_RETENTION_DAYS = 7


def compact_table(spark, table: str):
    """Rewrite data files to reduce small files."""
    print(f"Compacting {table}...")
    try:
        spark.sql(f"""
            CALL nessie.system.rewrite_data_files(
                table => '{table}',
                options => map('min-input-files', '5')
            )
        """)
        print(f"  Compaction complete for {table}")
    except Exception as e:
        print(f"  Compaction failed for {table}: {e}")


def expire_snapshots(spark, table: str, retention_days: int):
    """Remove snapshots older than retention period."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Expiring snapshots older than {cutoff_str} for {table}...")
    try:
        spark.sql(f"""
            CALL nessie.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff_str}',
                retain_last => 5
            )
        """)
        print(f"  Snapshot expiration complete for {table}")
    except Exception as e:
        print(f"  Snapshot expiration failed for {table}: {e}")


def remove_orphan_files(spark, table: str):
    """Remove orphaned data files."""
    cutoff = datetime.now() - timedelta(days=3)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    print(f"Removing orphan files for {table}...")
    try:
        spark.sql(f"""
            CALL nessie.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff_str}'
            )
        """)
        print(f"  Orphan file removal complete for {table}")
    except Exception as e:
        print(f"  Orphan file removal failed for {table}: {e}")


def main():
    print("=" * 60)
    print("Iceberg Table Maintenance")
    print("=" * 60)
    print(f"Tables: {TABLES}")
    print(f"Snapshot retention: {SNAPSHOT_RETENTION_DAYS} days")
    print("=" * 60)

    spark = get_spark_session("iceberg-maintenance")

    try:
        for table in TABLES:
            print(f"\nProcessing {table}...")
            compact_table(spark, table)
            expire_snapshots(spark, table, SNAPSHOT_RETENTION_DAYS)
            remove_orphan_files(spark, table)

        print("\n" + "=" * 60)
        print("Maintenance complete")
        print("=" * 60)

    except Exception as e:
        print(f"Maintenance job failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
