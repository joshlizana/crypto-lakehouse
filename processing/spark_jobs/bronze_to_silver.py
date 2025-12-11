#!/usr/bin/env python3
"""
Spark Batch Job: Bronze → Silver

Processes raw Bronze trades into cleaned, deduplicated Silver trades.

Features:
- Deduplication by trade_id
- Type casting (string → decimal, timestamp)
- Latency metrics computation
- MERGE into Silver table (upsert)
- Incremental processing with watermark
"""

import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    lit,
    when,
    row_number,
    to_date,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, LongType

# Import shared config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_config import get_spark_session


# Configuration
LOOKBACK_HOURS = int(os.getenv("BATCH_LOOKBACK_HOURS", "2"))
LATE_ARRIVAL_THRESHOLD_SECONDS = int(os.getenv("LATE_ARRIVAL_THRESHOLD", "300"))  # 5 minutes


def create_silver_table(spark: SparkSession):
    """Create the Silver trades table if it doesn't exist."""

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS silver
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.trades (
            trade_id BIGINT,
            product_id STRING,
            price DECIMAL(18, 8),
            size DECIMAL(18, 8),
            side STRING,
            trade_time TIMESTAMP,
            ingested_at TIMESTAMP,
            _is_late_arrival BOOLEAN,
            _source_latency_ms BIGINT,
            _trade_date DATE
        )
        USING iceberg
        PARTITIONED BY (_trade_date, product_id)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.merge.mode' = 'copy-on-write'
        )
    """)

    print("Silver table created/verified: nessie.silver.trades")


def get_incremental_bronze_data(spark: SparkSession, lookback_hours: int):
    """Read Bronze data for the lookback window."""

    cutoff_time = datetime.utcnow() - timedelta(hours=lookback_hours)

    print(f"Reading Bronze data from {cutoff_time.isoformat()} onwards")

    bronze_df = (
        spark.read
        .format("iceberg")
        .load("nessie.bronze.trades")
        .filter(col("_ingested_at") >= lit(cutoff_time))
    )

    count = bronze_df.count()
    print(f"Found {count} records in Bronze for processing")

    return bronze_df


def transform_to_silver(bronze_df):
    """Transform Bronze data to Silver schema."""

    # Deduplicate by trade_id (keep earliest ingestion)
    window_spec = Window.partitionBy("trade_id").orderBy("_ingested_at")

    deduped_df = (
        bronze_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Transform and type cast
    silver_df = (
        deduped_df
        # Cast trade_id to bigint
        .withColumn("trade_id", col("trade_id").cast(LongType()))
        # Cast price and size to decimal
        .withColumn("price", col("price").cast(DecimalType(18, 8)))
        .withColumn("size", col("size").cast(DecimalType(18, 8)))
        # Parse trade time
        .withColumn("trade_time", to_timestamp(col("time")))
        # Rename ingested_at
        .withColumn("ingested_at", col("_ingested_at"))
        # Compute latency (ms between trade_time and ingested_at)
        .withColumn(
            "_source_latency_ms",
            (
                (unix_timestamp(col("ingested_at")) - unix_timestamp(col("trade_time"))) * 1000
            ).cast(LongType())
        )
        # Flag late arrivals (latency > threshold)
        .withColumn(
            "_is_late_arrival",
            col("_source_latency_ms") > lit(LATE_ARRIVAL_THRESHOLD_SECONDS * 1000)
        )
        # Add partition column
        .withColumn("_trade_date", to_date(col("trade_time")))
        # Select final columns
        .select(
            "trade_id",
            "product_id",
            "price",
            "size",
            "side",
            "trade_time",
            "ingested_at",
            "_is_late_arrival",
            "_source_latency_ms",
            "_trade_date",
        )
        # Filter invalid records
        .filter(col("trade_id").isNotNull())
        .filter(col("price").isNotNull())
        .filter(col("price") > 0)
        .filter(col("trade_time").isNotNull())
    )

    return silver_df


def merge_to_silver(spark: SparkSession, silver_df):
    """MERGE transformed data into Silver table."""

    # Register as temp view for SQL merge
    silver_df.createOrReplaceTempView("silver_updates")

    # Get count before merge
    updates_count = silver_df.count()
    print(f"Merging {updates_count} records into Silver table")

    if updates_count == 0:
        print("No records to merge")
        return

    # MERGE statement
    spark.sql("""
        MERGE INTO nessie.silver.trades AS target
        USING silver_updates AS source
        ON target.trade_id = source.trade_id
        WHEN MATCHED THEN UPDATE SET
            product_id = source.product_id,
            price = source.price,
            size = source.size,
            side = source.side,
            trade_time = source.trade_time,
            ingested_at = source.ingested_at,
            _is_late_arrival = source._is_late_arrival,
            _source_latency_ms = source._source_latency_ms,
            _trade_date = source._trade_date
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("Merge completed successfully")


def print_statistics(spark: SparkSession):
    """Print table statistics."""

    stats = spark.sql("""
        SELECT
            product_id,
            COUNT(*) as trade_count,
            MIN(trade_time) as earliest_trade,
            MAX(trade_time) as latest_trade,
            AVG(_source_latency_ms) as avg_latency_ms,
            SUM(CASE WHEN _is_late_arrival THEN 1 ELSE 0 END) as late_arrivals
        FROM nessie.silver.trades
        GROUP BY product_id
        ORDER BY product_id
    """)

    print("\nSilver Table Statistics:")
    print("-" * 80)
    stats.show(truncate=False)


def main():
    """Main entry point for the batch job."""
    print("=" * 60)
    print("Spark Batch: Bronze → Silver")
    print("=" * 60)
    print(f"Lookback window: {LOOKBACK_HOURS} hours")
    print(f"Late arrival threshold: {LATE_ARRIVAL_THRESHOLD_SECONDS} seconds")
    print("=" * 60)

    # Initialize Spark
    spark = get_spark_session("bronze-to-silver-batch")

    try:
        # Create Silver table if needed
        create_silver_table(spark)

        # Read Bronze data
        bronze_df = get_incremental_bronze_data(spark, LOOKBACK_HOURS)

        if bronze_df.count() == 0:
            print("No new data to process")
            return

        # Transform to Silver schema
        silver_df = transform_to_silver(bronze_df)

        # Merge into Silver table
        merge_to_silver(spark, silver_df)

        # Print statistics
        print_statistics(spark)

        print("\n" + "=" * 60)
        print("Bronze → Silver batch job completed successfully")
        print("=" * 60)

    except Exception as e:
        print(f"Error in batch job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
