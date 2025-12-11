#!/usr/bin/env python3
"""
Spark Streaming Job: Kafka → Bronze Iceberg

Consumes raw trade events from Kafka and writes them to the Bronze
Iceberg table with append-only semantics.

Features:
- Structured streaming with 5-second micro-batches
- Exactly-once semantics via checkpointing
- Automatic table creation if not exists
- Partitioning by ingestion date
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_date,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

# Import shared config
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_config import (
    get_spark_session,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    STREAMING_TRIGGER_INTERVAL,
    CHECKPOINT_LOCATION,
)


# Schema for Coinbase trade messages
TRADE_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("maker_order_id", StringType(), True),
    StructField("taker_order_id", StringType(), True),
    StructField("time", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("size", StringType(), True),
    StructField("price", StringType(), True),
    StructField("side", StringType(), True),
    StructField("_ingested_at", StringType(), True),
])


def create_bronze_table(spark: SparkSession):
    """Create the Bronze trades table if it doesn't exist."""

    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS bronze
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.trades (
            _ingested_at TIMESTAMP,
            _raw_payload STRING,
            _kafka_offset BIGINT,
            _kafka_partition INT,
            trade_id STRING,
            product_id STRING,
            price STRING,
            size STRING,
            side STRING,
            time STRING,
            _ingestion_date DATE
        )
        USING iceberg
        PARTITIONED BY (_ingestion_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    print("Bronze table created/verified: nessie.bronze.trades")


def process_batch(batch_df, batch_id):
    """Process each micro-batch."""
    if batch_df.isEmpty():
        return

    count = batch_df.count()
    print(f"Processing batch {batch_id} with {count} records")


def main():
    """Main entry point for the streaming job."""
    print("=" * 60)
    print("Spark Streaming: Kafka → Bronze Iceberg")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC_RAW}")
    print(f"Trigger: {STREAMING_TRIGGER_INTERVAL}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")
    print("=" * 60)

    # Initialize Spark
    spark = get_spark_session("bronze-streaming-writer")

    # Create Bronze table if needed
    create_bronze_table(spark)

    # Read from Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse and transform
    bronze_df = (
        kafka_df
        # Keep raw payload
        .withColumn("_raw_payload", col("value").cast("string"))
        # Parse JSON
        .withColumn("parsed", from_json(col("value").cast("string"), TRADE_SCHEMA))
        # Extract fields
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_kafka_offset", col("offset"))
        .withColumn("_kafka_partition", col("partition"))
        .withColumn("trade_id", col("parsed.trade_id"))
        .withColumn("product_id", col("parsed.product_id"))
        .withColumn("price", col("parsed.price"))
        .withColumn("size", col("parsed.size"))
        .withColumn("side", col("parsed.side"))
        .withColumn("time", col("parsed.time"))
        # Add partition column
        .withColumn("_ingestion_date", to_date(col("_ingested_at")))
        # Select final columns
        .select(
            "_ingested_at",
            "_raw_payload",
            "_kafka_offset",
            "_kafka_partition",
            "trade_id",
            "product_id",
            "price",
            "size",
            "side",
            "time",
            "_ingestion_date",
        )
        # Filter out non-trade messages (subscriptions, heartbeats)
        .filter(col("trade_id").isNotNull())
    )

    # Write to Iceberg
    query = (
        bronze_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime=STREAMING_TRIGGER_INTERVAL)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .option("fanout-enabled", "true")
        .toTable("nessie.bronze.trades")
    )

    print("Streaming query started. Writing to nessie.bronze.trades")
    print("Press Ctrl+C to stop...")

    # Wait for termination
    query.awaitTermination()


if __name__ == "__main__":
    main()
