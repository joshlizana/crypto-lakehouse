"""
Shared Spark configuration for Iceberg + Nessie + SeaweedFS.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "lakehouse-crypto") -> SparkSession:
    """
    Create a SparkSession configured for Iceberg with Nessie catalog
    and SeaweedFS (S3-compatible) storage.
    """

    # Configuration from environment
    nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v1")
    nessie_ref = os.getenv("NESSIE_REF", "main")
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://seaweedfs-s3:8333")
    s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "admin")
    warehouse_path = os.getenv("WAREHOUSE_PATH", "s3a://warehouse/")

    builder = (
        SparkSession.builder
        .appName(app_name)
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # Nessie catalog configuration
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "nessie")
        .config("spark.sql.catalog.nessie.uri", nessie_uri)
        .config("spark.sql.catalog.nessie.ref", nessie_ref)
        .config("spark.sql.catalog.nessie.warehouse", warehouse_path)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        # S3/SeaweedFS configuration
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Default catalog
        .config("spark.sql.defaultCatalog", "nessie")
        # Performance tuning
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
    )

    return builder.getOrCreate()


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "coinbase.raw.trades")

# Streaming configuration
STREAMING_TRIGGER_INTERVAL = os.getenv("STREAMING_TRIGGER_INTERVAL", "5 seconds")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://checkpoints/bronze_streaming")
