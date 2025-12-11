#!/usr/bin/env python3
"""
Create S3 buckets in SeaweedFS for the lakehouse.
"""

import os
import boto3
from botocore.config import Config

# Configuration
S3_ENDPOINT = os.getenv("SEAWEEDFS_S3_ENDPOINT", "http://seaweedfs-s3:8333")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin")

BUCKETS = [
    "warehouse",     # Iceberg tables
    "checkpoints",   # Spark streaming checkpoints
    "bronze",        # Bronze layer (if using separate buckets)
    "silver",        # Silver layer
    "gold",          # Gold layer
]


def get_s3_client():
    """Create S3 client for SeaweedFS."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


def create_buckets():
    """Create all required S3 buckets."""
    s3 = get_s3_client()

    existing_buckets = set()
    try:
        response = s3.list_buckets()
        existing_buckets = {b["Name"] for b in response.get("Buckets", [])}
    except Exception:
        pass  # Bucket list might fail if no buckets exist

    for bucket in BUCKETS:
        if bucket in existing_buckets:
            print(f"    Bucket '{bucket}' already exists")
        else:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"    Created bucket '{bucket}'")
            except s3.exceptions.BucketAlreadyOwnedByYou:
                print(f"    Bucket '{bucket}' already exists")
            except Exception as e:
                print(f"    Failed to create bucket '{bucket}': {e}")
                raise


if __name__ == "__main__":
    create_buckets()
