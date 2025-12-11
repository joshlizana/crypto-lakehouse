#!/usr/bin/env python3
"""
Infrastructure initialization orchestrator.
Creates S3 buckets, Kafka topics, and Nessie namespaces.
"""

import sys
import time
from seaweedfs_buckets import create_buckets
from kafka_topics import create_topics
from nessie_setup import setup_nessie


def wait_for_service(name: str, check_fn, max_retries: int = 30, delay: int = 2):
    """Wait for a service to become available."""
    print(f"Waiting for {name}...")
    for i in range(max_retries):
        try:
            if check_fn():
                print(f"  {name} is ready")
                return True
        except Exception as e:
            if i == max_retries - 1:
                print(f"  {name} check failed: {e}")
        time.sleep(delay)
    print(f"  {name} not available after {max_retries * delay}s")
    return False


def main():
    print("=" * 60)
    print("Lakehouse Crypto - Infrastructure Initialization")
    print("=" * 60)
    print()

    # Step 1: Create S3 buckets
    print("[1/3] Creating S3 buckets...")
    try:
        create_buckets()
        print("  S3 buckets created successfully")
    except Exception as e:
        print(f"  Failed to create S3 buckets: {e}")
        sys.exit(1)

    print()

    # Step 2: Create Kafka topics
    print("[2/3] Creating Kafka topics...")
    try:
        create_topics()
        print("  Kafka topics created successfully")
    except Exception as e:
        print(f"  Failed to create Kafka topics: {e}")
        sys.exit(1)

    print()

    # Step 3: Setup Nessie catalog
    print("[3/3] Setting up Nessie catalog...")
    try:
        setup_nessie()
        print("  Nessie catalog configured successfully")
    except Exception as e:
        print(f"  Failed to setup Nessie: {e}")
        sys.exit(1)

    print()
    print("=" * 60)
    print("Infrastructure initialization complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
