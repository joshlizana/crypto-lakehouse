#!/usr/bin/env python3
"""
Create Kafka topics for the lakehouse.
"""

import os
from confluent_kafka.admin import AdminClient, NewTopic

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPICS = [
    {
        "name": "coinbase.raw.trades",
        "partitions": 4,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days
            "cleanup.policy": "delete",
        },
    },
    {
        "name": "coinbase.dlq",
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "cleanup.policy": "delete",
        },
    },
]


def get_admin_client():
    """Create Kafka admin client."""
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def create_topics():
    """Create all required Kafka topics."""
    admin = get_admin_client()

    # Get existing topics
    metadata = admin.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    # Filter out topics that already exist
    new_topics = []
    for topic_config in TOPICS:
        if topic_config["name"] in existing_topics:
            print(f"    Topic '{topic_config['name']}' already exists")
        else:
            new_topics.append(
                NewTopic(
                    topic=topic_config["name"],
                    num_partitions=topic_config["partitions"],
                    replication_factor=topic_config["replication_factor"],
                    config=topic_config.get("config", {}),
                )
            )

    if not new_topics:
        return

    # Create topics
    futures = admin.create_topics(new_topics)

    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"    Created topic '{topic_name}'")
        except Exception as e:
            if "TopicExistsException" in str(e):
                print(f"    Topic '{topic_name}' already exists")
            else:
                print(f"    Failed to create topic '{topic_name}': {e}")
                raise


if __name__ == "__main__":
    create_topics()
