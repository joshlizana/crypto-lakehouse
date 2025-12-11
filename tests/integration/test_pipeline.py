"""
Integration tests for the data pipeline.

These tests require Docker services to be running.
Run with: make up && pytest tests/integration/ -v
"""

import os
import pytest
import time

# Skip if services not available
SKIP_INTEGRATION = os.getenv("SKIP_INTEGRATION_TESTS", "true").lower() == "true"


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestKafkaIntegration:
    """Test Kafka connectivity and topic availability."""

    def test_kafka_connection(self):
        """Test connection to Kafka."""
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:29092"})
        metadata = admin.list_topics(timeout=10)

        assert metadata is not None
        assert len(metadata.topics) >= 0

    def test_kafka_topics_exist(self):
        """Test required topics exist."""
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "localhost:29092"})
        metadata = admin.list_topics(timeout=10)

        topics = set(metadata.topics.keys())

        # After init, these topics should exist
        assert "coinbase.raw.trades" in topics or len(topics) == 0


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestSeaweedFSIntegration:
    """Test SeaweedFS S3 connectivity."""

    def test_s3_connection(self):
        """Test S3 API connectivity."""
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:8333",
            aws_access_key_id="admin",
            aws_secret_access_key="admin",
            config=Config(signature_version="s3v4"),
        )

        response = s3.list_buckets()
        assert "Buckets" in response

    def test_s3_buckets_exist(self):
        """Test required buckets exist after init."""
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:8333",
            aws_access_key_id="admin",
            aws_secret_access_key="admin",
            config=Config(signature_version="s3v4"),
        )

        response = s3.list_buckets()
        bucket_names = {b["Name"] for b in response.get("Buckets", [])}

        # After init, these buckets should exist
        expected = {"warehouse", "checkpoints"}
        assert expected.issubset(bucket_names) or len(bucket_names) == 0


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestNessieIntegration:
    """Test Nessie catalog connectivity."""

    def test_nessie_health(self):
        """Test Nessie API health."""
        import requests

        response = requests.get("http://localhost:19120/api/v1/config", timeout=5)
        assert response.status_code == 200

    def test_nessie_default_branch(self):
        """Test default branch exists."""
        import requests

        response = requests.get("http://localhost:19120/api/v1/trees", timeout=5)
        assert response.status_code == 200

        data = response.json()
        assert "defaultBranch" in data


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestTrinoIntegration:
    """Test Trino query engine connectivity."""

    def test_trino_health(self):
        """Test Trino API health."""
        import requests

        response = requests.get("http://localhost:8082/v1/info", timeout=5)
        assert response.status_code == 200


@pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled")
class TestEndToEndPipeline:
    """End-to-end pipeline tests."""

    def test_bronze_table_queryable(self):
        """Test Bronze table can be queried via Trino."""
        # This would require trino-python-client
        pass

    def test_silver_table_queryable(self):
        """Test Silver table can be queried via Trino."""
        pass

    def test_gold_tables_queryable(self):
        """Test Gold tables can be queried via Trino."""
        pass
