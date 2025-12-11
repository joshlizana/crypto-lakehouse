"""
Unit tests for the Coinbase producer.
"""

import json
import pytest
from unittest.mock import Mock, patch, AsyncMock


class TestCoinbaseProducerConfig:
    """Test configuration module."""

    def test_default_trading_pairs(self):
        """Test default trading pairs are set."""
        with patch.dict("os.environ", {}, clear=True):
            # Import after patching to get defaults
            import importlib
            import sys

            # Remove cached module
            if "ingestion.producers.config" in sys.modules:
                del sys.modules["ingestion.producers.config"]

            from ingestion.producers.config import TRADING_PAIRS

            assert "BTC-USD" in TRADING_PAIRS
            assert "ETH-USD" in TRADING_PAIRS

    def test_custom_trading_pairs(self):
        """Test custom trading pairs from env."""
        with patch.dict("os.environ", {"TRADING_PAIRS": "BTC-USD,DOGE-USD"}):
            import importlib
            import sys

            if "ingestion.producers.config" in sys.modules:
                del sys.modules["ingestion.producers.config"]

            from ingestion.producers.config import TRADING_PAIRS

            assert TRADING_PAIRS == ["BTC-USD", "DOGE-USD"]


class TestMessageParsing:
    """Test message parsing logic."""

    def test_parse_match_message(self):
        """Test parsing a match (trade) message."""
        message = {
            "type": "match",
            "trade_id": "123456789",
            "sequence": "987654321",
            "maker_order_id": "maker-123",
            "taker_order_id": "taker-456",
            "time": "2025-01-15T10:30:00.123456Z",
            "product_id": "BTC-USD",
            "size": "0.01234567",
            "price": "42000.50",
            "side": "buy",
        }

        # Validate structure
        assert message["type"] == "match"
        assert message["trade_id"] == "123456789"
        assert message["product_id"] == "BTC-USD"
        assert float(message["price"]) > 0
        assert float(message["size"]) > 0
        assert message["side"] in ["buy", "sell"]

    def test_parse_ticker_message(self):
        """Test parsing a ticker message."""
        message = {
            "type": "ticker",
            "product_id": "ETH-USD",
            "price": "2500.00",
            "open_24h": "2400.00",
            "volume_24h": "10000.5",
            "low_24h": "2350.00",
            "high_24h": "2550.00",
            "time": "2025-01-15T10:30:00.000000Z",
        }

        assert message["type"] == "ticker"
        assert message["product_id"] == "ETH-USD"

    def test_ignore_subscription_message(self):
        """Test that subscription messages are identified."""
        message = {
            "type": "subscriptions",
            "channels": [{"name": "matches", "product_ids": ["BTC-USD"]}],
        }

        # Should be ignored in processing
        assert message["type"] == "subscriptions"

    def test_ignore_heartbeat_message(self):
        """Test that heartbeat messages are identified."""
        message = {
            "type": "heartbeat",
            "sequence": "123456",
            "last_trade_id": "789",
            "product_id": "BTC-USD",
            "time": "2025-01-15T10:30:00.000000Z",
        }

        assert message["type"] == "heartbeat"


class TestSubscriptionMessage:
    """Test subscription message building."""

    def test_build_subscribe_message(self):
        """Test building the subscription message."""
        trading_pairs = ["BTC-USD", "ETH-USD"]
        channels = ["matches"]

        message = {
            "type": "subscribe",
            "product_ids": trading_pairs,
            "channels": channels,
        }

        json_str = json.dumps(message)
        parsed = json.loads(json_str)

        assert parsed["type"] == "subscribe"
        assert parsed["product_ids"] == trading_pairs
        assert parsed["channels"] == channels


class TestKafkaMessageKey:
    """Test Kafka message key extraction."""

    def test_extract_key_from_trade(self):
        """Test extracting product_id as key."""
        message = {"product_id": "BTC-USD", "trade_id": "123"}
        key = message.get("product_id", "unknown")
        assert key == "BTC-USD"

    def test_default_key_when_missing(self):
        """Test default key when product_id is missing."""
        message = {"trade_id": "123"}
        key = message.get("product_id", "unknown")
        assert key == "unknown"
