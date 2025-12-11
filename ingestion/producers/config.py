"""
Configuration for the Coinbase producer.
"""

import os

# Coinbase WebSocket configuration
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
TRADING_PAIRS = os.getenv("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD,DOGE-USD").split(",")
CHANNELS = os.getenv("CHANNELS", "matches").split(",")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "coinbase.raw.trades")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "coinbase.dlq")

# Prometheus metrics
PROMETHEUS_PORT = int(os.getenv("PROMETHEUS_PORT", "8000"))

# Reconnection settings
RECONNECT_DELAY_INITIAL = float(os.getenv("RECONNECT_DELAY_INITIAL", "1.0"))
RECONNECT_DELAY_MAX = float(os.getenv("RECONNECT_DELAY_MAX", "60.0"))
RECONNECT_DELAY_MULTIPLIER = float(os.getenv("RECONNECT_DELAY_MULTIPLIER", "2.0"))
