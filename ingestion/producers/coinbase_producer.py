#!/usr/bin/env python3
"""
Coinbase WebSocket Producer

Connects to the Coinbase WebSocket feed and publishes trade events to Kafka.
Features:
- Automatic reconnection with exponential backoff
- Prometheus metrics for monitoring
- Dead letter queue for failed messages
"""

import asyncio
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

import websockets
from confluent_kafka import Producer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .config import (
    COINBASE_WS_URL,
    TRADING_PAIRS,
    CHANNELS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_DLQ_TOPIC,
    PROMETHEUS_PORT,
    RECONNECT_DELAY_INITIAL,
    RECONNECT_DELAY_MAX,
    RECONNECT_DELAY_MULTIPLIER,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("coinbase_producer")

# Prometheus metrics
MESSAGES_RECEIVED = Counter(
    "coinbase_messages_received_total",
    "Total messages received from Coinbase",
    ["channel", "product_id"],
)
MESSAGES_PRODUCED = Counter(
    "coinbase_messages_produced_total",
    "Total messages produced to Kafka",
    ["topic"],
)
MESSAGES_FAILED = Counter(
    "coinbase_messages_failed_total",
    "Total messages that failed to produce",
    ["reason"],
)
WEBSOCKET_RECONNECTS = Counter(
    "coinbase_websocket_reconnects_total",
    "Total WebSocket reconnection attempts",
)
WEBSOCKET_CONNECTED = Gauge(
    "coinbase_websocket_connected",
    "WebSocket connection status (1=connected, 0=disconnected)",
)
MESSAGE_LATENCY = Histogram(
    "coinbase_message_latency_seconds",
    "Latency from Coinbase timestamp to ingestion",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


class CoinbaseProducer:
    """Coinbase WebSocket to Kafka producer."""

    def __init__(self):
        self.producer: Optional[Producer] = None
        self.running = True
        self.reconnect_delay = RECONNECT_DELAY_INITIAL

    def _create_producer(self) -> Producer:
        """Create Kafka producer with configuration."""
        config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "coinbase-producer",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
            "linger.ms": 5,
            "batch.size": 16384,
            "compression.type": "snappy",
        }
        return Producer(config)

    def _delivery_callback(self, err, msg):
        """Callback for Kafka delivery reports."""
        if err:
            logger.error(f"Message delivery failed: {err}")
            MESSAGES_FAILED.labels(reason="delivery_failed").inc()
        else:
            MESSAGES_PRODUCED.labels(topic=msg.topic()).inc()

    def _produce_message(self, message: dict):
        """Produce a message to Kafka."""
        try:
            # Extract key for partitioning (product_id)
            key = message.get("product_id", "unknown")

            # Add ingestion timestamp
            message["_ingested_at"] = datetime.now(timezone.utc).isoformat()

            # Serialize and produce
            value = json.dumps(message).encode("utf-8")

            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=value,
                callback=self._delivery_callback,
            )

            # Trigger delivery reports
            self.producer.poll(0)

        except BufferError:
            logger.warning("Kafka buffer full, waiting...")
            self.producer.poll(1)
            # Retry once
            try:
                self.producer.produce(
                    topic=KAFKA_TOPIC,
                    key=key.encode("utf-8"),
                    value=value,
                    callback=self._delivery_callback,
                )
            except Exception as e:
                logger.error(f"Failed to produce after retry: {e}")
                MESSAGES_FAILED.labels(reason="buffer_full").inc()
                self._send_to_dlq(message, str(e))

        except Exception as e:
            logger.error(f"Failed to produce message: {e}")
            MESSAGES_FAILED.labels(reason="produce_error").inc()
            self._send_to_dlq(message, str(e))

    def _send_to_dlq(self, message: dict, error: str):
        """Send failed message to dead letter queue."""
        try:
            dlq_message = {
                "original_message": message,
                "error": error,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.producer.produce(
                topic=KAFKA_DLQ_TOPIC,
                value=json.dumps(dlq_message).encode("utf-8"),
            )
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

    def _build_subscribe_message(self) -> str:
        """Build WebSocket subscription message."""
        return json.dumps({
            "type": "subscribe",
            "product_ids": TRADING_PAIRS,
            "channels": CHANNELS,
        })

    async def _handle_message(self, message: str):
        """Handle incoming WebSocket message."""
        try:
            data = json.loads(message)
            msg_type = data.get("type", "")

            # Skip subscription confirmations and heartbeats
            if msg_type in ("subscriptions", "heartbeat"):
                logger.debug(f"Received {msg_type} message")
                return

            # Track metrics for trade messages
            if msg_type in ("match", "last_match"):
                product_id = data.get("product_id", "unknown")
                MESSAGES_RECEIVED.labels(channel="matches", product_id=product_id).inc()

                # Calculate latency if timestamp available
                if "time" in data:
                    try:
                        msg_time = datetime.fromisoformat(data["time"].replace("Z", "+00:00"))
                        latency = (datetime.now(timezone.utc) - msg_time).total_seconds()
                        MESSAGE_LATENCY.observe(latency)
                    except Exception:
                        pass

                # Produce to Kafka
                self._produce_message(data)

            elif msg_type == "ticker":
                product_id = data.get("product_id", "unknown")
                MESSAGES_RECEIVED.labels(channel="ticker", product_id=product_id).inc()
                self._produce_message(data)

            elif msg_type == "error":
                logger.error(f"Coinbase error: {data.get('message', 'Unknown error')}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            MESSAGES_FAILED.labels(reason="parse_error").inc()

    async def _connect_and_stream(self):
        """Connect to WebSocket and stream messages."""
        try:
            logger.info(f"Connecting to {COINBASE_WS_URL}")

            async with websockets.connect(
                COINBASE_WS_URL,
                ping_interval=30,
                ping_timeout=10,
            ) as ws:
                WEBSOCKET_CONNECTED.set(1)
                self.reconnect_delay = RECONNECT_DELAY_INITIAL
                logger.info("Connected to Coinbase WebSocket")

                # Subscribe to channels
                subscribe_msg = self._build_subscribe_message()
                await ws.send(subscribe_msg)
                logger.info(f"Subscribed to {TRADING_PAIRS} on channels {CHANNELS}")

                # Stream messages
                async for message in ws:
                    if not self.running:
                        break
                    await self._handle_message(message)

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
        finally:
            WEBSOCKET_CONNECTED.set(0)

    async def run(self):
        """Main run loop with reconnection logic."""
        # Initialize Kafka producer
        self.producer = self._create_producer()
        logger.info(f"Kafka producer initialized, target topic: {KAFKA_TOPIC}")

        # Start Prometheus metrics server
        start_http_server(PROMETHEUS_PORT)
        logger.info(f"Prometheus metrics available on port {PROMETHEUS_PORT}")

        while self.running:
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")

            if self.running:
                WEBSOCKET_RECONNECTS.inc()
                logger.info(f"Reconnecting in {self.reconnect_delay:.1f}s...")
                await asyncio.sleep(self.reconnect_delay)

                # Exponential backoff
                self.reconnect_delay = min(
                    self.reconnect_delay * RECONNECT_DELAY_MULTIPLIER,
                    RECONNECT_DELAY_MAX,
                )

        # Cleanup
        if self.producer:
            logger.info("Flushing Kafka producer...")
            self.producer.flush(timeout=10)
            logger.info("Producer flushed")

    def stop(self):
        """Stop the producer gracefully."""
        logger.info("Shutting down producer...")
        self.running = False


def main():
    """Entry point."""
    producer = CoinbaseProducer()

    # Handle signals for graceful shutdown
    def signal_handler(sig, frame):
        producer.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the producer
    try:
        asyncio.run(producer.run())
    except KeyboardInterrupt:
        pass

    logger.info("Producer stopped")
    sys.exit(0)


if __name__ == "__main__":
    main()
