# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Kafka producer and consumer using confluent-kafka with queue support (v2.15+)."""

from __future__ import annotations

import logging

from confluent_kafka import Consumer, Producer

from ingestor_orchestrator.config import settings

logger = logging.getLogger(__name__)

_producer: Producer | None = None


def create_producer() -> Producer:
    """Return a singleton KafkaProducer."""
    global _producer
    if _producer is None:
        _producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "acks": "all",
                "retries": 5,
            }
        )
        logger.info(f"Kafka producer connected to {settings.kafka_bootstrap_servers}")
    return _producer


def create_consumer(topic: str, group_id: str) -> Consumer:
    """Create a Kafka consumer for the given topic and group."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "max.poll.interval.ms": 1_800_000,
        }
    )
    consumer.subscribe([topic])
    return consumer


def delivery_report(err, msg):
    """Callback for async producer delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
