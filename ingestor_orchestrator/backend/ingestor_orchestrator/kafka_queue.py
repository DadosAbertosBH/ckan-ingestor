# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

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
"""Kafka producer and consumer for job messages."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

# Lazy singleton — created on first use per process.
_producer = None


def get_kafka_producer():
    """Return a thread-safe KafkaProducer singleton."""
    global _producer

    if _producer is None:
        from kafka import KafkaProducer

        from ingestor_orchestrator.config import settings

        _producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: v,  # raw bytes
            acks="all",
            retries=5,
        )
        logger.info(f"Kafka producer connected to {settings.kafka_bootstrap_servers}")
    return _producer


def create_kafka_consumer(topic: str, group_id: str) -> "KafkaConsumer":
    """Create a Kafka consumer for the given topic and group."""
    from kafka import KafkaConsumer

    from ingestor_orchestrator.config import settings

    return KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: v,
        max_poll_interval_ms=1_800_000,
        max_poll_records=10,
    )
