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
"""Kafka producer wrapper — delegates to confluent-kafka via kafka_queue."""

from __future__ import annotations

import json
import logging

from ingestor_orchestrator.kafka_queue import create_producer, delivery_report

logger = logging.getLogger(__name__)


def get_kafka_producer():
    """Return a thread-safe KafkaProducer singleton."""
    return create_producer()


def enqueue_job(
    job_id: str, ckan_url: str = "", topic: str = None, retry: bool = False
):
    """Publish a job to Kafka for async processing."""
    from ingestor_orchestrator.config import settings

    target_topic = topic or (
        settings.kafka_topic_retry if retry else settings.kafka_topic
    )
    payload = json.dumps({"job_id": job_id, "ckan_url": ckan_url}).encode()
    producer = get_kafka_producer()
    producer.produce(target_topic, payload, callback=delivery_report)
    producer.flush(timeout=10)
    logger.info(f"Job {job_id} enqueued to {target_topic}")
