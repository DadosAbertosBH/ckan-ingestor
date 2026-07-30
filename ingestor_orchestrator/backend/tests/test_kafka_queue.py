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
"""Tests for kafka-python based producer and consumer factory."""

from unittest.mock import patch

import pytest
from ingestor_orchestrator.config import Settings
from ingestor_orchestrator.kafka_queue import create_kafka_consumer, get_kafka_producer


@pytest.fixture
def kafka_settings():
    return Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test.topic",
        kafka_topic_retry="test.topic.retry",
        kafka_group_id="test-group",
    )


class TestGetKafkaProducer:
    @patch("kafka.KafkaProducer")
    def test_creates_producer_with_correct_config(self, mock_producer, kafka_settings):
        import ingestor_orchestrator.kafka_queue as kq

        kq._producer = None
        producer = get_kafka_producer()

        mock_producer.assert_called_once()
        kwargs = mock_producer.call_args[1]
        assert kwargs["bootstrap_servers"] == "localhost:9092"
        assert kwargs["acks"] == "all"
        assert kwargs["retries"] == 5
        assert producer == mock_producer.return_value

    @patch("kafka.KafkaProducer")
    def test_singleton_caches_producer(self, mock_producer, kafka_settings):
        import ingestor_orchestrator.kafka_queue as kq

        kq._producer = None
        p1 = get_kafka_producer()
        p2 = get_kafka_producer()
        assert p1 is p2
        assert mock_producer.call_count == 1


class TestCreateKafkaConsumer:
    @patch("kafka.KafkaConsumer")
    def test_creates_consumer_with_correct_config(
        self, mock_consumer, kafka_settings
    ):
        consumer = create_kafka_consumer("test.topic", "test-group")

        mock_consumer.assert_called_once()
        args = mock_consumer.call_args[0]
        assert args[0] == "test.topic"
        kwargs = mock_consumer.call_args[1]
        assert kwargs["bootstrap_servers"] == "localhost:9092"
        assert kwargs["group_id"] == "test-group"
        assert kwargs["auto_offset_reset"] == "earliest"
        assert kwargs["enable_auto_commit"] is False
        assert kwargs["max_poll_interval_ms"] == 1_800_000
        assert kwargs["max_poll_records"] == 10
        assert consumer == mock_consumer.return_value
