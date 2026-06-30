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
"""Tests for confluent-kafka based producer and consumer factory."""

from unittest.mock import patch

import pytest
from ingestor_orchestrator.config import Settings
from ingestor_orchestrator.kafka_queue import create_consumer, create_producer


@pytest.fixture
def kafka_settings():
    return Settings(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic="test.topic",
        kafka_topic_retry="test.topic.retry",
        kafka_group_id="test-group",
    )


class TestCreateProducer:
    @patch("ingestor_orchestrator.kafka_queue.Producer")
    def test_creates_producer_with_correct_config(self, mock_producer, kafka_settings):
        import ingestor_orchestrator.kafka_queue as kq

        kq._producer = None
        producer = create_producer()

        mock_producer.assert_called_once()
        config = mock_producer.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["acks"] == "all"
        assert producer == mock_producer.return_value

    @patch("ingestor_orchestrator.kafka_queue.Producer")
    def test_singleton_caches_producer(self, mock_producer, kafka_settings):
        import ingestor_orchestrator.kafka_queue as kq

        kq._producer = None
        p1 = create_producer()
        p2 = create_producer()
        assert p1 is p2
        assert mock_producer.call_count == 1


class TestCreateConsumer:
    @patch("ingestor_orchestrator.kafka_queue.Consumer")
    def test_creates_consumer_with_correct_config(self, mock_consumer, kafka_settings):
        consumer = create_consumer("test.topic", "test-group")

        mock_consumer.assert_called_once()
        config = mock_consumer.call_args[0][0]
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == "test-group"
        assert config["enable.auto.commit"] is False
        assert config["auto.offset.reset"] == "earliest"
        assert consumer == mock_consumer.return_value
