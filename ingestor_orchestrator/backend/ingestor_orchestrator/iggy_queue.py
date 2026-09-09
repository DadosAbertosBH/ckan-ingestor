# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Apache Iggy producer and consumer helpers."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from datetime import timedelta

from apache_iggy import (
    AutoCommit,
    AutoCommitAfter,
    IggyClient,
    PollingStrategy,
    SendMessage,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PublishMetadata:
    broker_type: str
    stream: str
    topic: str
    partition: int
    offset: int | None = None


class IggyMessageBus:
    """Owns one connected Iggy client and the application topology."""

    def __init__(self, settings, *, client=None):
        self._settings = settings
        self._client = client
        self._connected = client is not None
        self._connect_lock = asyncio.Lock()

    @property
    def client(self):
        return self._client

    async def connect(self) -> None:
        if self._connected:
            return
        async with self._connect_lock:
            if self._connected:
                return
            self._client = IggyClient.from_connection_string(
                self._settings.iggy_connection_string
            )
            await self._client.connect()
            await self._ensure_topology()
            self._connected = True
            logger.info(
                "Iggy client connected to stream %s", self._settings.iggy_stream
            )

    async def invalidate(self) -> None:
        """Discard a client after its broker connection is no longer usable."""
        async with self._connect_lock:
            self._client = None
            self._connected = False

    async def _ensure_topology(self) -> None:
        stream = self._settings.iggy_stream
        if await self._client.get_stream(stream) is None:
            try:
                await self._client.create_stream(name=stream)
            except Exception:
                if await self._client.get_stream(stream) is None:
                    raise

        for topic, partitions in (
            (self._settings.iggy_topic, self._settings.iggy_partitions),
            (self._settings.iggy_topic_retry, self._settings.iggy_partitions),
            (self._settings.iggy_topic_results, 1),
            (self._settings.iggy_metadata_sync_topic, 1),
            (self._settings.iggy_metadata_sync_result_topic, 1),
        ):
            if await self._client.get_topic(stream, topic) is None:
                try:
                    await self._client.create_topic(
                        stream=stream,
                        name=topic,
                        partitions_count=partitions,
                        replication_factor=1,
                    )
                except Exception:
                    if await self._client.get_topic(stream, topic) is None:
                        raise

    def partition_for(self, key: str, topic: str | None = None) -> int:
        if topic in {
            self._settings.iggy_metadata_sync_topic,
            self._settings.iggy_metadata_sync_result_topic,
        }:
            return 0
        digest = hashlib.sha256(key.encode()).digest()
        return int.from_bytes(digest[:4], "big") % self._settings.iggy_partitions

    async def publish(self, topic: str, payload: bytes, *, key: str) -> PublishMetadata:
        await self.connect()
        partition = self.partition_for(key, topic)
        await self._client.send_messages(
            stream=self._settings.iggy_stream,
            topic=topic,
            partitioning=partition,
            messages=[SendMessage(payload)],
        )
        return PublishMetadata(
            broker_type="iggy",
            stream=self._settings.iggy_stream,
            topic=topic,
            partition=partition,
        )

    async def result_consumer(self):
        await self.connect()
        return await self._client.consumer_group(
            name=self._settings.iggy_result_group_id,
            stream=self._settings.iggy_stream,
            topic=self._settings.iggy_topic_results,
            polling_strategy=PollingStrategy.Next(),
            batch_length=10,
            poll_interval=timedelta(
                milliseconds=self._settings.iggy_consumer_poll_interval_ms
            ),
            auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()),
            create_consumer_group_if_not_exists=True,
            auto_join_consumer_group=True,
        )

    async def metadata_sync_result_consumer(self):
        await self.connect()
        return await self._client.consumer_group(
            name=self._settings.iggy_metadata_sync_result_group_id,
            stream=self._settings.iggy_stream,
            topic=self._settings.iggy_metadata_sync_result_topic,
            polling_strategy=PollingStrategy.Next(),
            batch_length=10,
            poll_interval=timedelta(
                milliseconds=self._settings.iggy_consumer_poll_interval_ms
            ),
            auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()),
            create_consumer_group_if_not_exists=True,
            auto_join_consumer_group=True,
        )

    async def ping(self) -> None:
        await self.connect()
        await self._client.ping()


_bus: IggyMessageBus | None = None


def get_iggy_bus() -> IggyMessageBus:
    global _bus
    if _bus is None:
        from ingestor_orchestrator.config import settings

        _bus = IggyMessageBus(settings)
    return _bus
