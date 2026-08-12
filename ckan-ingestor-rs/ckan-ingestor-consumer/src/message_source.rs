// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use rdkafka::TopicPartitionList;
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, OwnedMessage};
use std::future::Future;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// MessageSource trait
// ---------------------------------------------------------------------------

pub trait MessageSource {
    type Msg: Message + Send;

    fn recv(&self) -> impl std::future::Future<Output = Result<Self::Msg, KafkaError>> + Send;

    fn commit(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send;
}

// ---------------------------------------------------------------------------
// PartitionSource — production implementation
// ---------------------------------------------------------------------------

/// Wraps a partition queue with the main consumer so that commits are
/// routed to the consumer while recv comes from the queue.
pub struct PartitionSource<C: ConsumerContext, R = rdkafka::util::DefaultRuntime> {
    queue: StreamPartitionQueue<C, R>,
    consumer: Arc<StreamConsumer<C, R>>,
}

impl<C, R> PartitionSource<C, R>
where
    C: ConsumerContext + Send + Sync + 'static,
    R: rdkafka::util::AsyncRuntime,
{
    pub fn new(queue: StreamPartitionQueue<C, R>, consumer: Arc<StreamConsumer<C, R>>) -> Self {
        Self { queue, consumer }
    }
}

impl<C, R> MessageSource for PartitionSource<C, R>
where
    C: ConsumerContext + Send + Sync + 'static,
    R: rdkafka::util::AsyncRuntime,
{
    type Msg = OwnedMessage;

    fn recv(&self) -> impl Future<Output = Result<Self::Msg, KafkaError>> + Send {
        async {
            StreamPartitionQueue::recv(&self.queue)
                .await
                .map(|m| m.detach())
        }
    }

    fn commit(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> impl Future<Output = Result<(), KafkaError>> + Send {
        let mut tpl = TopicPartitionList::new();
        {
            let mut elem = tpl.add_partition(topic, partition);
            let _ = elem.set_offset(rdkafka::Offset::Offset(offset + 1));
        }
        let result = Consumer::commit(self.consumer.as_ref(), &tpl, CommitMode::Sync);
        std::future::ready(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rdkafka::message::{OwnedHeaders, Timestamp};
    use std::sync::{Arc, Mutex};
    use tokio::sync::mpsc;

    pub(crate) struct MockMsg {
        pub payload: Vec<u8>,
        pub offset: i64,
    }

    impl Message for MockMsg {
        type Headers = OwnedHeaders;

        fn payload(&self) -> Option<&[u8]> {
            if self.payload.is_empty() {
                None
            } else {
                Some(&self.payload)
            }
        }
        fn key(&self) -> Option<&[u8]> {
            None
        }
        fn topic(&self) -> &str {
            "mock"
        }
        fn partition(&self) -> i32 {
            0
        }
        fn offset(&self) -> i64 {
            self.offset
        }
        fn timestamp(&self) -> Timestamp {
            Timestamp::NotAvailable
        }
        unsafe fn payload_mut(&mut self) -> Option<&mut [u8]> {
            None
        }
        fn headers(&self) -> Option<&OwnedHeaders> {
            None
        }
    }

    pub(crate) struct MockSource {
        rx: tokio::sync::Mutex<mpsc::Receiver<MockMsg>>,
        pub committed: Arc<Mutex<Vec<(String, i32, i64)>>>,
    }

    impl MockSource {
        pub fn new(rx: mpsc::Receiver<MockMsg>) -> Self {
            Self {
                rx: tokio::sync::Mutex::new(rx),
                committed: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl MessageSource for MockSource {
        type Msg = MockMsg;

        fn recv(
            &self,
        ) -> impl std::future::Future<Output = Result<MockMsg, rdkafka::error::KafkaError>> + Send
        {
            async {
                self.rx
                    .lock()
                    .await
                    .recv()
                    .await
                    .ok_or(rdkafka::error::KafkaError::Canceled)
            }
        }

        fn commit(
            &self,
            topic: &str,
            partition: i32,
            offset: i64,
        ) -> impl std::future::Future<Output = Result<(), rdkafka::error::KafkaError>> + Send
        {
            self.committed
                .lock()
                .unwrap()
                .push((topic.into(), partition, offset));
            std::future::ready(Ok(()))
        }
    }
}
