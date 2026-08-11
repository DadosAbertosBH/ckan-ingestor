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

use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Message, OwnedMessage};

pub trait MessageSource {
    type Msg: Message + Send;

    fn recv(&self) -> impl Future<Output = Result<Self::Msg, KafkaError>> + Send;
}

impl MessageSource for StreamConsumer {
    type Msg = OwnedMessage;

    fn recv(&self) -> impl Future<Output = Result<Self::Msg, KafkaError>> + Send {
        async { StreamConsumer::recv(self).await.map(|m| m.detach()) }
    }
}

impl<C> MessageSource for StreamPartitionQueue<C>
where
    C: ConsumerContext,
{
    type Msg = OwnedMessage;

    async fn recv(&self) -> Result<Self::Msg, KafkaError> {
        StreamPartitionQueue::recv(self).await.map(|m| m.detach())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rdkafka::message::{OwnedHeaders, Timestamp};
    use tokio::sync::{Mutex, mpsc};

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
        rx: Mutex<mpsc::Receiver<MockMsg>>,
    }

    impl MockSource {
        pub fn new(rx: mpsc::Receiver<MockMsg>) -> Self {
            Self { rx: Mutex::new(rx) }
        }
    }

    impl MessageSource for MockSource {
        type Msg = MockMsg;

        async fn recv(&self) -> Result<MockMsg, rdkafka::error::KafkaError> {
            self.rx
                .lock()
                .await
                .recv()
                .await
                .ok_or(rdkafka::error::KafkaError::Canceled)
        }
    }
}
