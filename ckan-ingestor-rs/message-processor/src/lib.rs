// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use futures::Stream;
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

pub mod worker_handler;
pub use worker_handler::WorkerHandler;

pub struct OutgoingMessage<T: Serialize> {
    pub topic: String,
    pub partition_key: String,
    pub data: T,
}

impl<T: Serialize> OutgoingMessage<T> {
    pub fn new(topic: String, partition_key: String, data: T) -> Self {
        Self {
            topic,
            partition_key,
            data,
        }
    }
}

pub trait MessageProcessor {
    type IncomingMessage: DeserializeOwned;
    fn process(
        &self,
        message: Self::IncomingMessage,
    ) -> impl Stream<Item = OutgoingMessage<impl Serialize>>;
}

pub trait MessagePublisher {
    fn publish(&self, message: OutgoingMessage<impl Serialize>)
    -> impl Future<Output = Result<()>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMessage {
    pub payload: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

pub trait MessageSource {
    fn recv(&mut self) -> impl Future<Output = Result<BrokerMessage>>;
    fn commit(&mut self, partition: u32, offset: u64) -> impl Future<Output = Result<()>>;
}

#[cfg(feature = "test-support")]
pub mod test_support {
    use std::sync::{Arc, Mutex};

    use anyhow::{Result, anyhow};
    use serde::Serialize;
    use tokio::sync::mpsc;

    use crate::{BrokerMessage, MessagePublisher, MessageSource, OutgoingMessage};

    pub struct MockMessageSource {
        receiver: mpsc::Receiver<BrokerMessage>,
        pub committed: Arc<Mutex<Vec<(u32, u64)>>>,
    }

    impl MockMessageSource {
        pub fn new(receiver: mpsc::Receiver<BrokerMessage>) -> Self {
            Self {
                receiver,
                committed: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl MessageSource for MockMessageSource {
        async fn recv(&mut self) -> Result<BrokerMessage> {
            self.receiver
                .recv()
                .await
                .ok_or_else(|| anyhow!("mock source closed"))
        }

        async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
            self.committed.lock().unwrap().push((partition, offset));
            Ok(())
        }
    }

    pub struct MockResultPublisher {
        pub published: Arc<Mutex<Vec<OutgoingMessage<String>>>>,
    }

    impl MockResultPublisher {
        pub fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl Default for MockResultPublisher {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MessagePublisher for MockResultPublisher {
        async fn publish(&self, message: OutgoingMessage<impl Serialize>) -> Result<()> {
            self.published.lock().unwrap().push(OutgoingMessage::new(
                message.topic,
                message.partition_key,
                serde_json::to_string(&message.data)?,
            ));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::{BrokerMessage, MessageSource};

    struct Source;

    impl MessageSource for Source {
        async fn recv(&mut self) -> Result<BrokerMessage> {
            Ok(BrokerMessage {
                payload: vec![1],
                partition: 2,
                offset: 3,
            })
        }

        async fn commit(&mut self, _: u32, _: u64) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn exposes_the_broker_source_contract() {
        fn accepts_source(_: impl MessageSource) {}

        accepts_source(Source);
    }

    #[cfg(feature = "test-support")]
    #[test]
    fn provides_a_mock_message_source_for_consumer_tests() {
        fn accepts_source(_: impl MessageSource) {}

        let (_, receiver) = tokio::sync::mpsc::channel(1);
        accepts_source(crate::test_support::MockMessageSource::new(receiver));
    }
}
