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
pub use worker_handler::ConsumerWorker;

pub trait OutgoingMessage: Serialize {
    fn partition_key(&self) -> &str;
}

pub trait MessageProcessor {
    type IncomingMessage: DeserializeOwned;
    type OutgoingMessage: OutgoingMessage;
    fn process(&self, message: Self::IncomingMessage) -> impl Stream<Item = Self::OutgoingMessage>;
}

pub trait ResultPublisher<T: OutgoingMessage>: Clone {
    fn publish(&self, message: T) -> impl Future<Output = Result<()>>;
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
    use tokio::sync::mpsc;

    use crate::{BrokerMessage, MessageSource, OutgoingMessage, ResultPublisher};

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

    pub struct MockResultPublisher<OutcomingMessage> {
        pub published: Arc<Mutex<Vec<OutcomingMessage>>>,
    }

    impl<OutcomingMessage> Clone for MockResultPublisher<OutcomingMessage> {
        fn clone(&self) -> Self {
            Self {
                published: Arc::clone(&self.published),
            }
        }
    }

    impl<OutcomingMessage> Default for MockResultPublisher<OutcomingMessage> {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<OutcomingMessage> MockResultPublisher<OutcomingMessage> {
        pub fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl<OutcomingMessage: OutgoingMessage> ResultPublisher<OutcomingMessage>
        for MockResultPublisher<OutcomingMessage>
    {
        async fn publish(&self, result: OutcomingMessage) -> Result<()> {
            self.published.lock().unwrap().push(result);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use futures::StreamExt;
    use serde::{Deserialize, Serialize};

    use super::{BrokerMessage, MessageProcessor, MessageSource, OutgoingMessage, ResultPublisher};

    #[derive(Deserialize)]
    struct Input(u8);

    #[derive(Deserialize, Serialize)]
    struct Output(u8);

    impl OutgoingMessage for Output {
        fn partition_key(&self) -> &str {
            "test"
        }
    }

    #[derive(Clone)]
    struct Increment;

    impl MessageProcessor for Increment {
        type IncomingMessage = Input;
        type OutgoingMessage = Output;

        fn process(&self, message: Input) -> impl futures::Stream<Item = Output> {
            futures::stream::iter([Output(message.0 + 1)])
        }
    }

    #[tokio::test]
    async fn defines_a_typed_message_transformation_contract() {
        let output = Increment.process(Input(41)).next().await.unwrap();

        assert_eq!(output.0, 42);
    }

    #[derive(Clone)]
    struct Publisher;

    impl ResultPublisher<Output> for Publisher {
        async fn publish(&self, _: Output) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn defines_a_typed_result_publication_contract() {
        let publisher = Publisher;
        std::mem::drop(publisher.publish(Output(42)));
    }

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
