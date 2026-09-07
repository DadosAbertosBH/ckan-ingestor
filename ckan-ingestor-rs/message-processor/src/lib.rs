// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::future::Future;

pub trait MessageProcessor<IncomingMessage: Serialize, OutcomingMessage: DeserializeOwned> {
    fn process(&self, message: IncomingMessage) -> OutcomingMessage;
}

pub trait ResultPublisher<OutcomingMessage>: Clone {
    fn publish(&self, result: OutcomingMessage) -> impl Future<Output = Result<()>> + Send;
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

    use crate::{BrokerMessage, MessageSource, ResultPublisher};

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

    impl<OutcomingMessage> MockResultPublisher<OutcomingMessage> {
        pub fn new() -> Self {
            Self {
                published: Arc::new(Mutex::new(vec![])),
            }
        }
    }

    impl<OutcomingMessage: Send> ResultPublisher<OutcomingMessage>
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
    use serde::{Deserialize, Serialize};

    use super::{BrokerMessage, MessageProcessor, MessageSource, ResultPublisher};

    #[derive(Clone, Serialize)]
    struct Input(u8);

    #[derive(Deserialize)]
    struct Output(u8);

    #[derive(Clone)]
    struct Increment;

    impl MessageProcessor<Input, Output> for Increment {
        fn process(&self, message: Input) -> Output {
            Output(message.0 + 1)
        }
    }

    #[test]
    fn defines_a_typed_message_transformation_contract() {
        assert_eq!(Increment.process(Input(41)).0, 42);
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

    struct BorrowingProcessor<'a>(&'a str);

    impl MessageProcessor<Input, Output> for BorrowingProcessor<'_> {
        fn process(&self, message: Input) -> Output {
            Output(message.0 + self.0.len() as u8)
        }
    }

    #[test]
    fn does_not_require_threading_or_ownership_bounds() {
        let processor = BorrowingProcessor("a reference");

        assert_eq!(processor.process(Input(1)).0, 12);
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
