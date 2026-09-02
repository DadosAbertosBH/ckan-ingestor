// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Result, anyhow};
use futures::StreamExt;
use iggy::prelude::IggyConsumer;
use std::future::Future;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMessage {
    pub payload: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

pub trait MessageSource {
    fn recv(&mut self) -> impl Future<Output = Result<BrokerMessage>> + Send;

    fn commit(&mut self, partition: u32, offset: u64) -> impl Future<Output = Result<()>> + Send;
}

pub struct IggySource {
    consumer: IggyConsumer,
}

impl IggySource {
    pub fn new(consumer: IggyConsumer) -> Self {
        Self { consumer }
    }
}

impl MessageSource for IggySource {
    async fn recv(&mut self) -> Result<BrokerMessage> {
        let received = self
            .consumer
            .next()
            .await
            .ok_or_else(|| anyhow!("Iggy consumer stopped"))??;
        Ok(BrokerMessage {
            payload: received.message.payload.to_vec(),
            partition: received.partition_id,
            offset: received.message.header.offset,
        })
    }

    async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
        self.consumer.store_offset(offset, Some(partition)).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;

    pub(crate) struct MockSource {
        rx: mpsc::Receiver<BrokerMessage>,
        pub committed: Arc<StdMutex<Vec<(u32, u64)>>>,
    }

    impl MockSource {
        pub fn new(rx: mpsc::Receiver<BrokerMessage>) -> Self {
            Self {
                rx,
                committed: Arc::new(StdMutex::new(vec![])),
            }
        }
    }

    impl MessageSource for MockSource {
        async fn recv(&mut self) -> Result<BrokerMessage> {
            self.rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("mock source closed"))
        }

        async fn commit(&mut self, partition: u32, offset: u64) -> Result<()> {
            self.committed.lock().unwrap().push((partition, offset));
            Ok(())
        }
    }
}
