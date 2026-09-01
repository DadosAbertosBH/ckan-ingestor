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
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMessage {
    pub payload: Vec<u8>,
    pub partition: u32,
    pub offset: u64,
}

pub trait MessageSource {
    fn recv(&self) -> impl Future<Output = Result<BrokerMessage>> + Send;

    fn commit(&self, partition: u32, offset: u64) -> impl Future<Output = Result<()>> + Send;
}

pub struct IggySource {
    consumer: Mutex<IggyConsumer>,
}

impl IggySource {
    pub fn new(consumer: IggyConsumer) -> Self {
        Self {
            consumer: Mutex::new(consumer),
        }
    }
}

impl MessageSource for IggySource {
    async fn recv(&self) -> Result<BrokerMessage> {
        let mut consumer = self.consumer.lock().await;
        let received = consumer
            .next()
            .await
            .ok_or_else(|| anyhow!("Iggy consumer stopped"))??;
        Ok(BrokerMessage {
            payload: received.message.payload.to_vec(),
            partition: received.partition_id,
            offset: received.message.header.offset,
        })
    }

    async fn commit(&self, partition: u32, offset: u64) -> Result<()> {
        let consumer = self.consumer.lock().await;
        consumer.store_offset(offset, Some(partition)).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;

    pub(crate) struct MockSource {
        rx: Mutex<mpsc::Receiver<BrokerMessage>>,
        pub committed: Arc<StdMutex<Vec<(u32, u64)>>>,
    }

    impl MockSource {
        pub fn new(rx: mpsc::Receiver<BrokerMessage>) -> Self {
            Self {
                rx: Mutex::new(rx),
                committed: Arc::new(StdMutex::new(vec![])),
            }
        }
    }

    impl MessageSource for MockSource {
        async fn recv(&self) -> Result<BrokerMessage> {
            self.rx
                .lock()
                .await
                .recv()
                .await
                .ok_or_else(|| anyhow!("mock source closed"))
        }

        async fn commit(&self, partition: u32, offset: u64) -> Result<()> {
            self.committed.lock().unwrap().push((partition, offset));
            Ok(())
        }
    }
}
