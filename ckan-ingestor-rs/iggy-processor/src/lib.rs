// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::{str::FromStr, sync::Arc};

use anyhow::{Result, anyhow};
use futures::StreamExt;
use iggy::{
    clients::producer::IggyProducer,
    prelude::{IggyConsumer, IggyMessage, Partitioning},
};
use message_processor::{BrokerMessage, MessageSource, OutgoingMessage, ResultPublisher};

pub use iggy;

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

#[derive(Clone)]
pub struct IggyResultPublisher {
    producer: Arc<IggyProducer>,
}

impl IggyResultPublisher {
    pub fn new(producer: IggyProducer) -> Self {
        Self {
            producer: Arc::new(producer),
        }
    }
}

impl<T> ResultPublisher<T> for IggyResultPublisher
where
    T: serde::Serialize,
    T: OutgoingMessage + Send,
{
    async fn publish(&self, message: T) -> Result<(), anyhow::Error> {
        let payload = serde_json::to_string(&message).map_err(|error| anyhow::anyhow!(error))?;
        let iggy_message = IggyMessage::from_str(&payload)?;
        let partitioning = Arc::new(Partitioning::messages_key_str(message.partition_key())?);
        self.producer
            .as_ref()
            .send_with_partitioning(vec![iggy_message], Some(partitioning))
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Failed Iggy publish message {} with error {}",
                    payload,
                    error
                )
            })
    }
}
