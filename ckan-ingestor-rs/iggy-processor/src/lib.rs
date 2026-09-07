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
use message_processor::{BrokerMessage, MessageSource};

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
