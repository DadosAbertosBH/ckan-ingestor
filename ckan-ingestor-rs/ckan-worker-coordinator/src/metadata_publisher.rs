// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::str::FromStr;
use std::sync::Arc;

use ckan_metadata_ingestor::MetadataSyncResult;
use iggy_processor::iggy::prelude::{IggyMessage, IggyProducer, Partitioning};

#[derive(Clone)]
pub struct MetadataPublisher {
    producer: Arc<IggyProducer>,
}

impl MetadataPublisher {
    pub fn new(producer: IggyProducer) -> Self {
        Self {
            producer: Arc::new(producer),
        }
    }

    pub async fn publish(&self, result: MetadataSyncResult) -> Result<(), anyhow::Error> {
        let payload = serde_json::to_string(&result)?;
        let message = IggyMessage::from_str(&payload)?;
        let partitioning = Arc::new(Partitioning::messages_key_str(&result.sync_id)?);
        self.producer
            .send_with_partitioning(vec![message], Some(partitioning))
            .await?;
        Ok(())
    }
}
