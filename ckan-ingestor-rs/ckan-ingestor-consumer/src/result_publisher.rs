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

use iggy_processor::iggy::prelude::{IggyMessage, IggyProducer, Partitioning};
use message_processor::ResultPublisher;

use crate::messages::JobResultMessage;

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

impl ResultPublisher<JobResultMessage> for IggyResultPublisher {
    async fn publish(&self, result: JobResultMessage) -> Result<(), anyhow::Error> {
        let payload = serde_json::to_string(&result).map_err(|error| anyhow::anyhow!(error))?;
        let message = IggyMessage::from_str(&payload)?;
        let job_id = result.job_id.as_str();
        let partitioning = Arc::new(Partitioning::messages_key_str(&result.resource_id)?);
        self.producer
            .send_with_partitioning(vec![message], Some(partitioning))
            .await
            .map_err(|error| {
                anyhow::anyhow!(
                    "Iggy publish job={} status={}: {}",
                    job_id,
                    result.status,
                    error
                )
            })
    }
}
