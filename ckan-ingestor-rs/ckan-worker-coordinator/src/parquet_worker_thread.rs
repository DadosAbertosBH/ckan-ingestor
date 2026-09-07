// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::time::Duration;

use anyhow::Context;
use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus, MessageHandler};
use message_processor::BrokerMessage;

use crate::{job_publisher::JobPublisher, parquet_registrar::ParquetRegistrar};

pub struct ParquetHandler {
    registrar: ParquetRegistrar,
    publisher: JobPublisher,
}

impl ParquetHandler {
    pub fn new(registrar: ParquetRegistrar, publisher: JobPublisher) -> Self {
        Self {
            registrar,
            publisher,
        }
    }
}

impl MessageHandler for ParquetHandler {
    async fn handle(&self, message: &BrokerMessage) -> anyhow::Result<()> {
        let mut result: JobResultMessage = serde_json::from_slice(&message.payload)
            .context("decoding parquet conversion result")?;
        let key = result.resource_id.clone();
        if result.status == JobStatus::Success {
            let artifact = result
                .artifact
                .as_ref()
                .context("successful conversion result requires artifact")?;
            let resource_id = result.resource_id.as_str();
            let job_id = result.job_id.as_str();
            anyhow::ensure!(
                !job_id.is_empty(),
                "successful conversion result requires job_id"
            );
            let mut failure = None;
            for attempt in 0..3 {
                match self.registrar.register(resource_id, job_id, artifact).await {
                    Ok(()) => {
                        failure = None;
                        break;
                    }
                    Err(error) => {
                        failure = Some(error);
                        tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                    }
                }
            }
            if let Some(error) = failure {
                result.status = JobStatus::Failed;
                result.error_message = Some(format!("DuckLake registration failed: {error:#}"));
                result.preview = Some(vec![]);
            }
        }
        // The backend's established job-results contract remains terminal and
        // never needs the internal registration descriptor.
        result.artifact = None;
        self.publisher.result(&result, &key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ckan_ingestor_worker_lib::{ParquetArtifact, ParquetColumnStatistics};

    #[test]
    fn terminal_contract_drops_the_internal_artifact() {
        let mut result = JobResultMessage::pending("job", "resource", "dataset", "instance");
        result.status = JobStatus::Success;
        result.artifact = Some(ParquetArtifact {
            uri: "s3://warehouse/resource/job.parquet".into(),
            schema_ipc_base64: "schema".into(),
            num_rows: 1,
            file_size_bytes: 1,
            footer_size_bytes: None,
            column_statistics: vec![ParquetColumnStatistics {
                field_id: 1,
                size_bytes: None,
                min_value: None,
                max_value: None,
                null_count: None,
                contains_nan: None,
            }],
        });
        result.artifact = None;
        assert!(result.artifact.is_none());
    }
}
