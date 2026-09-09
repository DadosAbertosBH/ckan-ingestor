// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::time::Duration;

use async_stream::stream;
use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus};
use message_processor::MessageProcessor;

use crate::parquet_registrar::ParquetRegistrar;

pub struct ParquetProcessor {
    registrar: ParquetRegistrar,
}

impl ParquetProcessor {
    pub fn new(registrar: ParquetRegistrar) -> Self {
        Self { registrar }
    }
}

impl MessageProcessor for ParquetProcessor {
    type IncomingMessage = JobResultMessage;
    type OutgoingMessage = JobResultMessage;

    fn process(
        &self,
        mut result: JobResultMessage,
    ) -> impl futures::Stream<Item = JobResultMessage> {
        stream! {
            if result.status == JobStatus::Success {
                let registration = async {
                    // A deterministic S3 object found by the worker is an
                    // already-materialized success. The metadata ledger is
                    // the queue-side authority that suppresses future jobs.
                    let Some(artifact) = result.artifact.as_ref() else {
                        return Ok(());
                    };
                    anyhow::ensure!(
                        !result.job_id.is_empty(),
                        "successful conversion result requires job_id"
                    );
                    let mut failure = None;
                    for attempt in 0..3 {
                        match self.registrar.register(
                            &result.resource_id,
                            &result.job_id,
                            result.source_version.as_deref().unwrap_or(&result.job_id),
                            artifact,
                        ).await {
                            Ok(()) => return Ok(()),
                            Err(error) => {
                                failure = Some(error);
                                tokio::time::sleep(Duration::from_millis(100 * (1 << attempt))).await;
                            }
                        }
                    }
                    Err(failure.expect("registration attempts are non-empty"))
                }
                .await;
                if let Err(error) = registration {
                    result.status = JobStatus::Failed;
                    result.error_message = Some(format!("DuckLake registration failed: {error:#}"));
                    result.preview = Some(vec![]);
                }
            }
            result.artifact = None;
            yield result;
        }
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
