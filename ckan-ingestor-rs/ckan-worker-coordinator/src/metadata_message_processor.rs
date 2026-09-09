// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use async_stream::stream;
use ckan_ingestor_worker_lib::{JobMessage, JobResultMessage};
use ckan_metadata_ingestor::MetadataSyncCommand;
use message_processor::{MessageProcessor, OutgoingMessage};
use serde::Serialize;
use uuid::Uuid;

use crate::job_publisher::JobPublisher;
use crate::metadata_processor::{RealMetadataProcessor, ResourceCandidate};

pub struct MetadataProcessor {
    jobs: JobPublisher,
    processor: RealMetadataProcessor,
}

impl MetadataProcessor {
    pub fn new(jobs: JobPublisher, processor: RealMetadataProcessor) -> Self {
        Self { jobs, processor }
    }
}

#[derive(Serialize)]
#[serde(transparent)]
pub struct MetadataResult(pub ckan_metadata_ingestor::MetadataSyncResult);

impl OutgoingMessage for MetadataResult {
    fn partition_key(&self) -> &str {
        &self.0.sync_id
    }
}

fn job_id(instance_id: &str, candidate: &ResourceCandidate) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "{instance_id}:{}:{}",
            candidate.resource_id, candidate.source_version
        )
        .as_bytes(),
    )
    .to_string()
}

impl MessageProcessor for MetadataProcessor {
    type IncomingMessage = MetadataSyncCommand;
    type OutgoingMessage = MetadataResult;

    fn process(&self, command: MetadataSyncCommand) -> impl futures::Stream<Item = MetadataResult> {
        stream! {
        let mut result = self.processor.process(command.clone()).await;
        if result.status == "success" {
            let candidates = match self.processor.outdated_resources(&command.instance_url).await {
                Ok(candidates) => candidates,
                Err(error) => {
                    log::error!("could not determine outdated resources: {error:#}");
                    yield MetadataResult(result);
                    return;
                }
            };
            let messages = candidates.into_iter().map(|candidate| {
                let id = job_id(&command.instance_id, &candidate);
                let mut pending = JobResultMessage::pending(
                    id.clone(),
                    candidate.resource_id.clone(),
                    candidate.dataset_name.clone(),
                    command.instance_id.clone(),
                );
                pending.resource_name = candidate.resource_name.clone();
                pending.resource_url = candidate.resource_url.clone();
                pending.resource_format = candidate.resource_format.clone();
                pending.ckan_url = Some(command.instance_url.clone());
                pending.datastore_active = Some(candidate.datastore_active);
                let job = JobMessage {
                    job_id: id,
                    resource_id: candidate.resource_id,
                    source_version: candidate.source_version,
                    package_id: candidate.package_id,
                    ckan_url: command.instance_url.clone(),
                    resource_url: candidate.resource_url.unwrap_or_default(),
                    resource_format: candidate.resource_format.unwrap_or_default(),
                    csv_delimiter: None,
                    datastore_active: candidate.datastore_active,
                };
                (pending, job)
            }).collect::<Vec<_>>();
            if let Err(error) = self.jobs.pending_batch(&messages).await {
                log::error!("could not publish metadata jobs: {error:#}");
                result.status = "failure".into();
                result.error_message = Some(format!("could not publish metadata jobs: {error:#}"));
            }
        }
        yield MetadataResult(result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_processor::ResourceCandidate;

    fn candidate(resource_id: &str, version: &str) -> ResourceCandidate {
        ResourceCandidate {
            resource_id: resource_id.into(),
            package_id: "package".into(),
            resource_name: None,
            resource_url: None,
            resource_format: None,
            dataset_name: "dataset".into(),
            datastore_active: false,
            source_version: version.into(),
        }
    }

    #[test]
    fn job_id_is_stable_for_the_same_resource_version() {
        assert_eq!(
            job_id("instance", &candidate("resource", "v1")),
            job_id("instance", &candidate("resource", "v1"))
        );
        assert_ne!(
            job_id("instance", &candidate("resource", "v1")),
            job_id("instance", &candidate("resource", "v2"))
        );
    }
}
