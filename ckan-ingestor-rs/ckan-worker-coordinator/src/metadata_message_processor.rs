// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use async_stream::stream;
use ckan_ingestor_worker_lib::{JobDiscoveryMetadata, JobMessage, JobResultMessage};
use ckan_metadata_ingestor::{MetadataSyncCommand, MetadataSyncResult};
use futures::Stream;
use message_processor::{MessageProcessor, OutgoingMessage};
use serde::Serialize;
use uuid::Uuid;

use crate::metadata_processor::{RealMetadataProcessor, ResourceCandidate};

#[derive(Serialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum OutgoingDestination {
    Job(JobMessage),
    JobRetry(JobMessage),
    JobResult(JobResultMessage),
    SyncResult(MetadataSyncResult),
}

pub struct MetadataProcessor {
    job_destination: String,
    retry_destination: String,
    job_result_destination: String,
    sync_destination: String,
    processor: RealMetadataProcessor,
}

impl MetadataProcessor {
    pub fn new(
        job_destination: String,
        retry_destination: String,
        job_result_destination: String,
        sync_destination: String,
        processor: RealMetadataProcessor,
    ) -> Self {
        Self {
            job_destination,
            retry_destination,
            job_result_destination,
            sync_destination,
            processor,
        }
    }

    fn outgoing_message_from(
        &self,
        destination: OutgoingDestination,
    ) -> OutgoingMessage<OutgoingDestination> {
        match &destination {
            OutgoingDestination::Job(job) => OutgoingMessage {
                topic: self.job_destination.clone(),
                partition_key: job.resource_id.clone(),
                data: destination,
            },
            OutgoingDestination::JobRetry(job) => OutgoingMessage {
                topic: self.retry_destination.clone(),
                partition_key: job.resource_id.clone(),
                data: destination,
            },
            OutgoingDestination::JobResult(job_result_message) => OutgoingMessage {
                topic: self.job_result_destination.clone(),
                partition_key: job_result_message.resource_id.clone(),
                data: destination,
            },
            OutgoingDestination::SyncResult(sync) => OutgoingMessage {
                topic: self.sync_destination.clone(),
                partition_key: sync.instance_id.clone(),
                data: destination,
            },
        }
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
    fn process(
        &self,
        command: MetadataSyncCommand,
    ) -> impl Stream<Item = OutgoingMessage<impl Serialize>> {
        stream! {
            let processed = self.processor.process(command.clone()).await;
            let deleted_resource_candidates = processed.deleted_resource_candidates;
            let result = processed.result;
            if result.status == "success" {
                for candidate in &deleted_resource_candidates {
                    let id = Uuid::new_v5(
                        &Uuid::NAMESPACE_URL,
                        format!(
                            "{}:{}:deleted:{}",
                            command.instance_id, candidate.resource_id, candidate.source_version
                        )
                        .as_bytes(),
                    )
                    .to_string();
                    let metadata = JobDiscoveryMetadata {
                        resource_name: candidate.resource_name.clone(),
                        resource_url: candidate.resource_url.clone(),
                        resource_format: candidate.resource_format.clone(),
                        ckan_url: Some(command.instance_url.clone()),
                        datastore_active: Some(candidate.datastore_active),
                    };
                    let pending = JobResultMessage::pending_with_metadata(
                        id.clone(),
                        candidate.resource_id.clone(),
                        candidate.dataset_name.clone(),
                        command.instance_id.clone(),
                        metadata.clone(),
                    );
                    let deleted = JobResultMessage::deleted(
                        id.clone(),
                        candidate.resource_id.clone(),
                        candidate.dataset_name.clone(),
                        command.instance_id.clone(),
                        metadata,
                    );
                    yield self.outgoing_message_from(OutgoingDestination::JobResult(pending));
                    yield self.outgoing_message_from(OutgoingDestination::JobResult(deleted));
                }
                let candidates = match self.processor.outdated_resources(&command.instance_url).await {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        log::error!("could not determine outdated resources: {error:#}");
                        yield self.outgoing_message_from(OutgoingDestination::SyncResult(result));
                        return;
                    }
                };
                for candidate in candidates {
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

                    yield self.outgoing_message_from(OutgoingDestination::JobResult(pending));

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

                    yield self.outgoing_message_from(OutgoingDestination::Job(job));
                }
                yield self.outgoing_message_from(OutgoingDestination::SyncResult(result));
            }
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
