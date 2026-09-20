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
use ckan_metadata_ingestor::{MetadataSyncCommand, MetadataSyncResult, MetadataSyncStatus};
use futures::Stream;
use message_processor::{MessageProcessor, OutgoingMessage};
use serde::Serialize;
use uuid::Uuid;

use crate::metadata_sync_processor::{DeletedResource, MetadataSyncProcessor, ResourceCandidate};

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
    processor: MetadataSyncProcessor,
}

impl MetadataProcessor {
    pub fn new(
        job_destination: String,
        retry_destination: String,
        job_result_destination: String,
        sync_destination: String,
        processor: MetadataSyncProcessor,
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

fn deleted_job_results(
    command: &MetadataSyncCommand,
    candidate: &DeletedResource,
) -> (JobResultMessage, JobResultMessage) {
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
        id,
        candidate.resource_id.clone(),
        candidate.dataset_name.clone(),
        command.instance_id.clone(),
        metadata,
    );

    (pending, deleted)
}

fn outdated_resource_messages(
    command: &MetadataSyncCommand,
    candidate: ResourceCandidate,
) -> (JobResultMessage, JobMessage) {
    let id = job_id(&command.instance_id, &candidate);
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
        metadata,
    );
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
}

impl MessageProcessor for MetadataProcessor {
    type IncomingMessage = MetadataSyncCommand;
    fn process(
        &self,
        command: MetadataSyncCommand,
    ) -> impl Stream<Item = OutgoingMessage<impl Serialize>> {
        stream! {
            let mut processed = self.processor.process(command.clone()).await;
            if processed.result.status != MetadataSyncStatus::Success {
                yield self.outgoing_message_from(OutgoingDestination::SyncResult(processed.result));
                return
            }

            for candidate in &processed.deleted_resource_candidates {
                let (pending, deleted) = deleted_job_results(&command, candidate);
                yield self.outgoing_message_from(OutgoingDestination::JobResult(pending));
                yield self.outgoing_message_from(OutgoingDestination::JobResult(deleted));
            }

            let candidates = match self.processor.outdated_resources(&command.instance_url).await {
                Ok(candidates) => candidates,
                Err(error) => {
                    log::error!("could not determine outdated resources: {error:#}");
                    yield self.outgoing_message_from(OutgoingDestination::SyncResult(processed.result));
                    return;
                }
            };
            processed.result.outdated_resources = candidates.len() as i64;
            for candidate in candidates {
                let (pending, job) = outdated_resource_messages(&command, candidate);
                yield self.outgoing_message_from(OutgoingDestination::JobResult(pending));
                yield self.outgoing_message_from(OutgoingDestination::Job(job));
            }

            yield self.outgoing_message_from(OutgoingDestination::SyncResult(processed.result));
        }
    }
}

#[cfg(test)]
mod tests {
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
    use ckan_ingestor_worker_lib::JobStatus;
    use futures::StreamExt;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::ducklake_data_writer::DucklakeDataWriter;
    use crate::metadata_sync_processor::ResourceCandidate;

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

    #[test]
    fn deleted_job_results_share_an_id_and_preserve_discovery_metadata() {
        let command = MetadataSyncCommand {
            sync_id: "sync-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Test CKAN".into(),
            instance_url: "https://ckan.example".into(),
        };
        let candidate = DeletedResource {
            resource_id: "resource-1".into(),
            package_id: "package-1".into(),
            dataset_name: "Dataset".into(),
            resource_name: Some("Resource".into()),
            resource_url: Some("https://ckan.example/resource.csv".into()),
            resource_format: Some("CSV".into()),
            datastore_active: true,
            source_version: "2026-09-17T00:00:00Z".into(),
        };

        let (pending, deleted) = deleted_job_results(&command, &candidate);

        assert_eq!(pending.status, JobStatus::Pending);
        assert_eq!(deleted.status, JobStatus::Deleted);
        assert_eq!(pending.job_id, deleted.job_id);
        assert_eq!(pending.resource_id, candidate.resource_id);
        assert_eq!(pending.dataset_name, Some(candidate.dataset_name));
        assert_eq!(pending.resource_name, candidate.resource_name);
        assert_eq!(pending.resource_url, candidate.resource_url);
        assert_eq!(pending.resource_format, candidate.resource_format);
        assert_eq!(pending.instance_id, Some(command.instance_id));
        assert_eq!(pending.ckan_url, Some(command.instance_url));
        assert_eq!(pending.datastore_active, Some(candidate.datastore_active));
    }

    #[test]
    fn outdated_resource_messages_share_an_id_and_preserve_discovery_metadata() {
        let command = MetadataSyncCommand {
            sync_id: "sync-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Test CKAN".into(),
            instance_url: "https://ckan.example".into(),
        };
        let candidate = ResourceCandidate {
            resource_id: "resource-1".into(),
            package_id: "package-1".into(),
            dataset_name: "Dataset".into(),
            resource_name: Some("Resource".into()),
            resource_url: Some("https://ckan.example/resource.csv".into()),
            resource_format: Some("CSV".into()),
            datastore_active: true,
            source_version: "2026-09-17T00:00:00Z".into(),
        };

        let (pending, job) = outdated_resource_messages(&command, candidate);

        assert_eq!(pending.status, JobStatus::Pending);
        assert_eq!(pending.job_id, job.job_id);
        assert_eq!(pending.resource_id, job.resource_id);
        assert_eq!(pending.dataset_name, Some("Dataset".into()));
        assert_eq!(pending.resource_name, Some("Resource".into()));
        assert_eq!(pending.resource_url, Some(job.resource_url.clone()));
        assert_eq!(pending.resource_format, Some(job.resource_format.clone()));
        assert_eq!(pending.instance_id, Some(command.instance_id));
        assert_eq!(pending.ckan_url, Some(job.ckan_url));
        assert_eq!(pending.datastore_active, Some(job.datastore_active));
    }

    #[tokio::test]
    async fn forwards_a_failed_sync_result_without_creating_jobs() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let writer = DucklakeDataWriter::new(
            factory.client().await.unwrap(),
            factory.storage_options().to_vec(),
        );
        let processor = MetadataProcessor::new(
            "jobs".into(),
            "retries".into(),
            "job-results".into(),
            "sync-results".into(),
            MetadataSyncProcessor::new(factory, writer),
        );
        let command = MetadataSyncCommand {
            sync_id: "sync-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Test CKAN".into(),
            instance_url: "not a valid URL".into(),
        };

        let messages = tokio::task::spawn_blocking(move || {
            futures::executor::block_on(
                processor
                    .process(command)
                    .map(|message| {
                        (
                            message.topic,
                            message.partition_key,
                            serde_json::to_value(&message.data).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, "sync-results");
        assert_eq!(messages[0].1, "instance-1");
        assert_eq!(messages[0].2["status"], json!("failure"));
        assert_eq!(messages[0].2["outdated_resources"], json!(0));
    }
}
