// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use async_stream::stream;
use ckan_ingestor_worker_lib::{JobMessage, JobResultMessage, JobStatus};
use ckan_metadata_ingestor::MetadataSyncCommand;
use message_processor::{MessageProcessor, OutgoingMessage};
use serde::Serialize;
use uuid::Uuid;

use crate::job_planner::{JobPlan, JobPlanner};
use crate::job_publisher::JobPublisher;
use crate::metadata_processor::RealMetadataProcessor;

pub struct MetadataProcessor {
    jobs: JobPublisher,
    planner: JobPlanner,
    processor: RealMetadataProcessor,
}

impl MetadataProcessor {
    pub fn new(jobs: JobPublisher, planner: JobPlanner, processor: RealMetadataProcessor) -> Self {
        Self {
            jobs,
            planner,
            processor,
        }
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

impl MessageProcessor for MetadataProcessor {
    type IncomingMessage = MetadataSyncCommand;
    type OutgoingMessage = MetadataResult;

    fn process(&self, command: MetadataSyncCommand) -> impl futures::Stream<Item = MetadataResult> {
        stream! {
        let result = self.processor.process(command.clone()).await;
        if result.status == "success" {
            let candidates = match self.processor.outdated_resources(&command.instance_url).await {
                Ok(candidates) => candidates,
                Err(error) => {
                    log::error!("could not determine outdated resources: {error:#}");
                    yield MetadataResult(result);
                    return;
                }
            };
            let plans = match self.planner.classify(candidates) {
                Ok(plans) => plans,
                Err(error) => {
                    log::error!("could not plan metadata jobs: {error:#}");
                    yield MetadataResult(result);
                    return;
                }
            };
            for JobPlan {
                candidate,
                enqueue,
                retry,
            } in plans {
                if enqueue {
                    let id = Uuid::new_v4().to_string();
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
                        ckan_url: command.instance_url.clone(),
                        resource_url: candidate.resource_url.unwrap_or_default(),
                        resource_format: candidate.resource_format.unwrap_or_default(),
                        csv_delimiter: None,
                        datastore_active: candidate.datastore_active,
                    };
                    if let Err(error) = self.jobs.pending(&pending, &job, retry).await {
                        log::error!("could not publish planned job: {error:#}");
                    }
                } else {
                    let skipped = JobResultMessage {
                        // The backend treats an empty id as a metadata-only result.
                        job_id: String::new(),
                        status: JobStatus::Success,
                        resource_id: candidate.resource_id.clone(),
                        dataset_name: Some(candidate.dataset_name),
                        resource_name: candidate.resource_name,
                        resource_url: candidate.resource_url,
                        resource_format: candidate.resource_format,
                        instance_id: Some(command.instance_id.clone()),
                        ckan_url: Some(command.instance_url.clone()),
                        datastore_active: Some(candidate.datastore_active),
                        reader: None,
                        rows_processed: None,
                        expected_rows: None,
                        encoding: None,
                        csv_strict_mode: None,
                        csv_delimiter: None,
                        expected_columns: None,
                        error_message: None,
                        preview: None,
                        artifact: None,
                    };
                    if let Err(error) = self.jobs.skipped(&skipped, &candidate.resource_id).await {
                        log::error!("could not publish skipped job: {error:#}");
                    }
                }
            }
        }
        yield MetadataResult(result);
        }
    }
}
