// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_worker_lib::{
    BrokerMessage, JobMessage, JobResultMessage, JobStatus, MessageHandler,
};
use ckan_metadata_ingestor::MetadataSyncCommand;
use uuid::Uuid;

use crate::job_planner::MysqlJobPlanner;
use crate::job_publisher::JobPublisher;
use crate::metadata_processor::RealMetadataProcessor;
use crate::metadata_publisher::MetadataPublisher;

pub struct MetadataHandler {
    publisher: MetadataPublisher,
    jobs: JobPublisher,
    planner: MysqlJobPlanner,
    processor: RealMetadataProcessor,
}

impl MetadataHandler {
    pub fn new(
        publisher: MetadataPublisher,
        jobs: JobPublisher,
        planner: MysqlJobPlanner,
        processor: RealMetadataProcessor,
    ) -> Self {
        Self {
            publisher,
            jobs,
            planner,
            processor,
        }
    }
}

impl MessageHandler for MetadataHandler {
    async fn handle(&self, message: &BrokerMessage) -> anyhow::Result<()> {
        let command = serde_json::from_slice::<MetadataSyncCommand>(&message.payload)?;
        let result = self.processor.process(command.clone());
        if result.status == "success" {
            for (candidate, enqueue, retry) in self
                .planner
                .classify(self.processor.outdated_resources(&command.instance_url)?)?
            {
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
                    self.jobs.pending(&pending, &job, retry).await?;
                } else {
                    let skipped = JobResultMessage {
                        job_id: None,
                        status: JobStatus::Success,
                        resource_id: Some(candidate.resource_id.clone()),
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
                    };
                    self.jobs.skipped(&skipped, &candidate.resource_id).await?;
                }
            }
        }
        self.publisher.publish(result).await
    }
}
