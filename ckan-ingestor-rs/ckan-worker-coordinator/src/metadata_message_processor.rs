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

fn jobs_to_publish(plans: Vec<JobPlan>) -> impl Iterator<Item = JobPlan> {
    plans.into_iter().filter(|plan| plan.enqueue)
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
                retry,
                ..
            } in jobs_to_publish(plans) {
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
                    package_id: candidate.package_id,
                    ckan_url: command.instance_url.clone(),
                    resource_url: candidate.resource_url.unwrap_or_default(),
                    resource_format: candidate.resource_format.unwrap_or_default(),
                    csv_delimiter: None,
                    datastore_active: candidate.datastore_active,
                };
                if let Err(error) = self.jobs.pending(&pending, &job, retry).await {
                    log::error!("could not publish planned job: {error:#}");
                }
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

    fn plan(resource_id: &str, enqueue: bool) -> JobPlan {
        JobPlan {
            candidate: ResourceCandidate {
                resource_id: resource_id.into(),
                package_id: "package".into(),
                resource_name: None,
                resource_url: None,
                resource_format: None,
                dataset_name: "dataset".into(),
                datastore_active: false,
            },
            enqueue,
            retry: false,
        }
    }

    #[test]
    fn only_returns_plans_that_require_job_publication() {
        let plans = jobs_to_publish(vec![plan("queued", true), plan("known", false)])
            .map(|plan| plan.candidate.resource_id)
            .collect::<Vec<_>>();

        assert_eq!(plans, vec!["queued"]);
    }
}
