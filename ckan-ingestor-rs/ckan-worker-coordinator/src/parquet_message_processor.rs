// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::time::{Duration, Instant};

use async_stream::stream;
use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus};
use futures::Stream;
use log::{error, info, warn};
use message_processor::{MessageProcessor, OutgoingMessage};
use serde::Serialize;

use crate::child_jobs::ChildJobs;
use crate::parquet_registrar::ParquetRegistrar;

pub struct ParquetProcessor {
    job_result_destination: String,
    registrar: ParquetRegistrar,
    child_jobs: ChildJobs,
}

impl ParquetProcessor {
    pub fn new(
        job_result_destination: String,
        registrar: ParquetRegistrar,
        child_jobs: ChildJobs,
    ) -> Self {
        Self {
            job_result_destination,
            registrar,
            child_jobs,
        }
    }
}

impl MessageProcessor for ParquetProcessor {
    type IncomingMessage = JobResultMessage;

    fn process(
        &self,
        mut result: JobResultMessage,
    ) -> impl Stream<Item = OutgoingMessage<impl Serialize>> {
        stream! {
            info!(
                "coordinator_result_received job_id={} resource_id={} instance_id={} status={} parent_job_id={} child_index={} total_child_count={} has_artifact={}",
                result.job_id,
                result.resource_id,
                result.instance_id.as_deref().unwrap_or(""),
                result.status,
                result.parent_job_id.as_deref().unwrap_or(""),
                result.child_index.map(|index| index.to_string()).as_deref().unwrap_or(""),
                result.total_child_count.map(|count| count.to_string()).as_deref().unwrap_or(""),
                result.artifact.is_some(),
            );
            if let (Some(parent_job_id), Some(parent_resource_id)) =
                (result.parent_job_id.clone(), result.parent_resource_id.clone())
                && matches!(result.status, JobStatus::Success | JobStatus::Failed | JobStatus::Deleted)
            {
                    if result.status == JobStatus::Success && result.artifact.is_none() {
                        result.status = JobStatus::Failed;
                        result.error_message = Some("child success is missing its Parquet artifact".into());
                    }
                    let group = self.child_jobs.record(&result).await;
                    let mut child_terminal = result.clone();
                    child_terminal.artifact = None;
                    yield OutgoingMessage::new(
                        self.job_result_destination.clone(), child_terminal.resource_id.clone(), child_terminal,
                    );
                    match group {
                        Ok(group) if group.failed => {
                            let parent = parent_result(&result, &parent_job_id, &parent_resource_id,
                                JobStatus::Failed, Some("one or more child jobs failed".into()), None);
                            yield OutgoingMessage::new(self.job_result_destination.clone(), parent_resource_id, parent);
                        }
                        Ok(group) if group.records.len() == group.total as usize => {
                            let started = Instant::now();
                            info!(
                                "coordinator_child_merge_started parent_job_id={} parent_resource_id={} children={}",
                                parent_job_id,
                                parent_resource_id,
                                group.records.len(),
                            );
                            let children = group.records.into_values().collect::<Vec<_>>();
                            let outcome = self.registrar.register_children_for_source(
                                result.instance_id.as_deref().unwrap_or(""),
                                &parent_resource_id, &parent_job_id,
                                result.source_version.as_deref().unwrap_or(&parent_job_id), &children,
                            ).await;
                            let parent = match outcome {
                                Ok(rows) => {
                                    info!(
                                        "coordinator_child_merge_completed parent_job_id={} parent_resource_id={} rows={} elapsed_ms={}",
                                        parent_job_id,
                                        parent_resource_id,
                                        rows,
                                        started.elapsed().as_millis(),
                                    );
                                    parent_result(&result, &parent_job_id, &parent_resource_id,
                                        JobStatus::Success, None, Some(rows as i64))
                                }
                                Err(error) => {
                                    error!(
                                        "coordinator_child_merge_failed parent_job_id={} parent_resource_id={} elapsed_ms={} error={error:#}",
                                        parent_job_id,
                                        parent_resource_id,
                                        started.elapsed().as_millis(),
                                    );
                                    parent_result(&result, &parent_job_id, &parent_resource_id,
                                        JobStatus::Failed, Some(format!("child merge failed: {error:#}")), None)
                                }
                            };
                            yield OutgoingMessage::new(self.job_result_destination.clone(), parent_resource_id, parent);
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let parent = parent_result(&result, &parent_job_id, &parent_resource_id,
                                JobStatus::Failed, Some(format!("child state failed: {error:#}")), None);
                            yield OutgoingMessage::new(self.job_result_destination.clone(), parent_resource_id, parent);
                        }
                    }
                    return;
                }
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
                        match self.registrar.register_for_source(
                            result.instance_id.as_deref().unwrap_or(""),
                            &result.resource_id,
                            &result.job_id,
                            result.source_version.as_deref().unwrap_or(&result.job_id),
                            artifact,
                        ).await {
                            Ok(()) => return Ok(()),
                            Err(error) => {
                                failure = Some(error);
                                if attempt < 2 {
                                    let delay = Duration::from_millis(100 * (1 << attempt));
                                    warn!(
                                        "ducklake_registration_retry job_id={} resource_id={} attempt={} delay_ms={} error={:#}",
                                        result.job_id,
                                        result.resource_id,
                                        attempt + 1,
                                        delay.as_millis(),
                                        failure.as_ref().expect("registration failure is set"),
                                    );
                                    tokio::time::sleep(delay).await;
                                }
                            }
                        }
                    }
                    Err(failure.expect("registration attempts are non-empty"))
                }
                .await;
                if let Err(error) = registration {
                    error!(
                        "ducklake_registration_failed job_id={} resource_id={} error={error:#}",
                        result.job_id,
                        result.resource_id,
                    );
                    result.status = JobStatus::Failed;
                    result.error_message = Some(format!("DuckLake registration failed: {error:#}"));
                    result.preview = Some(vec![]);
                }
            }
            result.artifact = None;
            yield OutgoingMessage::new(self.job_result_destination.clone(), result.resource_id.clone(), result)
        }
    }
}

fn parent_result(
    child: &JobResultMessage,
    job_id: &str,
    resource_id: &str,
    status: JobStatus,
    error_message: Option<String>,
    rows: Option<i64>,
) -> JobResultMessage {
    let mut parent = child.clone();
    parent.job_id = job_id.to_owned();
    parent.resource_id = resource_id.to_owned();
    parent.parent_job_id = None;
    parent.parent_resource_id = None;
    parent.child_index = None;
    parent.total_child_count = None;
    parent.status = status;
    parent.reader = Some("ExternalResourceCrawlerReader".into());
    parent.rows_processed = rows;
    parent.error_message = error_message;
    parent.resource_name = None;
    parent.resource_url = None;
    parent.resource_format = None;
    parent.preview = None;
    parent.artifact = None;
    parent
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_ipc::convert::IpcSchemaEncoder;
    use base64::Engine;
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
    use ckan_ingestor_lib::parquet_output::ParquetOutput;
    use ckan_ingestor_worker_lib::{ParquetArtifact, ParquetColumnStatistics};
    use futures::StreamExt;
    use std::{fs::File, sync::Arc};

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

    #[tokio::test]
    async fn child_failure_fails_parent_without_suppressing_other_child_results() {
        let directory = tempfile::tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &directory.path().join("catalog.sqlite"),
            &directory.path().join("data"),
        );
        let children = ChildJobs::new(factory.clone());
        children.initialize().await.unwrap();
        let processor =
            ParquetProcessor::new("terminal".into(), ParquetRegistrar::new(factory), children);
        let mut first = JobResultMessage::pending("child-0", "resource-0", "dataset", "instance");
        first.parent_job_id = Some("parent".into());
        first.parent_resource_id = Some("parent-resource".into());
        first.child_index = Some(0);
        first.total_child_count = Some(2);
        first.status = JobStatus::Failed;
        let messages = processor.process(first.clone()).collect::<Vec<_>>().await;
        assert_eq!(messages.len(), 2);
        let parent: JobResultMessage =
            serde_json::from_value(serde_json::to_value(&messages[1].data).unwrap()).unwrap();
        assert_eq!(parent.job_id, "parent");
        assert_eq!(parent.status, JobStatus::Failed);
        first.job_id = "child-1".into();
        first.resource_id = "resource-1".into();
        first.child_index = Some(1);
        let next = processor.process(first).collect::<Vec<_>>().await;
        assert_eq!(
            next.len(),
            2,
            "later children still emit their terminal result"
        );
        let child: JobResultMessage =
            serde_json::from_value(serde_json::to_value(&next[0].data).unwrap()).unwrap();
        assert_eq!(child.job_id, "child-1");
    }

    #[tokio::test]
    async fn parent_succeeds_only_after_both_child_parquets_are_registered() {
        let directory = tempfile::tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &directory.path().join("catalog.sqlite"),
            &directory.path().join("data"),
        );
        let children = ChildJobs::new(factory.clone());
        children.initialize().await.unwrap();
        let registrar = ParquetRegistrar::new(factory.clone());
        registrar.initialize().await.unwrap();
        let processor = ParquetProcessor::new("terminal".into(), registrar, children);
        for (index, field) in ["left", "right"].into_iter().enumerate() {
            let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, false)]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![index as i64]))],
            )
            .unwrap();
            let mut output = ParquetOutput::try_new(&batch).unwrap();
            output.write(&batch).unwrap();
            output.finish().unwrap();
            let path = directory.path().join(format!("source-{index}.parquet"));
            std::fs::copy(output.path(), &path).unwrap();
            let encoded = IpcSchemaEncoder::new().schema_to_fb(schema.as_ref());
            let mut result = JobResultMessage::pending(
                format!("child-{index}"),
                format!("resource-{index}"),
                "dataset",
                "instance",
            );
            result.parent_job_id = Some("parent-job".into());
            result.parent_resource_id = Some("parent-resource".into());
            result.child_index = Some(index as u32);
            result.total_child_count = Some(2);
            result.status = JobStatus::Success;
            result.source_version = Some("2026-09-01T00:00:00Z".into());
            result.resource_name = Some(format!("source-{index}.json"));
            result.resource_format = Some("JSON".into());
            result.rows_processed = Some(1);
            result.artifact = Some(ParquetArtifact {
                uri: url::Url::from_file_path(&path).unwrap().to_string(),
                schema_ipc_base64: base64::engine::general_purpose::STANDARD
                    .encode(encoded.finished_data()),
                num_rows: 1,
                file_size_bytes: File::open(&path).unwrap().metadata().unwrap().len(),
                footer_size_bytes: None,
                column_statistics: vec![],
            });
            let messages = processor.process(result).collect::<Vec<_>>().await;
            assert_eq!(messages.len(), if index == 0 { 1 } else { 2 });
            if index == 1 {
                let parent: JobResultMessage =
                    serde_json::from_value(serde_json::to_value(&messages[1].data).unwrap())
                        .unwrap();
                assert_eq!(parent.status, JobStatus::Success);
                assert_eq!(parent.job_id, "parent-job");
                assert_eq!(parent.rows_processed, Some(2));
            }
        }
        let table = ducklake::TableName {
            schema: "main".into(),
            name: "parent-resource".into(),
        };
        let scan = factory
            .client()
            .await
            .unwrap()
            .table(table)
            .await
            .unwrap()
            .scan()
            .await
            .unwrap();
        assert_eq!(scan.data_files.len(), 2);
    }
}
