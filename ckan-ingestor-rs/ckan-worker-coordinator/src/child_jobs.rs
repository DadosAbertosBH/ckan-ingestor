// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::StringViewArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus, ParquetArtifact};
use ducklake::{Column, IfExistsStrategy};

const TABLE: &str = "child_jobs";

#[derive(Clone)]
pub struct ChildJobs {
    factory: DucklakeFactory,
}

#[derive(Clone)]
pub struct ChildRecord {
    pub index: u32,
    pub status: JobStatus,
    pub artifact: Option<ParquetArtifact>,
    pub rows: i64,
    pub source: String,
    pub format: String,
}

pub struct ChildGroup {
    pub records: BTreeMap<u32, ChildRecord>,
    pub total: u32,
    pub failed: bool,
}

impl ChildJobs {
    pub fn new(factory: DucklakeFactory) -> Self {
        Self { factory }
    }

    pub async fn initialize(&self) -> Result<()> {
        let client = self.factory.client().await?;
        if client.table_exists(TABLE).await? {
            return Ok(());
        }
        let columns = schema()
            .fields()
            .iter()
            .map(|field| Column::try_from(field.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut transaction = client.transaction().await?;
        transaction.create_table(TABLE, columns, None, None, None, IfExistsStrategy::Fail)?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn record(&self, result: &JobResultMessage) -> Result<ChildGroup> {
        let parent_id = result
            .parent_job_id
            .as_deref()
            .context("child requires parent_job_id")?;
        let parent_resource_id = result
            .parent_resource_id
            .as_deref()
            .context("child requires parent_resource_id")?;
        let index = result.child_index.context("child requires child_index")?;
        let total = result
            .total_child_count
            .context("child requires total_child_count")?;
        anyhow::ensure!(total > 1 && index < total, "invalid child index/count");
        anyhow::ensure!(
            matches!(
                result.status,
                JobStatus::Success | JobStatus::Failed | JobStatus::Deleted
            ),
            "child result is not terminal"
        );
        let client = self.factory.client().await?;
        let table = client.table(TABLE).await?;
        let artifact = result
            .artifact
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_default();
        let values = [
            parent_id.to_owned(),
            parent_resource_id.to_owned(),
            result.job_id.clone(),
            index.to_string(),
            total.to_string(),
            result.status.to_string(),
            artifact,
            result.rows_processed.unwrap_or_default().to_string(),
            result.resource_name.clone().unwrap_or_default(),
            result.source_version.clone().unwrap_or_default(),
            result.resource_format.clone().unwrap_or_default(),
        ];
        let batch = RecordBatch::try_new(
            schema(),
            values
                .iter()
                .map(|value| {
                    Arc::new(StringViewArray::from(vec![value.as_str()])) as arrow::array::ArrayRef
                })
                .collect(),
        )?;
        table.write_inline_data(vec![batch]).await?;
        self.group(parent_id).await
    }

    pub async fn group(&self, parent_id: &str) -> Result<ChildGroup> {
        let client = self.factory.client().await?;
        let scan = client.table(TABLE).await?.scan().await?;
        let mut records = BTreeMap::new();
        let mut total = 0;
        for batch in scan.inline_data {
            for row in 0..batch.num_rows() {
                if cell(&batch, "parent_job_id", row)? != parent_id {
                    continue;
                }
                let index: u32 = cell(&batch, "child_index", row)?.parse()?;
                total = cell(&batch, "total_child_count", row)?.parse()?;
                let status = match cell(&batch, "status", row)? {
                    "SUCCESS" => JobStatus::Success,
                    "FAILED" | "DELETED" => JobStatus::Failed,
                    value => anyhow::bail!("unexpected child status {value}"),
                };
                let artifact = match cell(&batch, "artifact", row)? {
                    "" => None,
                    value => Some(serde_json::from_str(value)?),
                };
                let record = ChildRecord {
                    index,
                    status,
                    artifact,
                    rows: cell(&batch, "rows_processed", row)?.parse()?,
                    source: cell(&batch, "source", row)?.to_owned(),
                    format: cell(&batch, "format", row)?.to_owned(),
                };
                records
                    .entry(index)
                    .and_modify(|previous: &mut ChildRecord| {
                        if previous.status != JobStatus::Failed {
                            *previous = record.clone();
                        }
                    })
                    .or_insert(record);
            }
        }
        let failed = records
            .values()
            .any(|record| record.status == JobStatus::Failed);
        Ok(ChildGroup {
            records,
            total,
            failed,
        })
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(
        [
            "parent_job_id",
            "parent_resource_id",
            "child_job_id",
            "child_index",
            "total_child_count",
            "status",
            "artifact",
            "rows_processed",
            "source",
            "source_version",
            "format",
        ]
        .map(|name| Field::new(name, DataType::Utf8View, false))
        .to_vec(),
    ))
}

fn cell<'a>(batch: &'a RecordBatch, name: &str, row: usize) -> Result<&'a str> {
    Ok(batch
        .column(batch.schema().index_of(name)?)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .context("child_jobs column is not text")?
        .value(row))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn inline_rows_survive_reopening_and_deduplicate_child_index() {
        let directory = tempfile::tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &directory.path().join("catalog.sqlite"),
            &directory.path().join("data"),
        );
        let jobs = ChildJobs::new(factory);
        jobs.initialize().await.unwrap();
        let mut result = JobResultMessage::pending("child", "resource", "dataset", "instance");
        result.parent_job_id = Some("parent".into());
        result.parent_resource_id = Some("parent-resource".into());
        result.child_index = Some(0);
        result.total_child_count = Some(2);
        result.status = JobStatus::Success;
        jobs.record(&result).await.unwrap();
        let group = jobs.record(&result).await.unwrap();
        assert_eq!(group.records.len(), 1);
        assert_eq!(group.total, 2);
        assert!(!group.failed);
    }
}
