// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use arrow::array::{Array, BooleanArray, RecordBatch, new_null_array};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::SchemaRef;
use arrow_ipc::reader::FileReader;
use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
use ckan_metadata_ingestor::{MetadataSyncCommand, MetadataSyncResult, StructuredIpc};
use ducklake::Ducklake;
use object_store::{ObjectStoreExt, parse_url_opts};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use url::Url;

use crate::data_writer::DataWriter;
use crate::ducklake_data_writer::DucklakeDataWriter;

#[derive(Debug, Clone)]
pub struct ResourceCandidate {
    pub resource_id: String,
    pub resource_name: Option<String>,
    pub resource_url: Option<String>,
    pub resource_format: Option<String>,
    pub dataset_name: String,
    pub datastore_active: bool,
}

#[derive(Clone)]
pub struct RealMetadataProcessor<T: DataWriter = DucklakeDataWriter> {
    factory: DucklakeFactory,
    writer: T,
}

impl<T: DataWriter> RealMetadataProcessor<T> {
    pub fn new(factory: DucklakeFactory, writer: T) -> Self {
        Self { factory, writer }
    }

    pub async fn process(&self, command: MetadataSyncCommand) -> MetadataSyncResult {
        match self.sync(&command).await {
            Ok(result) => result,
            Err(error) => MetadataSyncResult {
                sync_id: command.sync_id,
                instance_id: command.instance_id,
                instance_name: command.instance_name,
                status: "failure".into(),
                total_packages: 0,
                new_datasets: 0,
                new_resources: 0,
                updated_datasets: 0,
                updated_resources: 0,
                dataset_count: 0,
                resource_count: 0,
                error_message: Some(error.to_string().chars().take(16_000).collect()),
            },
        }
    }

    async fn sync(&self, command: &MetadataSyncCommand) -> Result<MetadataSyncResult> {
        let ipc = StructuredIpc::fetch(&command.instance_url)?;
        self.ingest_ipc(command, &ipc).await
    }

    async fn ingest_ipc(
        &self,
        command: &MetadataSyncCommand,
        ipc: &StructuredIpc,
    ) -> Result<MetadataSyncResult> {
        let client = self.factory.client().await?;
        let package_batches = read_ipc(ipc.package_path())?;
        let resource_batches = ipc.resource_path().map(read_ipc).transpose()?;

        let datasets = merge_table(
            &client,
            &self.factory,
            "ckan_dataset",
            package_batches,
            "metadata_modified",
        )
        .await?;
        let resources = match resource_batches {
            Some(batches) => Some(
                merge_table(
                    &client,
                    &self.factory,
                    "ckan_resource",
                    batches,
                    "last_modified",
                )
                .await?,
            ),
            None => None,
        };

        let mut updated_datasets = datasets.updated_ids.iter().cloned().collect::<HashSet<_>>();
        if let Some(resources) = &resources {
            for row in 0..resources.batch.num_rows() {
                let Some(resource_id) = value(&resources.batch, "id", row)? else {
                    continue;
                };
                if resources.updated_ids.contains(&resource_id)
                    && let Some(package_id) = value(&resources.batch, "package_id", row)?
                {
                    updated_datasets.insert(package_id);
                }
            }
        }

        self.writer
            .initialize_table("ckan_dataset", &datasets.batch.schema())
            .await?;
        if let Some(resources) = &resources {
            self.writer
                .initialize_table("ckan_resource", &resources.batch.schema())
                .await?;
        }
        self.writer.ingest("ckan_dataset", &datasets.batch).await?;
        if let Some(resources) = &resources {
            self.writer
                .ingest("ckan_resource", &resources.batch)
                .await?;
        }

        Ok(MetadataSyncResult {
            sync_id: command.sync_id.clone(),
            instance_id: command.instance_id.clone(),
            instance_name: command.instance_name.clone(),
            status: "success".into(),
            total_packages: ipc.package_rows() as i64,
            new_datasets: datasets.new,
            new_resources: resources.as_ref().map_or(0, |result| result.new),
            updated_datasets: updated_datasets.len() as i64,
            updated_resources: resources
                .as_ref()
                .map_or(0, |result| result.updated_ids.len() as i64),
            dataset_count: ipc.package_rows() as i64,
            resource_count: ipc.resource_rows() as i64,
            error_message: None,
        })
    }

    pub async fn outdated_resources(&self, ckan_url: &str) -> Result<Vec<ResourceCandidate>> {
        let client = self.factory.client().await?;
        query_outdated_resources(&client, &self.factory, ckan_url).await
    }
}

struct MergeResult {
    new: i64,
    updated_ids: HashSet<String>,
    batch: RecordBatch,
}

async fn merge_table(
    client: &Ducklake,
    factory: &DucklakeFactory,
    table_name: &str,
    incoming: Vec<RecordBatch>,
    updated_at: &str,
) -> Result<MergeResult> {
    let schema = incoming
        .first()
        .context("metadata IPC contains no record batches")?
        .schema();
    let existing = load_table(client, factory, table_name).await?;
    let existing = existing
        .iter()
        .map(|batch| align_batch(batch, &schema))
        .collect::<Result<Vec<_>>>()?;
    let existing = concat_batches(&schema, &existing)?;
    let incoming = concat_batches(&schema, &incoming)?;

    let mut existing_rows = newest_rows(&existing, updated_at)?;
    let mut new = 0_i64;
    let mut updated_ids = HashSet::new();
    let mut selected = vec![false; incoming.num_rows()];
    for (row, is_selected) in selected.iter_mut().enumerate() {
        let id = value(&incoming, "id", row)?.context("metadata id cannot be null")?;
        let incoming_value = value(&incoming, updated_at, row)?.unwrap_or_default();
        match existing_rows.get(&id) {
            None => {
                new += 1;
                existing_rows.insert(id, (row, incoming_value));
                *is_selected = true;
            }
            Some((_, existing_value)) if incoming_value > *existing_value => {
                updated_ids.insert(id.clone());
                existing_rows.insert(id, (row, incoming_value));
                *is_selected = true;
            }
            _ => {}
        }
    }

    let predicate = BooleanArray::from_iter(selected.into_iter().map(Some));
    Ok(MergeResult {
        new,
        updated_ids,
        batch: filter_record_batch(&incoming, &predicate)?,
    })
}

fn newest_rows(batch: &RecordBatch, updated_at: &str) -> Result<HashMap<String, (usize, String)>> {
    let mut rows = HashMap::<String, (usize, String)>::new();
    for row in 0..batch.num_rows() {
        let id = value(batch, "id", row)?.context("metadata id cannot be null")?;
        let timestamp = value(batch, updated_at, row)?.unwrap_or_default();
        if rows
            .get(&id)
            .is_none_or(|(_, current)| timestamp > *current)
        {
            rows.insert(id, (row, timestamp));
        }
    }
    Ok(rows)
}

async fn query_outdated_resources(
    client: &Ducklake,
    factory: &DucklakeFactory,
    ckan_url: &str,
) -> Result<Vec<ResourceCandidate>> {
    let resources = load_table(client, factory, "ckan_resource").await?;
    if resources.is_empty() {
        return Ok(Vec::new());
    }
    let datasets = load_table(client, factory, "ckan_dataset").await?;
    let updates = load_table(client, factory, "ckan_resource_last_update").await?;
    let dataset_names = latest_values(&datasets, "id", "name", "metadata_modified")?;
    let update_times = latest_values(
        &updates,
        "ckan_resource_id",
        "last_modified",
        "last_modified",
    )?;
    let resources = latest_batches(&resources, "last_modified")?;
    let mut result = Vec::new();
    for row in 0..resources.num_rows() {
        if value(&resources, "ckan_url", row)?.as_deref() != Some(ckan_url) {
            continue;
        }
        let resource_id = value(&resources, "id", row)?.context("resource id cannot be null")?;
        let modified = value(&resources, "last_modified", row)?.unwrap_or_default();
        if update_times
            .get(&resource_id)
            .is_some_and(|last_update| modified < *last_update)
        {
            continue;
        }
        let package_id = value(&resources, "package_id", row)?.unwrap_or_default();
        result.push(ResourceCandidate {
            resource_id,
            resource_name: value(&resources, "name", row)?,
            resource_url: value(&resources, "url", row)?,
            resource_format: value(&resources, "format", row)?,
            dataset_name: dataset_names
                .get(&package_id)
                .cloned()
                .unwrap_or(package_id),
            datastore_active: boolean_value(&resources, "datastore_active", row)?.unwrap_or(false),
        });
    }
    Ok(result)
}

fn latest_values(
    batches: &[RecordBatch],
    id_column: &str,
    value_column: &str,
    updated_at: &str,
) -> Result<HashMap<String, String>> {
    let mut values = HashMap::<String, (String, String)>::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let Some(id) = value(batch, id_column, row)? else {
                continue;
            };
            let timestamp = value(batch, updated_at, row)?.unwrap_or_default();
            if values
                .get(&id)
                .is_none_or(|(_, current)| timestamp > *current)
            {
                values.insert(
                    id,
                    (
                        value(batch, value_column, row)?.unwrap_or_default(),
                        timestamp,
                    ),
                );
            }
        }
    }
    Ok(values
        .into_iter()
        .map(|(id, (value, _))| (id, value))
        .collect())
}

fn latest_batches(batches: &[RecordBatch], updated_at: &str) -> Result<RecordBatch> {
    let schema = batches
        .first()
        .context("table contains no batches")?
        .schema();
    let batches = batches
        .iter()
        .map(|batch| align_batch(batch, &schema))
        .collect::<Result<Vec<_>>>()?;
    let combined = concat_batches(&schema, &batches)?;
    let selected = newest_rows(&combined, updated_at)?;
    let indices = selected
        .values()
        .map(|(row, _)| *row)
        .collect::<HashSet<_>>();
    let predicate =
        BooleanArray::from_iter((0..combined.num_rows()).map(|row| Some(indices.contains(&row))));
    Ok(filter_record_batch(&combined, &predicate)?)
}

async fn load_table(
    client: &Ducklake,
    factory: &DucklakeFactory,
    name: &str,
) -> Result<Vec<RecordBatch>> {
    if !client.table_exists(name).await? {
        return Ok(Vec::new());
    }
    let scan = client.table(name).await?.scan().await?;
    let mut batches = scan.inline_data;
    for data_file in scan.data_files {
        batches.extend(read_parquet(&data_file.path, factory.storage_options()).await?);
    }
    Ok(batches)
}

async fn read_parquet(
    path: &str,
    storage_options: &[(String, String)],
) -> Result<Vec<RecordBatch>> {
    if path.contains("://") && !path.starts_with("file://") {
        let url = Url::parse(path)?;
        let (store, object_path) = parse_url_opts(&url, storage_options.to_vec())?;
        let bytes = store.get(&object_path).await?.bytes().await?;
        return ParquetRecordBatchReaderBuilder::try_new(bytes)?
            .build()?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into);
    }
    let local_path = if path.starts_with("file://") {
        Url::parse(path)?
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid local Parquet URL: {path}"))?
    } else {
        path.into()
    };
    ParquetRecordBatchReaderBuilder::try_new(File::open(local_path)?)?
        .build()?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn read_ipc(path: &std::path::Path) -> Result<Vec<RecordBatch>> {
    FileReader::try_new(File::open(path)?, None)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn align_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match batch.schema().index_of(field.name()) {
            Ok(index) if batch.column(index).data_type() == field.data_type() => {
                Ok(Arc::clone(batch.column(index)))
            }
            Ok(index) => cast(batch.column(index), field.data_type()).map_err(Into::into),
            Err(_) => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<String>> {
    let array = batch
        .column_by_name(column)
        .with_context(|| format!("missing metadata column '{column}'"))?;
    if array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(arrow::util::display::array_value_to_string(
        array, row,
    )?))
}

fn boolean_value(batch: &RecordBatch, column: &str, row: usize) -> Result<Option<bool>> {
    let array = batch
        .column_by_name(column)
        .with_context(|| format!("missing metadata column '{column}'"))?;
    if array.is_null(row) {
        return Ok(None);
    }
    let value = arrow::util::display::array_value_to_string(array, row)?;
    ensure!(value == "true" || value == "false", "invalid boolean value");
    Ok(Some(value == "true"))
}

#[cfg(test)]
mod tests {
    use super::{RealMetadataProcessor, query_outdated_resources};
    use crate::data_writer::DataWriter;
    use crate::ducklake_data_writer::DucklakeDataWriter;
    use anyhow::Result;
    use arrow::array::RecordBatch;
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
    use ckan_metadata_ingestor::{MetadataSyncCommand, StructuredIpc};
    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    #[derive(Clone, Default)]
    struct RecordingWriter {
        calls: Arc<Mutex<Vec<(String, usize)>>>,
    }

    impl DataWriter for RecordingWriter {
        async fn initialize_table(
            &self,
            table_name: &str,
            schema: &arrow::datatypes::SchemaRef,
        ) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((format!("initialize:{table_name}"), schema.fields().len()));
            Ok(())
        }

        async fn ingest(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((format!("ingest:{table_name}"), batch.num_rows()));
            Ok(())
        }
    }

    async fn ducklake_writer(factory: &DucklakeFactory) -> DucklakeDataWriter {
        DucklakeDataWriter::new(
            factory.client().await.unwrap(),
            factory.storage_options().to_vec(),
        )
    }

    fn package(dataset_modified: &str, resource_modified: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "dataset-1",
            "name": "Dataset",
            "metadata_modified": dataset_modified,
            "resources": [{
                "id": "resource-1",
                "name": "Resource",
                "package_id": "dataset-1",
                "last_modified": resource_modified,
                "url": "https://example.test/resource.csv",
                "format": "CSV"
            }]
        })
    }

    fn command() -> MetadataSyncCommand {
        MetadataSyncCommand {
            sync_id: "sync-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Test".into(),
            instance_url: "https://ckan.example".into(),
        }
    }

    #[tokio::test]
    async fn sdk_merge_counts_new_updated_and_identical_rows() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let processor =
            RealMetadataProcessor::new(factory.clone(), ducklake_writer(&factory).await);

        let first = StructuredIpc::from_packages([package("2024-01-01", "2024-01-01")]).unwrap();
        let first = processor.ingest_ipc(&command(), &first).await.unwrap();
        assert_eq!((first.new_datasets, first.new_resources), (1, 1));

        let identical =
            StructuredIpc::from_packages([package("2024-01-01", "2024-01-01")]).unwrap();
        let identical = processor.ingest_ipc(&command(), &identical).await.unwrap();
        assert_eq!(
            (
                identical.new_datasets,
                identical.new_resources,
                identical.updated_datasets,
                identical.updated_resources
            ),
            (0, 0, 0, 0)
        );

        let updated = StructuredIpc::from_packages([package("2025-01-01", "2025-01-01")]).unwrap();
        let updated = processor.ingest_ipc(&command(), &updated).await.unwrap();
        assert_eq!(
            (updated.updated_datasets, updated.updated_resources),
            (1, 1)
        );

        let client = factory.client().await.unwrap();
        for table_name in ["ckan_dataset", "ckan_resource"] {
            let table = client.table(table_name).await.unwrap();
            let scan = table.scan().await.unwrap();
            assert!(
                scan.inline_data.is_empty(),
                "{table_name} must store metadata in Parquet data files"
            );
            assert!(!scan.data_files.is_empty());
        }
    }

    #[tokio::test]
    async fn outdated_resources_are_scoped_to_the_ckan_url() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let processor =
            RealMetadataProcessor::new(factory.clone(), ducklake_writer(&factory).await);
        let ipc = StructuredIpc::from_packages([package("2025-01-01", "2025-01-02")]).unwrap();
        processor.ingest_ipc(&command(), &ipc).await.unwrap();

        let client = factory.client().await.unwrap();
        let resources = query_outdated_resources(&client, &factory, "")
            .await
            .unwrap();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].resource_id, "resource-1");
        assert!(
            query_outdated_resources(&client, &factory, "https://other.example")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn persists_metadata_through_the_data_writer_contract() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let writer = RecordingWriter::default();
        let calls = Arc::clone(&writer.calls);
        let processor = RealMetadataProcessor::new(factory, writer);
        let ipc = StructuredIpc::from_packages([package("2025-01-01", "2025-01-02")]).unwrap();

        processor.ingest_ipc(&command(), &ipc).await.unwrap();

        assert_eq!(
            *calls.lock().unwrap(),
            vec![
                ("initialize:ckan_dataset".into(), 27),
                ("initialize:ckan_resource".into(), 30),
                ("ingest:ckan_dataset".into(), 1),
                ("ingest:ckan_resource".into(), 1),
            ]
        );
    }
}
