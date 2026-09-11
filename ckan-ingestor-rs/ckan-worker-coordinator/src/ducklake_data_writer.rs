// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::{
    array::{RecordBatch, StringViewArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use ckan_ingestor_lib::parquet_output::ParquetOutput;
use ducklake::{Column, Ducklake, IfExistsStrategy, TableName, WriteDataFile};
use object_store::{ObjectStoreExt, WriteMultipart, parse_url_opts};
use tokio::io::AsyncReadExt;
use url::Url;

use crate::data_writer::DataWriter;

pub const LAST_UPDATE_TABLE: &str = "ckan_resource_last_update";

#[derive(Clone)]
pub struct DucklakeDataWriter {
    client: Arc<Ducklake>,
    storage_options: Vec<(String, String)>,
}

impl DucklakeDataWriter {
    pub fn new(client: Arc<Ducklake>, storage_options: impl Into<Vec<(String, String)>>) -> Self {
        Self {
            client,
            storage_options: storage_options.into(),
        }
    }

    async fn initialize_table_impl(
        &self,
        name: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<()> {
        let table_name = TableName {
            schema: "main".into(),
            name: name.into(),
        };
        if self.client.table_exists(table_name.clone()).await? {
            let table = self.client.table(table_name.clone()).await?;
            let existing_columns = table
                .columns()
                .await?
                .map(|column| column.name)
                .collect::<std::collections::HashSet<_>>();
            let missing_columns = schema
                .fields()
                .iter()
                .filter(|field| !existing_columns.contains(field.name()))
                .map(|field| Column::try_from(field.as_ref()))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if !missing_columns.is_empty() {
                let mut transaction = self.client.transaction().await?;
                let mut table = transaction.table(table_name)?;
                for column in missing_columns {
                    table.add_column(column).await?;
                }
                transaction.commit().await?;
            }
            return Ok(());
        }
        let columns = schema
            .fields()
            .iter()
            .map(|field| Column::try_from(field.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut transaction = self.client.transaction().await?;
        transaction.create_table(
            table_name,
            columns,
            None,
            None,
            None,
            IfExistsStrategy::Fail,
        )?;
        transaction.commit().await?;
        Ok(())
    }

    async fn append_batch(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut parquet = ParquetOutput::try_new(batch)?;
        parquet.write(batch)?;
        parquet.finish()?;
        let table = self.client.table(table_name).await?;
        let (_, path_generator) = table.get_write_info().await?;
        let relative_path = path_generator.generate_relative(&Default::default());
        let absolute_path = format!("{}{relative_path}", path_generator.base_path());
        copy_data_file(parquet.path(), &absolute_path, &self.storage_options).await?;
        table
            .write_data_files(vec![WriteDataFile {
                path: relative_path,
                statistics: None,
                partition_values: None,
            }])
            .await
            .context("registering metadata Parquet file")?;
        Ok(())
    }
}

impl DataWriter for DucklakeDataWriter {
    async fn initialize_table(
        &self,
        table_name: &str,
        schema: &arrow::datatypes::SchemaRef,
    ) -> Result<()> {
        self.initialize_table_impl(table_name, schema).await
    }

    async fn ingest(&self, table_name: &str, batch: &RecordBatch) -> Result<()> {
        self.append_batch(table_name, batch).await
    }
}

pub async fn initialize_last_update_table(client: &Ducklake) -> Result<()> {
    if client.table_exists(LAST_UPDATE_TABLE).await? {
        let table = client.table(LAST_UPDATE_TABLE).await?;
        if !table
            .columns()
            .await?
            .any(|column| column.name == "source_version")
        {
            let mut transaction = client.transaction().await?;
            transaction
                .table(LAST_UPDATE_TABLE)?
                .add_column(Column::try_from(&Field::new(
                    "source_version",
                    DataType::Utf8View,
                    true,
                ))?)
                .await?;
            transaction.commit().await?;
        }
        return Ok(());
    }
    let schema = last_update_schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| Column::try_from(field.as_ref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut transaction = client.transaction().await?;
    transaction.create_table(
        LAST_UPDATE_TABLE,
        columns,
        None,
        None,
        None,
        IfExistsStrategy::Fail,
    )?;
    transaction.commit().await?;
    Ok(())
}

pub fn append_last_update(
    transaction: &mut ducklake::Transaction<'_>,
    resource_id: &str,
    source_version: &str,
) -> Result<()> {
    let mut table = transaction.table(LAST_UPDATE_TABLE)?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    table.write_inline_data(vec![RecordBatch::try_new(
        last_update_schema(),
        vec![
            Arc::new(StringViewArray::from(vec![resource_id])),
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
            Arc::new(StringViewArray::from(vec![Some(source_version)])),
        ],
    )?])?;
    Ok(())
}

fn last_update_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("ckan_resource_id", DataType::Utf8View, false),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("source_version", DataType::Utf8View, true),
    ]))
}

async fn copy_data_file(
    source: &std::path::Path,
    destination: &str,
    storage_options: &[(String, String)],
) -> Result<()> {
    if destination.starts_with("s3://") {
        let url = Url::parse(destination)?;
        let (store, path) = parse_url_opts(&url, storage_options.to_vec())?;
        let upload = store.put_multipart(&path).await?;
        let mut writer = WriteMultipart::new(upload);
        let mut input = tokio::fs::File::open(source).await?;
        let mut buffer = vec![0; 5 * 1024 * 1024];
        loop {
            let bytes_read = input.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            writer.write(&buffer[..bytes_read]);
            writer.wait_for_capacity(4).await?;
        }
        writer.finish().await?;
        return Ok(());
    }
    let destination_path = if destination.starts_with("file://") {
        Url::parse(destination)?
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid local data file URL: {destination}"))?
    } else {
        destination.into()
    };
    if let Some(parent) = destination_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(source, destination_path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;

    use super::DucklakeDataWriter;
    use crate::data_writer::DataWriter;

    #[tokio::test]
    async fn initialize_table_adds_columns_missing_from_an_existing_table() {
        let temp = tempfile::tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let client = factory.client().await.unwrap();
        let writer = DucklakeDataWriter::new(client.clone(), factory.storage_options().to_vec());
        let legacy_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        writer
            .initialize_table("ckan_dataset", &legacy_schema)
            .await
            .unwrap();

        let expanded_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("instance_id", DataType::Utf8, true),
        ]));
        writer
            .initialize_table("ckan_dataset", &expanded_schema)
            .await
            .unwrap();

        let columns = client
            .table("ckan_dataset")
            .await
            .unwrap()
            .columns()
            .await
            .unwrap()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        assert_eq!(columns, vec!["id", "instance_id"]);
    }
}
