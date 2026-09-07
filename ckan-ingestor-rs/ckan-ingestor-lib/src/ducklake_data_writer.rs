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
use ducklake::{Column, IfExistsStrategy, TableName, WriteDataFile};
use object_store::{parse_url_opts, ObjectStoreExt, WriteMultipart};
use tokio::io::AsyncReadExt;
use url::Url;

use crate::{data_writer::DataWriter, parquet_output::ParquetOutput};

pub(crate) const LAST_UPDATE_TABLE: &str = "ckan_resource_last_update";

pub struct DucklakeDataWriter {
    client: Arc<ducklake::Ducklake>,
    storage_options: Vec<(String, String)>,
}

impl DucklakeDataWriter {
    pub fn new(
        client: Arc<ducklake::Ducklake>,
        storage_options: impl Into<Vec<(String, String)>>,
    ) -> Self {
        Self {
            client,
            storage_options: storage_options.into(),
        }
    }
}

impl DataWriter for DucklakeDataWriter {
    async fn ingest(&self, resource_id: &str, parquet: &ParquetOutput) -> Result<()> {
        let columns = parquet
            .schema
            .fields()
            .iter()
            .map(|field| Column::try_from(field.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let table_name = TableName {
            schema: "main".into(),
            name: resource_id.into(),
        };
        let mut transaction = self.client.transaction().await?;

        if self.client.table_exists(table_name.clone()).await? {
            transaction.table(table_name.clone())?.delete()?;
        }
        let mut table = transaction.create_table(
            table_name,
            columns,
            None,
            None,
            None,
            IfExistsStrategy::Fail,
        )?;
        let (_, path_generator) = table.get_write_info()?;
        let partitions = Default::default();
        let relative_path = path_generator.generate_relative(&partitions);
        let absolute_path = format!("{}{relative_path}", path_generator.base_path());
        copy_data_file(parquet.path(), &absolute_path, &self.storage_options).await?;
        table
            .write_data_files(vec![WriteDataFile {
                path: relative_path,
                statistics: None,
                partition_values: None,
            }])
            .await
            .context("registering the Parquet data file")?;
        transaction
            .commit()
            .await
            .context("committing the resource table")?;
        replace_last_update(&self.client, resource_id).await?;
        Ok(())
    }
}

async fn replace_last_update(client: &ducklake::Ducklake, resource_id: &str) -> Result<()> {
    if client.table_exists(LAST_UPDATE_TABLE).await? {
        let mut transaction = client.transaction().await?;
        transaction.table(LAST_UPDATE_TABLE)?.delete()?;
        transaction
            .commit()
            .await
            .context("removing the previous inline last-update row")?;
    }
    let mut transaction = client.transaction().await?;
    let schema = last_update_schema();
    let columns = schema
        .fields()
        .iter()
        .map(|field| Column::try_from(field.as_ref()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    transaction.create_table(
        LAST_UPDATE_TABLE,
        columns,
        None,
        None,
        None,
        IfExistsStrategy::Fail,
    )?;
    transaction
        .commit()
        .await
        .context("creating the last-update table")?;

    let mut transaction = client.transaction().await?;
    let mut table = transaction.table(LAST_UPDATE_TABLE)?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_micros() as i64;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![resource_id])),
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
        ],
    )?;
    table.write_inline_data(vec![batch])?;
    transaction
        .commit()
        .await
        .context("committing the inline last-update row")?;
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
    } else {
        let destination_path;
        let destination = if destination.starts_with("file://") {
            destination_path = Url::parse(destination)?
                .to_file_path()
                .map_err(|_| anyhow::anyhow!("invalid local data file URL: {destination}"))?;
            destination_path.as_path()
        } else {
            std::path::Path::new(destination)
        };
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::copy(source, destination).await?;
    }
    Ok(())
}
