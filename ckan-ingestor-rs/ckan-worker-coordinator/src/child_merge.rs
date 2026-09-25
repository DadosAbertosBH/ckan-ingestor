// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::{collections::BTreeMap, fs::File, io::Write, path::Path, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, StringArray, new_empty_array, new_null_array},
    compute::cast,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_ipc::convert::{IpcSchemaEncoder, fb_to_schema};
use base64::Engine;
use ckan_ingestor_lib::parquet_output::ParquetOutput;
use ckan_ingestor_worker_lib::ParquetArtifact;
use futures::StreamExt;
use object_store::{ObjectStoreExt, WriteMultipart, parse_url_opts};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::KeyValue;
use tokio::io::AsyncReadExt;
use url::Url;

use crate::child_jobs::ChildRecord;

pub async fn normalize_children(
    parent_resource_id: &str,
    parent_job_id: &str,
    children: &[ChildRecord],
    storage_options: &[(String, String)],
) -> Result<(SchemaRef, Vec<ParquetArtifact>)> {
    let mut fields = BTreeMap::<String, Field>::new();
    for child in children {
        let artifact = child
            .artifact
            .as_ref()
            .context("successful child missing artifact")?;
        let schema = decode_schema(&artifact.schema_ipc_base64)?;
        for field in schema.fields() {
            fields
                .entry(field.name().clone())
                .and_modify(|previous| {
                    if previous.data_type() != field.data_type() {
                        *previous = Field::new(field.name(), DataType::Utf8, true);
                    } else if field.is_nullable() {
                        *previous = previous.clone().with_nullable(true);
                    }
                })
                .or_insert_with(|| field.as_ref().clone().with_nullable(true));
        }
    }
    let tabular = children.first().is_some_and(|child| {
        let format = child.format.to_ascii_uppercase();
        format.contains("CSV") || format.contains("JSON")
    });
    let source_column = if tabular {
        let mut name = "__source_file".to_owned();
        let mut index = 1;
        while fields.contains_key(&name) {
            name = format!("__source_file_{index}");
            index += 1;
        }
        Some(name)
    } else {
        None
    };
    let mut merged = fields.into_values().collect::<Vec<_>>();
    if let Some(name) = &source_column {
        merged.push(Field::new(name, DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(merged));
    let mut outputs = Vec::with_capacity(children.len());
    let mut geo_metadata = None::<KeyValue>;
    let mut saw_plain = false;
    for child in children {
        let artifact = child
            .artifact
            .as_ref()
            .context("successful child missing artifact")?;
        let staged = download(&artifact.uri, storage_options).await?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(staged.path())?)?;
        let geo = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|items| items.iter().find(|item| item.key == "geo"))
            .cloned()
            .map(normalize_geo_metadata)
            .transpose()?;
        if let Some(value) = &geo {
            anyhow::ensure!(
                !saw_plain,
                "cannot combine GeoJSON and plain JSON in one resource"
            );
            if let Some(previous) = &geo_metadata {
                anyhow::ensure!(
                    previous == value,
                    "cannot combine GeoJSON files with incompatible GeoParquet metadata"
                );
            } else {
                geo_metadata = Some(value.clone());
            }
        } else {
            anyhow::ensure!(
                geo_metadata.is_none(),
                "cannot combine GeoJSON and plain JSON in one resource"
            );
            saw_plain = true;
        }
        let reader = builder.with_batch_size(8192).build()?;
        let empty = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect(),
        )?;
        let mut parquet = ParquetOutput::try_new(&empty)?;
        if let Some(value) = geo {
            parquet.append_key_value_metadata(value);
        }
        for batch in reader {
            let batch = batch?;
            let columns = schema
                .fields()
                .iter()
                .map(|field| {
                    if source_column.as_deref() == Some(field.name()) {
                        return Ok(Arc::new(StringArray::from(vec![
                            child.source.as_str();
                            batch.num_rows()
                        ])) as ArrayRef);
                    }
                    match batch.schema().index_of(field.name()) {
                        Ok(index) => convert(batch.column(index), field.data_type()),
                        Err(_) => Ok(new_null_array(field.data_type(), batch.num_rows())),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            parquet.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
        }
        parquet.finish()?;
        let uri = normalized_uri(
            &artifact.uri,
            parent_resource_id,
            parent_job_id,
            child.index,
        )?;
        upload(&uri, parquet.path(), storage_options).await?;
        let encoded = IpcSchemaEncoder::new().schema_to_fb(schema.as_ref());
        outputs.push(ParquetArtifact {
            uri,
            schema_ipc_base64: base64::engine::general_purpose::STANDARD
                .encode(encoded.finished_data()),
            num_rows: parquet.rows as u64,
            file_size_bytes: parquet.file_size()?,
            footer_size_bytes: None,
            column_statistics: vec![],
        });
    }
    Ok((schema, outputs))
}

fn normalize_geo_metadata(mut metadata: KeyValue) -> Result<KeyValue> {
    if let Some(value) = metadata.value.as_deref() {
        let mut document: serde_json::Value = serde_json::from_str(value)?;
        if let Some(columns) = document
            .get_mut("columns")
            .and_then(serde_json::Value::as_object_mut)
        {
            for column in columns.values_mut() {
                if let Some(column) = column.as_object_mut() {
                    column.remove("bbox");
                }
            }
        }
        metadata.value = Some(serde_json::to_string(&document)?);
    }
    Ok(metadata)
}

fn decode_schema(encoded: &str) -> Result<Schema> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    Ok(fb_to_schema(arrow_ipc::root_as_schema(&bytes)?))
}

fn convert(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if let Ok(converted) = cast(array, target) {
        return Ok(converted);
    }
    anyhow::ensure!(
        target == &DataType::Utf8,
        "unsupported child schema conversion"
    );
    let values = (0..array.len())
        .map(|index| {
            if array.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(arrow::util::display::array_value_to_string(
                    array, index,
                )?))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(values)))
}

fn normalized_uri(source_uri: &str, resource_id: &str, job_id: &str, index: u32) -> Result<String> {
    let mut url = Url::parse(source_uri)?;
    if url.scheme() == "file" {
        let source = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid file URI"))?;
        let target = source
            .parent()
            .context("source file has no directory")?
            .join(resource_id)
            .join(format!("child_{job_id}_{index}.parquet"));
        return Url::from_file_path(target)
            .map(|value| value.into())
            .map_err(|_| anyhow::anyhow!("invalid normalized file path"));
    }
    let key = format!("{resource_id}/child_{job_id}_{index}.parquet");
    url.set_path(&key);
    Ok(url.into())
}

async fn download(uri: &str, options: &[(String, String)]) -> Result<tempfile::NamedTempFile> {
    let mut staged = tempfile::NamedTempFile::new()?;
    let url = Url::parse(uri)?;
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid file URI"))?;
        std::io::copy(&mut File::open(path)?, &mut staged)?;
    } else {
        let (store, path) = parse_url_opts(&url, options.to_vec())?;
        let mut stream = store.get(&path).await?.into_stream();
        while let Some(chunk) = stream.next().await {
            staged.write_all(&chunk?)?;
        }
    }
    staged.flush()?;
    Ok(staged)
}

async fn upload(uri: &str, source: &Path, options: &[(String, String)]) -> Result<()> {
    let url = Url::parse(uri)?;
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|_| anyhow::anyhow!("invalid file URI"))?;
        std::fs::create_dir_all(path.parent().context("normalized file has no directory")?)?;
        std::fs::copy(source, path)?;
        return Ok(());
    }
    let (store, path) = parse_url_opts(&url, options.to_vec())?;
    let upload = store.put_multipart(&path).await?;
    let mut writer = WriteMultipart::new(upload);
    let mut input = tokio::fs::File::open(source).await?;
    let mut buffer = vec![0; 5 * 1024 * 1024];
    loop {
        let size = input.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        writer.write(&buffer[..size]);
        writer.wait_for_capacity(2).await?;
    }
    writer.finish().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
    use ckan_ingestor_worker_lib::JobStatus;

    #[tokio::test]
    async fn normalizes_two_child_schemas_and_adds_source_file() {
        let directory = tempfile::tempdir().unwrap();
        let mut children = Vec::new();
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
            let path = directory.path().join(format!("child-{index}.parquet"));
            std::fs::copy(output.path(), &path).unwrap();
            let uri = Url::from_file_path(&path).unwrap().to_string();
            let encoded = IpcSchemaEncoder::new().schema_to_fb(schema.as_ref());
            children.push(ChildRecord {
                index: index as u32,
                status: JobStatus::Success,
                artifact: Some(ParquetArtifact {
                    uri,
                    schema_ipc_base64: base64::engine::general_purpose::STANDARD
                        .encode(encoded.finished_data()),
                    num_rows: 1,
                    file_size_bytes: std::fs::metadata(&path).unwrap().len(),
                    footer_size_bytes: None,
                    column_statistics: vec![],
                }),
                rows: 1,
                source: format!("child-{index}.json"),
                format: "JSON".into(),
            });
        }
        let (schema, normalized) =
            normalize_children("parent-resource", "parent-job", &children, &[])
                .await
                .unwrap();
        assert_eq!(normalized.len(), 2);
        assert!(schema.field_with_name("left").is_ok());
        assert!(schema.field_with_name("right").is_ok());
        assert!(schema.field_with_name("__source_file").is_ok());
        for artifact in normalized {
            let path = Url::parse(&artifact.uri).unwrap().to_file_path().unwrap();
            assert!(path.exists());
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .build()
                .unwrap();
            assert_eq!(
                reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>(),
                1
            );
        }
        let factory = DucklakeFactory::for_sqlite(
            &directory.path().join("catalog.sqlite"),
            &directory.path().join("data"),
        );
        let registrar = crate::parquet_registrar::ParquetRegistrar::new(factory.clone());
        registrar.initialize().await.unwrap();
        assert_eq!(
            registrar
                .register_children("parent-resource", "parent-job", "version", &children)
                .await
                .unwrap(),
            2
        );
        let scan = factory
            .client()
            .await
            .unwrap()
            .table(ducklake::TableName {
                schema: "main".into(),
                name: "parent-resource".into(),
            })
            .await
            .unwrap()
            .scan()
            .await
            .unwrap();
        assert_eq!(scan.data_files.len(), 2);
    }
}
