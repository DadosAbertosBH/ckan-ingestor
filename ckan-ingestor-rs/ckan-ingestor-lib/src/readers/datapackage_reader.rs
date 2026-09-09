// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow_json::reader::ReaderBuilder;
use reqwest::blocking::Client;
use serde_json::{Map, Value};
use std::fs::File;
use std::sync::Arc;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::{
        ckan_reader::{download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult},
        temp_file_cleanup::TempFileCleanup,
    },
};

const TEXT_FIELDS: [&str; 13] = [
    "profile",
    "name",
    "path",
    "data",
    "schema",
    "title",
    "description",
    "homepage",
    "dialect",
    "format",
    "mediatype",
    "encoding",
    "hash",
];

/// Reads Frictionless Tabular Data Packages as one Arrow row per resource.
pub struct DatapackageReader {
    client: Client,
    supported_formats: Vec<String>,
}

impl DatapackageReader {
    pub fn new() -> Self {
        Self::with_client(Client::new())
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            supported_formats: vec!["JSON".to_string()],
        }
    }

    pub fn schema() -> Schema {
        let source_fields = Fields::from(vec![
            Field::new("title", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ]);
        let license_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
        ]);
        let mut fields = vec![Field::new("package_id", DataType::Utf8, false)];
        fields.extend(
            TEXT_FIELDS
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, true)),
        );
        fields.push(Field::new(
            "sources",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(source_fields),
                true,
            ))),
            true,
        ));
        fields.push(Field::new(
            "licenses",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(license_fields),
                true,
            ))),
            true,
        ));
        fields.push(Field::new("bytes", DataType::Int64, true));
        Schema::new(fields)
    }

    fn read_document(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let path = if is_remote {
            download_to_temp(&self.client, &resource.url, ".json")?
        } else {
            resource.url.clone()
        };
        let mut cleanup = TempFileCleanup::from_path(path.clone().into());
        if !is_remote {
            cleanup.commit();
        }
        let document: Value = serde_json::from_reader(File::open(&path)?)
            .with_context(|| format!("invalid datapackage JSON: {}", resource.url))?;
        let resources = document
            .get("resources")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                FailedResult::from_string(
                    "resources missing or not array",
                    self.reader_name().to_string(),
                )
            })?;
        if resources.is_empty() {
            return Err(FailedResult::from_string(
                "No data",
                self.reader_name().to_string(),
            ));
        }
        let rows = resources
            .iter()
            .map(|item| normalize_resource(item, &resource.package_id))
            .collect::<Result<Vec<_>>>()?;
        let mut decoder = ReaderBuilder::new(Arc::new(Self::schema()))
            .with_batch_size(8192)
            .build_decoder()?;
        decoder.serialize(&rows)?;
        let batch = decoder.flush()?.ok_or_else(|| anyhow::anyhow!("No data"))?;
        let mut output = ParquetOutput::try_new(&batch)?;
        output.write(&batch)?;
        output.finish()?;
        Ok(SuccessResult::new(output, self.reader_name().to_string()))
    }
}

fn normalize_resource(resource: &Value, package_id: &str) -> Result<Value> {
    let resource = resource
        .as_object()
        .context("datapackage resource must be an object")?;
    let mut normalized = Map::new();
    normalized.insert(
        "package_id".to_string(),
        Value::String(package_id.to_string()),
    );
    for field in TEXT_FIELDS {
        if let Some(value) = resource.get(field) {
            normalized.insert(field.to_string(), json_text(value)?);
        }
    }
    if let Some(value) = resource.get("bytes") {
        normalized.insert("bytes".to_string(), value.clone());
    }
    copy_object_list(
        resource,
        &mut normalized,
        "sources",
        &["title", "path", "email"],
    )?;
    copy_object_list(
        resource,
        &mut normalized,
        "licenses",
        &["name", "path", "title"],
    )?;
    Ok(Value::Object(normalized))
}

fn json_text(value: &Value) -> Result<Value> {
    match value {
        Value::Null | Value::String(_) => Ok(value.clone()),
        value => Ok(Value::String(serde_json::to_string(value)?)),
    }
}

fn copy_object_list(
    resource: &Map<String, Value>,
    normalized: &mut Map<String, Value>,
    field: &str,
    properties: &[&str],
) -> Result<()> {
    let Some(values) = resource.get(field) else {
        return Ok(());
    };
    let values = values
        .as_array()
        .with_context(|| format!("datapackage {field} must be an array"))?;
    let values = values
        .iter()
        .map(|value| {
            let value = value
                .as_object()
                .with_context(|| format!("datapackage {field} item must be an object"))?;
            let mut item = Map::new();
            for property in properties {
                if let Some(value) = value.get(*property) {
                    item.insert((*property).to_string(), json_text(value)?);
                }
            }
            Ok(Value::Object(item))
        })
        .collect::<Result<Vec<_>>>()?;
    normalized.insert(field.to_string(), Value::Array(values));
    Ok(())
}

impl Default for DatapackageReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CkanReader for DatapackageReader {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_document(resource)
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        resource
            .url
            .split(['?', '#'])
            .next()
            .and_then(|url| url.rsplit('/').next())
            .is_some_and(|filename| filename == "datapackage.json")
    }
}
