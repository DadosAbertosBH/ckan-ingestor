// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use crate::ckan_schemas::{PACKAGE_SCHEMA, RESOURCE_SCHEMA};
use crate::fetcher::CkanPackageStream;
use crate::package_processing::{drop_empty_list_columns, extract_resources, validate_list_shapes};
use anyhow::{Context, Result};
use arrow_json::ReaderBuilder;
use ckan_ingestor_lib::arrow_ipc_output::ArrowIpcOutput;
use serde_json::Value;
use std::path::Path;

/// Typed Arrow IPC files derived from one CKAN package stream.
pub struct StructuredIpc {
    packages: ArrowIpcOutput,
    resources: Option<ArrowIpcOutput>,
}

impl StructuredIpc {
    pub fn from_packages<I>(packages: I) -> Result<Self>
    where
        I: IntoIterator<Item = Value>,
    {
        Self::from_pages([packages.into_iter().collect()])
    }

    pub fn from_pages<I>(pages: I) -> Result<Self>
    where
        I: IntoIterator<Item = Vec<Value>>,
    {
        Self::write_pages(pages.into_iter().map(Ok), "")
    }

    pub(crate) fn write(mut packages: CkanPackageStream, instance_url: &str) -> Result<Self> {
        Self::write_pages(std::iter::from_fn(|| packages.next_page()), instance_url)
    }

    fn write_pages<I>(pages: I, instance_url: &str) -> Result<Self>
    where
        I: IntoIterator<Item = Result<Vec<Value>>>,
    {
        let mut package_output = None;
        let mut resource_output = None;
        let mut package_count = 0;
        for page in pages {
            let mut package_rows = page?;
            if package_rows.is_empty() {
                continue;
            }
            package_count += package_rows.len();
            write_normalized_batch(
                &mut package_rows,
                instance_url,
                &mut package_output,
                &mut resource_output,
            )?;
        }
        if package_count == 0 {
            anyhow::bail!("No packages returned from CKAN API");
        }
        let packages = finish_output(package_output.context("values contain no rows")?)?;
        let resources = resource_output.map(finish_output).transpose()?;
        Ok(Self {
            packages,
            resources,
        })
    }

    pub fn package_path(&self) -> &Path {
        self.packages.path()
    }

    pub fn resource_path(&self) -> Option<&Path> {
        self.resources.as_ref().map(|output| output.path())
    }

    pub(crate) fn package_rows(&self) -> usize {
        self.packages.rows
    }

    pub(crate) fn resource_rows(&self) -> usize {
        self.resources.as_ref().map_or(0, |output| output.rows)
    }

    pub(crate) fn packages(&self) -> &ArrowIpcOutput {
        &self.packages
    }

    pub(crate) fn resources(&self) -> Option<&ArrowIpcOutput> {
        self.resources.as_ref()
    }
}

fn write_normalized_batch(
    package_rows: &mut Vec<Value>,
    instance_url: &str,
    package_output: &mut Option<ArrowIpcOutput>,
    resource_output: &mut Option<ArrowIpcOutput>,
) -> Result<()> {
    drop_empty_list_columns(package_rows);
    validate_list_shapes(package_rows)?;
    let resource_rows = extract_resources(package_rows, instance_url);
    write_values(package_rows, package_output, &PACKAGE_SCHEMA)?;
    write_values(&resource_rows, resource_output, &RESOURCE_SCHEMA)?;
    package_rows.clear();
    Ok(())
}

fn write_values(
    values: &[Value],
    output: &mut Option<ArrowIpcOutput>,
    schema: &std::sync::Arc<arrow::datatypes::Schema>,
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut decoder = ReaderBuilder::new(std::sync::Arc::clone(schema))
        .with_batch_size(values.len())
        .with_strict_mode(true)
        .with_coerce_primitive(true)
        .build_decoder()?;
    decoder.serialize(values)?;
    let Some(batch) = decoder.flush()? else {
        return Ok(());
    };
    if output.is_none() {
        *output = Some(ArrowIpcOutput::try_new(&batch)?);
    }
    output
        .as_mut()
        .context("Arrow IPC output initialized")?
        .write(&batch)?;
    Ok(())
}

fn finish_output(mut output: ArrowIpcOutput) -> Result<ArrowIpcOutput> {
    output.finish()?;
    Ok(output)
}
