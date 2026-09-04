// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Result, bail};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

pub(crate) fn extract_resources(packages: &mut [Value], instance_url: &str) -> Vec<Value> {
    packages
        .iter_mut()
        .flat_map(|package| {
            match package
                .as_object_mut()
                .and_then(|object| object.remove("resources"))
            {
                Some(Value::Array(resources)) => resources,
                _ => Vec::new(),
            }
            .into_iter()
        })
        .map(|mut resource| {
            if let Some(object) = resource.as_object_mut() {
                object.insert("ckan_url".into(), Value::String(instance_url.into()));
            }
            resource
        })
        .collect()
}

pub(crate) fn drop_empty_list_columns(rows: &mut [Value]) {
    let names = rows
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|object| object.keys())
        .filter(|name| name.as_str() != "resources")
        .cloned()
        .collect::<HashSet<_>>();
    let empty_list_columns = names
        .into_iter()
        .filter(|name| {
            let mut saw_empty_list = false;
            let all_empty_or_null = rows.iter().all(|row| {
                let value = row.get(name).unwrap_or(&Value::Null);
                if let Some(values) = value.as_array()
                    && values.is_empty()
                {
                    saw_empty_list = true;
                    return true;
                }
                value.is_null()
            });
            saw_empty_list && all_empty_or_null
        })
        .collect::<Vec<_>>();
    for row in rows {
        if let Some(object) = row.as_object_mut() {
            for name in &empty_list_columns {
                object.remove(name);
            }
        }
    }
}

pub(crate) fn validate_list_shapes(rows: &[Value]) -> Result<()> {
    let mut shapes: HashMap<&str, &'static str> = HashMap::new();
    for row in rows {
        let Some(object) = row.as_object() else {
            continue;
        };
        for (name, value) in object {
            let Some(values) = value.as_array() else {
                continue;
            };
            let Some(first) = values.first() else {
                continue;
            };
            let shape = if first.is_object() {
                "object"
            } else if first.is_array() {
                "array"
            } else {
                "scalar"
            };
            if let Some(existing) = shapes.insert(name, shape)
                && existing != shape
            {
                bail!("incompatible list shape for column '{name}': {existing} versus {shape}");
            }
        }
    }
    Ok(())
}
