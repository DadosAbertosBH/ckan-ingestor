// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use arrow::datatypes::{DataType, Field, Fields, Schema};
use std::sync::{Arc, LazyLock};

pub(crate) static PACKAGE_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("author", DataType::Utf8, true),
        Field::new("author_email", DataType::Utf8, true),
        Field::new("creator_user_id", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("isopen", DataType::Boolean, true),
        Field::new("license_id", DataType::Utf8, true),
        Field::new("license_title", DataType::Utf8, true),
        Field::new("license_url", DataType::Utf8, true),
        Field::new("maintainer", DataType::Utf8, true),
        Field::new("maintainer_email", DataType::Utf8, true),
        Field::new("metadata_created", DataType::Utf8, true),
        Field::new("metadata_modified", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("notes", DataType::Utf8, true),
        Field::new("num_resources", DataType::Int64, true),
        Field::new("num_tags", DataType::Int64, true),
        Field::new("owner_org", DataType::Utf8, true),
        Field::new("private", DataType::Boolean, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        extras_field(),
        groups_field(),
        organization_field(),
        tags_field(),
    ]))
});

pub(crate) static RESOURCE_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("cache_last_updated", DataType::Utf8, true),
        Field::new("cache_url", DataType::Utf8, true),
        Field::new("ckan_url", DataType::Utf8, true),
        Field::new("created", DataType::Utf8, true),
        Field::new("datastore_active", DataType::Boolean, true),
        Field::new(
            "datastore_contains_all_records_of_source_file",
            DataType::Boolean,
            true,
        ),
        Field::new("description", DataType::Utf8, true),
        Field::new("format", DataType::Utf8, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("ignore_hash", DataType::Boolean, true),
        Field::new("key", DataType::Utf8, true),
        Field::new("last_modified", DataType::Utf8, true),
        Field::new("metadata_modified", DataType::Utf8, true),
        Field::new("mimetype", DataType::Utf8, true),
        Field::new("mimetype_inner", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("original_url", DataType::Utf8, true),
        Field::new("package_id", DataType::Utf8, true),
        Field::new("position", DataType::Int64, true),
        Field::new("resource_id", DataType::Utf8, true),
        Field::new("resource_type", DataType::Utf8, true),
        Field::new("set_url_type", DataType::Boolean, true),
        Field::new("size", DataType::Int64, true),
        Field::new("state", DataType::Utf8, true),
        Field::new("task_created", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("tries", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new("url_type", DataType::Utf8, true),
    ]))
});

fn groups_field() -> Field {
    let group = DataType::Struct(Fields::from(vec![
        Field::new("description", DataType::Utf8, true),
        Field::new("display_name", DataType::Utf8, true),
        Field::new("id", DataType::Utf8, true),
        Field::new("image_display_url", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
    ]));
    Field::new(
        "groups",
        DataType::List(Arc::new(Field::new("item", group, true))),
        true,
    )
}

fn extras_field() -> Field {
    Field::new(
        "extras",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ))),
        true,
    )
}

fn tags_field() -> Field {
    Field::new(
        "tags",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("display_name", DataType::Utf8, true),
                Field::new("id", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("state", DataType::Utf8, true),
                Field::new("vocabulary_id", DataType::Utf8, true),
            ])),
            true,
        ))),
        true,
    )
}

fn organization_field() -> Field {
    Field::new(
        "organization",
        DataType::Struct(Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("title", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("image_url", DataType::Utf8, true),
            Field::new("created", DataType::Utf8, true),
            Field::new("is_organization", DataType::Boolean, true),
            Field::new("approval_status", DataType::Utf8, true),
            Field::new("state", DataType::Utf8, true),
        ])),
        true,
    )
}
