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
use arrow::datatypes::DataType;
use arrow_ipc::reader::FileReader;
use ckan_metadata_ingestor::{StructuredIpc, fetcher::PAGE_SIZE};
use httpmock::MockServer;
use std::fs::File;

#[test]
fn writes_packages_as_typed_arrow_columns() {
    let ipc = StructuredIpc::from_packages(vec![serde_json::json!({
        "id": "dataset-1",
        "title": "Dataset",
        "notes": 42,
        "resources": [{"id": "resource-1", "url": "https://example.test/data.csv"}]
    })])
    .expect("IPC should be written");

    let package_reader =
        FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let package_batch = package_reader.into_iter().next().unwrap().unwrap();
    assert!(package_batch.column_by_name("id").is_some());
    assert!(package_batch.column_by_name("title").is_some());
    assert!(package_batch.column_by_name("resources").is_none());
    assert_eq!(
        package_batch
            .schema()
            .field_with_name("notes")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );

    let resource_reader =
        FileReader::try_new(File::open(ipc.resource_path().unwrap()).unwrap(), None).unwrap();
    let resource_batch = resource_reader.into_iter().next().unwrap().unwrap();
    assert!(resource_batch.column_by_name("id").is_some());
    assert!(resource_batch.column_by_name("url").is_some());
}

#[test]
fn rejects_fields_not_present_in_the_initial_schema() {
    let packages = (0..101)
        .map(|index| {
            let mut package = serde_json::json!({"id": format!("dataset-{index}")});
            if index == 100 {
                package["new_field"] = serde_json::json!("requires a schema rule");
            }
            package
        })
        .collect::<Vec<_>>();

    let pages = vec![packages[..100].to_vec(), packages[100..].to_vec()];
    assert!(StructuredIpc::from_pages(pages).is_err());
}

#[test]
fn rejects_package_fields_outside_the_static_schema() {
    let packages = vec![serde_json::json!({
        "id": "dataset-1",
        "unrecognized_field": "requires an explicit schema rule"
    })];

    assert!(StructuredIpc::from_packages(packages).is_err());
}

#[test]
fn writes_groups_with_a_stable_struct_schema_across_pages() {
    let pages = vec![
        vec![serde_json::json!({"id": "dataset-1"})],
        vec![serde_json::json!({
            "id": "dataset-2",
            "groups": [{
                "description": "Health data",
                "display_name": "Health",
                "id": "a530bcef-1c72-4c9a-b8ce-7c4cdd85f2be",
                "image_display_url": "https://example.test/group.png",
                "name": "health",
                "title": "Health"
            }]
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("groups should have a stable schema");
    let reader = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    let groups = schema.field_with_name("groups").unwrap();
    let DataType::List(item) = groups.data_type() else {
        panic!("groups should be a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("groups should contain structs");
    };
    let names = fields.iter().map(|field| field.name()).collect::<Vec<_>>();
    assert_eq!(
        names,
        [
            "description",
            "display_name",
            "id",
            "image_display_url",
            "name",
            "title"
        ]
    );
    assert!(
        fields
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8)
    );
}

#[test]
fn writes_dataset_fields_with_a_stable_schema_across_pages() {
    let pages = vec![
        vec![serde_json::json!({"id": "dataset-1"})],
        vec![serde_json::json!({
            "id": "c96021c7-95d-4f9c-b530-ebb3f1f505f9",
            "author": "Diretoria Central de Transparência Ativa",
            "author_email": "transparencia@cge.mg.gov.br",
            "creator_user_id": "f405aa35-436e-4699-9928-24b3622f673e",
            "isopen": false,
            "license_id": "CC-BY-4.0",
            "license_title": "CC-BY-4.0",
            "maintainer": "",
            "maintainer_email": "",
            "metadata_created": "2022-06-26T14:52:01.610463",
            "metadata_modified": "2026-09-06T13:12:50.328392",
            "name": "proposta-lei-orcamentaria",
            "num_resources": 37,
            "num_tags": 1,
            "owner_org": "a26dd6c5-5760-482d-a610-08d6098271c6",
            "private": false,
            "state": "active",
            "title": "Proposta Orçamentária e Alteração Orçamentária",
            "type": "dataset",
            "url": "http://www.transparencia.mg.gov.br/planejamento-e-resultados/proposta-lei-orcamentaria",
            "version": ""
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("dataset fields should have a stable schema");
    let reader = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    for field in [
        "author",
        "author_email",
        "creator_user_id",
        "id",
        "isopen",
        "license_id",
        "license_title",
        "maintainer",
        "maintainer_email",
        "metadata_created",
        "metadata_modified",
        "name",
        "num_resources",
        "num_tags",
        "owner_org",
        "private",
        "state",
        "title",
        "type",
        "url",
        "version",
    ] {
        assert!(schema.field_with_name(field).is_ok(), "missing {field}");
    }
    assert_eq!(
        schema.field_with_name("num_resources").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("num_tags").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("isopen").unwrap().data_type(),
        &DataType::Boolean
    );
    assert_eq!(
        schema.field_with_name("private").unwrap().data_type(),
        &DataType::Boolean
    );
}

#[test]
fn writes_organization_with_a_stable_struct_schema_across_pages() {
    let pages = vec![
        vec![serde_json::json!({"id": "dataset-1"})],
        vec![serde_json::json!({
            "id": "dataset-2",
            "organization": {
                "id": "a26dd6c5-5760-482d-a610-08d6098271c6",
                "name": "controladoria-geral-do-estado-cge",
                "title": "Controladoria Geral do Estado - CGE",
                "type": "organization",
                "description": "Órgão central de controle interno",
                "image_url": "logo_gov.png",
                "created": "2020-07-24T14:08:24.024553",
                "is_organization": true,
                "approval_status": "approved",
                "state": "active"
            }
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("organization should have a stable schema");
    let reader = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    let organization = schema.field_with_name("organization").unwrap();
    let DataType::Struct(fields) = organization.data_type() else {
        panic!("organization should be a struct");
    };
    assert_eq!(
        fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
        [
            "id",
            "name",
            "title",
            "type",
            "description",
            "image_url",
            "created",
            "is_organization",
            "approval_status",
            "state"
        ]
    );
    assert_eq!(
        fields
            .iter()
            .find(|field| field.name() == "is_organization")
            .unwrap()
            .data_type(),
        &DataType::Boolean
    );
}

#[test]
fn writes_extras_as_a_stable_list_of_key_value_structs() {
    let pages = vec![
        vec![serde_json::json!({"id": "dataset-1"})],
        vec![serde_json::json!({
            "id": "dataset-2",
            "extras": [
                {"key": "tema", "value": "saude"},
                {"key": "fonte", "value": "CKAN"}
            ]
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("extras should be retained in the IPC");
    let reader = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    let extras = schema.field_with_name("extras").unwrap();
    let DataType::List(item) = extras.data_type() else {
        panic!("extras should be a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("extras should contain structs");
    };
    assert_eq!(
        fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
        ["key", "value"]
    );
    assert!(
        fields
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8)
    );
}

#[test]
fn writes_tags_as_a_stable_list_of_structs() {
    let pages = vec![
        vec![serde_json::json!({"id": "dataset-1"})],
        vec![serde_json::json!({
            "id": "dataset-2",
            "tags": [{
                "display_name": "licenciamento",
                "id": "84031112-f7ae-4f5f-865d-c6820b256ee9",
                "name": "licenciamento",
                "state": "active",
                "vocabulary_id": null
            }]
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("tags should have a stable schema");
    let reader = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    let tags = schema.field_with_name("tags").unwrap();
    let DataType::List(item) = tags.data_type() else {
        panic!("tags should be a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("tags should contain structs");
    };
    assert_eq!(
        fields.iter().map(|field| field.name()).collect::<Vec<_>>(),
        ["display_name", "id", "name", "state", "vocabulary_id"]
    );
    assert!(
        fields
            .iter()
            .all(|field| field.data_type() == &DataType::Utf8)
    );
}

#[test]
fn writes_resources_with_a_stable_schema_across_pages() {
    let pages = vec![
        vec![serde_json::json!({
            "id": "dataset-1",
            "resources": [{"id": "resource-1"}]
        })],
        vec![serde_json::json!({
            "id": "dataset-2",
            "resources": [{
                "cache_last_updated": null,
                "cache_url": null,
                "created": "2022-05-06T19:15:08.351208",
                "datastore_active": true,
                "datastore_contains_all_records_of_source_file": true,
                "description": "Dados das licenças",
                "format": "CSV",
                "hash": "8c97fef884026a074b1006338f59081b",
                "id": "4a065a30-0921-4c7f-9c5a-7032d7d783aa",
                "ignore_hash": true,
                "key": "20241129_edificacao.csv15-12-2024_00:08:52",
                "last_modified": "2022-05-06T19:15:08.217769",
                "metadata_modified": "2026-06-11T22:06:53.793201",
                "mimetype": "text/csv",
                "mimetype_inner": null,
                "name": "2021-03_Licencas.csv",
                "original_url": "https://example.test/original.csv",
                "package_id": "031929b9-89f5-451d-b050-1ac17a27ab8b",
                "position": 3,
                "resource_id": "4a065a30-0921-4c7f-9c5a-7032d7d783aa",
                "resource_type": null,
                "set_url_type": false,
                "size": 9042,
                "state": "active",
                "task_created": "2022-05-06 19:15:08.560637",
                "title": "20241129_edificacao.csv",
                "tries": null,
                "url": "https://example.test/data.csv",
                "url_type": "upload"
            }]
        })],
    ];

    let ipc = StructuredIpc::from_pages(pages).expect("resources should have a stable schema");
    let reader =
        FileReader::try_new(File::open(ipc.resource_path().unwrap()).unwrap(), None).unwrap();
    let batch = reader.into_iter().next().unwrap().unwrap();
    let schema = batch.schema();
    for field in [
        "cache_last_updated",
        "cache_url",
        "ckan_url",
        "created",
        "datastore_active",
        "datastore_contains_all_records_of_source_file",
        "description",
        "format",
        "hash",
        "id",
        "ignore_hash",
        "key",
        "last_modified",
        "metadata_modified",
        "mimetype",
        "mimetype_inner",
        "name",
        "original_url",
        "package_id",
        "position",
        "resource_id",
        "resource_type",
        "set_url_type",
        "size",
        "state",
        "task_created",
        "title",
        "tries",
        "url",
        "url_type",
    ] {
        assert!(schema.field_with_name(field).is_ok(), "missing {field}");
    }
    assert_eq!(
        schema.field_with_name("position").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("size").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema
            .field_with_name("datastore_active")
            .unwrap()
            .data_type(),
        &DataType::Boolean
    );
}

#[test]
fn fetch_writes_typed_ipcs() {
    let server = MockServer::start();
    let response = serde_json::json!({
        "result": [{
            "id": "dataset-1",
            "name": "dataset",
            "metadata_modified": "2025-01-01",
            "resources": [{
                "id": "resource-1",
                "package_id": "dataset-1",
                "last_modified": "2025-01-01",
                "url": "https://example.test/data.csv"
            }]
        }]
    });
    let mock = server.mock(|when, then| {
        when.method("GET")
            .path("/api/action/current_package_list_with_resources")
            .query_param("limit", PAGE_SIZE.to_string())
            .query_param("offset", "0");
        then.status(200).json_body(response);
    });
    let ipc = StructuredIpc::fetch(&server.url("")).expect("fetch should write IPC files");

    mock.assert();
    assert_eq!(ipc.package_rows(), 1);
    assert_eq!(ipc.resource_rows(), 1);
    let packages = FileReader::try_new(File::open(ipc.package_path()).unwrap(), None)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .unwrap();
    assert!(packages.column_by_name("id").is_some());
}
