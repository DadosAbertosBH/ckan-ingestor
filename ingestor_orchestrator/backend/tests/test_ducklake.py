# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

import duckdb

from ingestor_orchestrator import ducklake
from ingestor_orchestrator.ducklake import DucklakeSettings, get_outdated_resource_ids


def test_explicit_catalog_uri_wins_over_individual_settings():
    settings = DucklakeSettings(
        catalog_uri="postgres:host=explicit",
        host="ignored",
    )

    assert settings.get_catalog_uri() == "postgres:host=explicit"


def test_catalog_uri_is_built_from_individual_settings():
    settings = DucklakeSettings(
        host="myhost",
        port="5432",
        dbname="mydb",
        username="myuser",
        password="mypass",
    )

    assert settings.get_catalog_uri() == (
        "postgres:host=myhost port=5432 dbname=mydb user=myuser password=mypass"
    )


def test_catalog_uri_defaults_to_memory():
    assert DucklakeSettings().get_catalog_uri() == ":memory:"


def test_from_settings_can_be_called_repeatedly(monkeypatch):
    class Connection:
        def __init__(self):
            self.statements = []

        def install_extension(self, extension):
            self.statements.append(f"install {extension}")

        def load_extension(self, extension):
            self.statements.append(f"load {extension}")

        def execute(self, statement):
            self.statements.append(statement)

    connections = []

    def connect(*_args, **_kwargs):
        connection = Connection()
        connections.append(connection)
        return connection

    monkeypatch.setattr(ducklake.duckdb, "connect", connect)

    ducklake.from_settings(DucklakeSettings())
    ducklake.from_settings(DucklakeSettings())

    assert len(connections) == 2
    assert all(
        any("ATTACH IF NOT EXISTS" in statement for statement in connection.statements)
        for connection in connections
    )


def test_get_outdated_resource_ids_filters_processed_and_instance_rows():
    conn = duckdb.connect(":memory:")
    conn.execute(
        "CREATE TABLE ckan_resource "
        "(id VARCHAR, last_modified VARCHAR, ckan_url VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE ckan_resource_last_update "
        "(ckan_resource_id VARCHAR, last_modified TIMESTAMP)"
    )
    conn.execute(
        "INSERT INTO ckan_resource VALUES "
        "('outdated', '2024-01-01', 'https://one.example'), "
        "('current', '2024-01-02', 'https://one.example'), "
        "('never-run', '2024-01-03', 'https://one.example'), "
        "('other', '2024-01-03', 'https://other.example')"
    )
    conn.execute(
        "INSERT INTO ckan_resource_last_update VALUES "
        "('outdated', '2023-12-31'), ('current', '2025-01-01')"
    )

    result = get_outdated_resource_ids(conn, "https://one.example")

    assert set(result) == {"outdated", "never-run"}
