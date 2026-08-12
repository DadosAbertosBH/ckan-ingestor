# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""Tests for DucklakeSettings URI construction."""

from ckan_ingestor.config.ducklake_settings import DucklakeSettings


class TestGetCatalogUri:
    def test_explicit_catalog_uri_wins(self):
        s = DucklakeSettings(catalog_uri="postgres:host=x dbname=test")
        assert s.get_catalog_uri() == "postgres:host=x dbname=test"

    def test_builds_from_individual_params(self, monkeypatch):
        monkeypatch.delenv("DUCKLAKE_CATALOG_URI", raising=False)
        s = DucklakeSettings(
            host="myhost",
            port="5432",
            dbname="mydb",
            username="myuser",
            password="mypass",
        )
        uri = s.get_catalog_uri()
        assert "host=myhost" in uri
        assert "port=5432" in uri
        assert "dbname=mydb" in uri
        assert "user=myuser" in uri
        assert "password=mypass" in uri
        assert uri.startswith("postgres:")

    def test_defaults_to_memory(self):
        s = DucklakeSettings()
        assert s.get_catalog_uri() == ":memory:"

    def test_catalog_uri_overrides_individual(self):
        s = DucklakeSettings(
            catalog_uri="postgres:host=explicit",
            host="ignored",
        )
        assert s.get_catalog_uri() == "postgres:host=explicit"
