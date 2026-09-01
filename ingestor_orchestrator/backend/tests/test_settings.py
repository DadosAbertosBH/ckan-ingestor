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
"""Tests for Settings — environment variables and URL generation."""

from ingestor_orchestrator.config import Settings


class TestDatabaseUrl:
    """database_url must URL-encode special characters in the password."""

    def test_simple_alphanumeric_password(self):
        settings = Settings(mysql_password="abc123")
        assert "abc123" in settings.database_url
        assert "@" in settings.database_url

    def test_special_chars_password(self):
        settings = Settings(mysql_password="P4ss!%W0rd+Test_?Extra@k&=end")
        url = settings.database_url
        before_host = url.split("@")[0]
        assert "@" not in before_host, f"Unencoded @ in password: {url}"
        assert "!" not in url, f"Unencoded ! in password: {url}"
        assert "?" not in url, f"Unencoded ? in password: {url}"
        # SQLAlchemy roundtrip via create_engine (not configparser)
        from sqlalchemy import create_engine

        engine = create_engine(url, connect_args={"connect_timeout": 1})
        assert engine.url.password == "P4ss!%W0rd+Test_?Extra@k&=end", (
            f"SQLAlchemy decode mismatch: {engine.url.password}"
        )

    def test_empty_password(self):
        settings = Settings(mysql_password="")
        assert "mysql+aiomysql://root:@localhost:3306/ingestor_orchestrator" in (
            settings.database_url
        )


class TestIggyConnectionString:
    def test_credentials_are_url_encoded(self):
        settings = Settings(
            iggy_address="iggy:8090",
            iggy_username="iggy",
            iggy_password="password*with*asterisk",
        )

        assert settings.iggy_connection_string == (
            "iggy+tcp://iggy:password*with*asterisk@iggy:8090"
        )
