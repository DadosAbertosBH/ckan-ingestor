# Pedalin
# Copyright (C) 2025  Pedalin

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
        # SQLAlchemy roundtrip
        from sqlalchemy.engine.url import make_url

        parsed = make_url(url)
        assert parsed.password == "P4ss!%W0rd+Test_?Extra@k&=end", (
            f"SQLAlchemy decode mismatch: {parsed.password}"
        )

    def test_configparser_does_not_choke(self):
        """configparser (used by alembic) must not raise on % in URL."""
        from configparser import ConfigParser

        settings = Settings(mysql_password="P4ss!%W0rd+Test_?Extra@k&=end")
        url = settings.database_url
        cp = ConfigParser()
        cp.add_section("alembic")
        # This must not raise ValueError about interpolation
        cp.set("alembic", "sqlalchemy.url", url)

    def test_empty_password(self):
        settings = Settings(mysql_password="")
        assert "mysql+aiomysql://root:@localhost:3306/ingestor_orchestrator" in (
            settings.database_url
        )
