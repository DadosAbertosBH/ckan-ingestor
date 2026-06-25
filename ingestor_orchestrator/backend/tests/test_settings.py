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
        settings = Settings(mysql_password="4KV!%R{1y%1+U5V_?z7h8_Uoqe@k&=sm")
        url = settings.database_url
        assert "@" in url
        assert "!" not in url, f"Special char '!' should be encoded, got: {url}"
        assert "%" not in url.replace("%40", "").replace("%25", "").replace(
            "%21", ""
        ), f"All remaining '%' should be from other encoded chars, got: {url}"
        assert "?" not in url, f"Special char '?' should be encoded, got: {url}"

    def test_empty_password(self):
        settings = Settings(mysql_password="")
        assert "mysql+aiomysql://root:@localhost:3306/ingestor_orchestrator" in (
            settings.database_url
        )
