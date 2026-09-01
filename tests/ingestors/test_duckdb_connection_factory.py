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
from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_connection_factory import from_settings


def test_from_settings_idempotent():
    """Testa se executar from_settings duas vezes não gera erro de attach duplicado."""
    settings = DucklakeSettings()  # Use valores padrão ou mock conforme necessário
    conn1 = from_settings(settings)
    conn2 = from_settings(settings)

    conn1.close()
    conn2.close()
