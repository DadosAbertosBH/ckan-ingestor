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
from datetime import datetime

from pydantic import BaseModel


class MetadataSyncResponse(BaseModel):
    id: str
    instance_id: str
    instance_name: str | None = None
    start_time: datetime
    end_time: datetime | None = None
    status: str | None = None
    error_message: str | None = None
    total_packages: int = 0
    new_datasets: int = 0
    new_resources: int = 0
    updated_datasets: int = 0
    updated_resources: int = 0
    deleted_datasets: int = 0
    deleted_resources: int = 0
