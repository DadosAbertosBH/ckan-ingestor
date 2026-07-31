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
from datetime import datetime

from pydantic import BaseModel


class CkanInstanceResponse(BaseModel):
    id: str
    name: str
    url: str
    last_metadata_synced: datetime | None = None
    dataset_count: int = 0
    resource_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
