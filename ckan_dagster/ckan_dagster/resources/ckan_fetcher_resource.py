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
import os

import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanFetcherResource(dg.ConfigurableResource[DatasetFetcher]):
    def create_resource(self, context: InitResourceContext) -> DatasetFetcher:
        return CkanDatasetFetcher(
            os.environ.get("CKAN_URL", "https://dados.pbh.gov.br/")
        )
