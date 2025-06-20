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
import pyarrow
import pyarrow as pa

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class FakeCkanDatasetFetcher(DatasetFetcher):
    dataset: pa.Table

    def __init__(self, dataset: pyarrow.Table):
        self.dataset = dataset

    def fetch(self):
        return self.dataset
