from abc import ABC, abstractmethod

import pyarrow


class DatasetFetcher(ABC):

    def fetch(self) -> pyarrow.Table:
        dataset = self.do_fetch()
        return DatasetFetcher.to_arrow(dataset)

    @abstractmethod
    def do_fetch(self) -> list[dict[str, any]]:
        pass

    @staticmethod
    def to_arrow(dataset: list[dict[str, any]]) -> pyarrow.Table:
        return pyarrow.Table.from_pylist(dataset)