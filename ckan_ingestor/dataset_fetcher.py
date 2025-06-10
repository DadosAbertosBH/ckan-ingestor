from abc import ABC, abstractmethod

import pyarrow


class DatasetFetcher(ABC):

    @abstractmethod
    def fetch(self) -> pyarrow.Table:
        pass