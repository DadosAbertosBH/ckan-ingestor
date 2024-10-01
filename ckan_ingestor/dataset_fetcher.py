from abc import ABC, abstractmethod

class DatasetFetcher(ABC):

    @abstractmethod
    def fetch(self):
        pass
