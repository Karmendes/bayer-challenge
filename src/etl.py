from abc import ABC,abstractmethod

class ETL(ABC):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
    @abstractmethod
    def extract(self):
        pass
    @abstractmethod
    def transform(self):
        pass
    @abstractmethod
    def load(self):
        pass
    @abstractmethod
    def run(self):
        pass

class Extractors(ABC):
    def __init__(self):
        pass
    @abstractmethod
    def extract(self):
        pass
class Loaders(ABC):
    def __init__(self):
        pass
    @abstractmethod
    def load(self):
        pass