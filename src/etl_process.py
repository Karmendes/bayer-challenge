from src.etl import ETL
from src.logger.logger import Logger

class ETLProcessAmericas(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None
    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for Americas')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for Americas')