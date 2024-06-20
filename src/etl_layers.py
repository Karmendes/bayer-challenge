from src.etl import ETL
from src.logger.logger import Logger
from src.factory.factory_bronze import dict_processors
from src.factory.factory_silver import factory
from src.factory.factory_gold import factory as factory_gold


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
        processor = dict_processors['americas']
        self.data = processor.run(self.data) 
        
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

class ETLProcessEMEA(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None

    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        processor = dict_processors['emea']
        self.data = processor.run(self.data) 
        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for EMEA')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for EMEA')

class ETLProcessAsia(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None

    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        processor = dict_processors['asia']
        self.data = processor.run(self.data) 
        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for Asia')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for Asia')

class ETLProcessForecast(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None

    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        processor = dict_processors['forecast']
        self.data = processor.run(self.data) 
        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for Forecasting')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for Forecasting')

class ETLProcessAggregation(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None

    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        for key,_ in self.data.items():
            processor = factory[key]
            self.data[key] = processor.run(self.data[key])  
        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for Aggregation')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for Aggreagation')

class ETLProcessJoining(ETL):
    def __init__(self,extractor,loader):
        self.extractor = extractor
        self.loader = loader
        self.data = None

    def extract(self):
        Logger.emit('Extracting data')
        self.data = self.extractor.extract()
    
    def transform(self):
        Logger.emit('Transforming data')
        processor_bin = factory_gold['binding']
        processor_join = factory_gold['joining']
        data_bin = processor_bin.run(self.data)
        data_fin = {'forecast':self.data['forecast'],'bin':data_bin}
        self.data = processor_join.run(data_fin)

        
    def load(self):
        Logger.emit('Loading data')
        self.loader.data = self.data
        self.loader.load()
    
    def run(self):
        Logger.emit('Starting process for Joining')
        self.extract()
        self.transform()
        self.load()
        Logger.emit('Ending process for Joining')