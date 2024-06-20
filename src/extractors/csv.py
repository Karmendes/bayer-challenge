import pandas as pd
from src.etl import Extractors
class ExtractCSV(Extractors):
    def __init__(self,path):
        self.path = path
    def extract(self,sep = ","):
        return pd.read_csv(self.path,sep=sep)
    
class ExtractCSVs(Extractors):
    def __init__(self,paths:dict):
        self.paths = paths
    def extract(self,sep = ","):
        data = {}
        for key,value in self.paths.items():
            data[key] = pd.read_csv(value,sep=sep)
        return data
