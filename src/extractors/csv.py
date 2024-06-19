import pandas as pd
from src.etl import Extractors
class ExtractCSV(Extractors):
    def __init__(self,path):
        self.path = path
    def extract(self,sep = "::"):
        return pd.read_csv(self.path,sep=sep)