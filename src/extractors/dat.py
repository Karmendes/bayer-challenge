import pandas as pd
from src.etl import Extractors

class ExtractDat(Extractors):
    def __init__(self,path):
        self.path = path
    def extract(self,sep = "::"):
        return pd.read_table(self.path,sep=sep, engine='python')