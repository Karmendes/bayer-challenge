import pandas as pd
from src.etl import Extractors
class ExtractXLSX(Extractors):
    def __init__(self,path):
        self.path = path
    def extract(self):
        return pd.read_excel(self.path)