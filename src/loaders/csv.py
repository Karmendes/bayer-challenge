from src.etl import Loaders


class LoadCSV(Loaders):
    def __init__(self,path):
        self.data = None
        self.path = path
    def load(self):
        self.data.to_csv(self.path,index = False)

class LoadCSVs(Loaders):
    def __init__(self,paths:dict):
        self.data = None
        self.paths = paths
    def load(self):
        for key,value in self.paths.items():
            self.data[key].to_csv(value,index = False)
