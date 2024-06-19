from src.etl import Loaders


class LoadCSV(Loaders):
    def __init__(self,path):
        self.data = None
        self.path = path
    def load(self):
        self.data.to_csv(self.path,index = False)
