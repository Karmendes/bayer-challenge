import sqlite3
from src.etl import Loaders


class LoadSQLite(Loaders):
    def __init__(self,db,table,mode,index = False):
        self.data = None
        self.conn = sqlite3.connect(db)
        self.table = table
        self.mode = mode
        self.index = index
    def load(self):
        self.data.to_sql(self.table, self.conn, if_exists=self.mode, index=self.index)