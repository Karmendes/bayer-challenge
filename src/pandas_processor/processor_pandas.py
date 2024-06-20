from abc import ABC,abstractmethod
from datetime import datetime
from unidecode import unidecode
import pandas as pd
import numpy as np


class Processor(ABC):
    @abstractmethod
    def process(self,data):
        pass

class ProcessData(Processor):
    def __init__(self,column,form = '%Y.%m'):
        self.column = column
        self.form = form
    def process(self,data):
        data['PERIOD'] = pd.to_datetime(data[self.column].astype(str),format=self.form)
        return data


class ProcessNanValues(Processor):
    def __init__(self,column):
        self.column = column
    def process(self,data):
        data[self.column] = data[self.column].replace(np.nan, None)
        return data
    
class ProcessFloatToString(Processor):
    def __init__(self,column):
        self.column = column
    def process(self, data):
        data[self.column] = data[self.column].astype(int).astype(str)
        return data

class ProcessReplacePattern(Processor):
    def __init__(self,column,new_pattern,old_pattern):
        self.column = column
        self.new_pattern = new_pattern
        self.old_pattern = old_pattern
    def process(self, data):
        data[self.column] = [x.replace(self.old_pattern,self.new_pattern) for x in data[self.column]]
        return data

class ProcessStringToUpper():
    def __init__(self,column):
        self.column = column
    def process(self,data):
        data[self.column] = data[self.column].str.upper()
        return data

class ProcessRemovePunctuation():
    def __init__(self,column):
        self.column = column
    def process(self,data):
        data[self.column] = [unidecode(x) for x in data[self.column]]
        return data

class ProcessAddTimestamp():
    def process(self,data):
        data['dh_extraction'] = datetime.now()
        return data
    
class ProcessRenameColumn():
    def __init__(self,dict_old_new):
        self.dict = dict_old_new
    def process(self,data):
        data.rename(columns=self.dict, inplace=True)
        return data

class ProcessRenameColumnFromDict():
    def __init__(self,dict_old_new):
        self.dict = dict_old_new
    def process(self,data):
        list_data = []
        for _,value in data.items():
            value.rename(columns=self.dict, inplace=True)
            list_data.append(value)
        return list_data

class ProcessReduceDataFrame():
    def process(self,data):
        data = pd.concat(data, ignore_index=True)
        return data

class ProcessGroupAndAggregate():
    def __init__(self,columns_to_group,dict_to_agg):
        self.columns = columns_to_group
        self.dict = dict_to_agg
    def process(self,data):
        data = data.groupby(self.columns).agg(self.dict).reset_index()
        return data

class ProcessCreateNewColumnFixed():
    def __init__(self,name_column,value):
        self.name_column = name_column
        self.value = value
    def process(self,data):
        data[self.name_column] = self.value
        return data

class ProcessTransformColumnforDate():
    def __init__(self,column,form = '%Y-%m-%d'):
        self.column = column
        self.format = form
    def process(self,data):
        data[self.column] = pd.to_datetime(data[self.column], format=self.format)
        return data

class ProcessTransformColumnDateinYear():
    def __init__(self,column):
        self.column = column
    def process(self,data):
        data[self.column] = data[self.column].dt.year
        return data

class ProcessDropNAColumn():
    def __init__(self,columns):
        self.columns = columns
    def process(self,data):
        data = data.dropna(subset=self.columns)
        return data

class ProcessJoinDataFrames():
    def __init__(self,on,base_df,how = 'left'):
        self.on = on
        self.how = how
        self.base_df = base_df
    def process(self,data:dict):
        merged_df = data[self.base_df]
        for key,_ in data.items():
            if key != self.base_df:
                merged_df = pd.merge(merged_df,
                     data[key],
                     on=self.on,
                     how=self.how)
        return merged_df

class ProcessRowBind():
    def __init__(self,names_dfs:list):
        self.names_dfs = names_dfs
    def process(self,data:dict[pd.DataFrame]):
        list_df = []
        for key,value in data.items():
            if key in self.names_dfs:
                list_df.append(value)
        data = pd.concat(list_df)
        return data


class ProcessSelectColumns():
    def __init__(self,columns):
        self.columns = columns
    def process(self,data):
        data = data[self.columns]
        return data


class ProcessorBySource():
    def __init__(self):
        self.process_list = []
    def add_process(self,processor):
        self.process_list.append(processor)
    def add_list_process(self,list_processor,before = True):
        if before:
            self.process_list = self.process_list + list_processor
        else:
            self.process_list = list_processor + self.process_list
    def run(self,data):
        for processor in self.process_list:
            data = processor.process(data)
        return data
        
