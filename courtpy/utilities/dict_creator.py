from dataclasses import dataclass
import pandas as pd

@dataclass
class CSVDict(dict):
    """
    Dictionary creator class for making basic dictionary types from .csv files.
    """    
    import_path : str
    dict_type : str = 'str_str'
    dedup : bool = False
    keys : str = 'keys'
    values : str = 'values'
    encoding : str = 'windows-1252'   
    
    def __post_init__(self):
        df = pd.read_csv(self.import_path, index_col = False, dtype = str, 
                         encoding = self.encoding)
        if self.dedup:
            df.drop_duplicates(subset = [self.keys], keep = 'first', 
                               inplace = True)
        if self.dict_type == 'str_str':
            self.lookup = df.set_index(self.keys).to_dict()[self.values]
        elif self.dict_type == 'str_list':
            df.set_index(self.keys)
            self.lookup = {}
            value = []
            for i, row in df.iterrows():
                value = row[1:].tolist()
                self.lookup.update({row[self.keys] : value})
        return self
        
    def __getitem__(self, key):
        return self.lookup[key]
        
    def __setitem__(self, key, value):
        self.lookup.update({key, value})
        return self