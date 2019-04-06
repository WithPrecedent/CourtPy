"""
Container for case data, a subclass of generic Bunch, which contains various
methods for feature engineering, splitting, encoding, sampling, and otherwise
preparing data for machine learning.

Cases stores the key attributes of the court opinion data for use throughout
the CourtPy pipeline. Edits to the dictionaries and lists here will change
the functionality of methods and functions throughout the CourtPy package.
"""
from dataclasses import dataclass
import os
import pandas as pd

@dataclass
class Cases(object):
    
    paths : object
    settings : object
    stage : str = ''
    source : str = ''  

    def __post_init__(self):
        self.make_rules()
        return
    
    def make_rules(self):
        self.rules_cols = {'key' : str, 'data_type' : str, 
                           'lexis_nexis' : bool, 'court_listener' : bool,
                           'caselaw_access' : bool, 'divide_source' : str,
                           'munge_stage' : str, 'munge_type' : str, 
                           'munge_file' : str, 'munge_source' : str, 
                           'combiner' : bool, 'external' : bool,
                           'scale' : bool, 'drop' : bool, 'encoder' : str}
        
        self.rules = pd.read_csv(self.paths.case_rules, 
                                 usecols = self.rules_cols,
                                 encoding = self.settings['files']['encoding'],
                                 file_format = 'csv')
        if self.source == 'lexis_nexis':
            self.index_col = 'index_lexis'  
            self.meta_cols = {'file_name' : str, 'word_count' : int} 
            self.dividers_file = os.path.join(self.paths.dicts, 
                                              'lexis_dividers.csv')
        if self.source == 'court_listener':
            self.index_col = 'index_court_listener'
            self.meta_cols = {'file_name' : str, 'word_count' : int,
                              'cl_url' : str, 'cl_id' : str}
            self.dividers_file = os.path.join(self.paths.dicts, 
                                              'cl_dividers.csv')
            self.import_cols = {'resource_uri' : str, 'id' : int, 
                                'author' : str, 'plain_text' : str}  
            self.rename_cols = {'resource_uri' :'cl_url',
                                'id' : 'cl_id', 
                                'plain_text' : 'opinion'}
        if self.source == 'caselaw_access':
            self.index_col = 'index_caselaw_access'
            self.meta_cols = {'file_name' : str, 'word_count' : int}
            self.dividers_file = os.path.join(self.paths.dicts, 
                                              'ca_dividers.csv')
        if self.stage in ['parse', 'wrangle', 'merge']:
            self.bool_prefixes = (self.rules.loc[self.rules['data_type'] 
                                  == 'bool']['key'].tolist())
            self.float_prefixes = (self.rules.loc[self.rules['data_type'] 
                                   == 'float']['key'].tolist())
            self.int_prefixes = (self.rules.loc[self.rules['data_type'] 
                                 == 'int']['key'].tolist())        
            self.list_prefixes = (self.rules.loc[self.rules['data_type'] 
                                  == 'list']['key'].tolist())   
            self.str_prefixes = (self.rules.loc[self.rules['data_type'] 
                                 == 'str']['key'].tolist())   
            self.str_prefixes.append('sec')
        elif self.stage in ['engineer', 'analyze', 'plot']:
            self.bool_prefixes = (self.rules.loc[self.rules['encoder'] 
                                  == 'bool']['key'].tolist())
            self.cat_prefixes = (self.rules.loc[self.rules['encoder'] 
                                 == 'categorical']['key'].tolist())
            self.float_prefixes = (self.rules.loc[self.rules['encoder'] 
                                   == 'float']['key'].tolist())
            self.int_prefixes = (self.rules.loc[self.rules['encoder'] 
                                 == 'int']['key'].tolist())
            self.interact_prefixes = (self.rules.loc[self.rules['encoder'] 
                                      == 'interact']['key'].tolist())
            self.drop_prefixes = (
                    self.rules.loc[self.rules['drop']]['key'].tolist())
        return self
  