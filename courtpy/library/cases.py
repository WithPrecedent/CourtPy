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

from utilities.pandas_tools import PandasTools

@dataclass
class Cases(PandasTools):
    
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
        self.rules = self.df_import(self.paths.case_rules,
                                    usecols = self.rules_cols,
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
    
    def check_list(self, variable):
        if isinstance(variable, list):
            return variable
        else:
            return [variable]  
        
#    def import_data(self):
#        if self.settings['general']['verbose']:
#            print('Importing data file(s)')
#        if self.settings['files']['test_data']:
#            nrows = self.settings['files']['test_chunk']
#        else:
#            nrows = None
#        if self.stage in ['wrangle', 'merge', 'engineer', 'analyze']:
#            self.df = self.df_import(self.paths.import_path, nrows = nrows) 
#        elif self.stage == 'plot':
#            self.bunch = {}
#            file_list = ['x', 'y', 'x_train', 'y_train']
#            if self.settings['analyzer']['val_size'] > 0:
#                file_list.extend(['x_val', 'y_val'])
#            for file in file_list:
#                file_name = file + '.' + self.file_format_in
#                import_path = os.path.join(self.paths.output, file_name)
#                self.bunch.update({file : self.df_import(import_path,
#                                                         nrows = nrows)})
#        return self
#    
#    def export_data(self):
#        if self.settings['general']['verbose']:
#            print('Exporting data file(s)')
#        if self.stage in ['wrangle', 'merge', 'engineer']:
#            self.df_export(self.df, export_path = self.paths.export_path)     
#        elif self.stage in ['analyze', 'plot']:
#            file_dict = {'x' : self.x, 'y' : self.y, 'x_train' : self.x_train, 
#                         'y_train' : self.y_train}
#            if self.settings['analyzer']['val_size'] > 0:
#                file_dict.update({'x_val' : self.x_val, 'y_val' : self.y_val})
#            for key, value in file_dict.items():
#                file_name = key + '.' + self.file_format_out
#                export_path = os.path.join(self.paths.output, file_name)
#                self.df_export(value, export_path = export_path)
#        return self   
  