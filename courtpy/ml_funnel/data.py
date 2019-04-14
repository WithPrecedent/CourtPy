"""
Class for organizing and loading data into machine learning algorithms.
"""
from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass 
class Data(object):
    
    settings : object = None
    df : object = None
    x : object = None
    y : object = None
    x_train : object = None
    y_train : object = None
    x_test : object = None
    y_test : object = None
    x_val : object = None
    y_val : object = None
    quick_start : bool = False
    filer : object = None
    
    def __post_init__(self):
        self.verbose = self.settings['general']['verbose']
        self.seed = self.settings['general']['seed']
        if self.verbose:
            print('Building data container')
        self.splice_options = {}
        if self.quick_start:
            self.load(import_path = self.filer.data_file_in,
                      file_format = self.settings['files']['data_in'],
                      test_data = self.settings['files']['test_data'],
                      test_rows = self.settings['files']['test_chunk'],
                      encoding = self.settings['files']['encoding']) 
        self.dropped_columns = []
        return self
             
    def _get_xy(self, data_to_use):
        options = {'full' : [self.x, self.y],
                   'train' : [self.x_train, self.y_train],
                   'test' : [self.x_test, self.y_test],
                   'val' : [self.x_val, self.y_val]}
        return options[data_to_use]
    
    def apply(self, df = None, func = None, **kwargs):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if func:
            df = func(df, **kwargs)
        if not_df:
            self.df = df
            return self
        else: 
            return df

    def column_types(self, df = None, cat_cols = [], cat_prefixes = [], 
                     float_cols = [], float_prefixes = [],
                     int_cols = [], int_prefixes = [],
                     bool_cols = [], bool_prefixes = [],
                     interact_cols = [], interact_prefixes = [],
                     list_cols = [], list_prefixes = [],
                     str_cols = [], str_prefixes = []):
        if not isinstance(df, pd.DataFrame):
            df = self.df
        self.bool_cols = self.create_column_list(df = df, 
                                                 prefixes = bool_prefixes, 
                                                 cols = bool_cols) 
        self.cat_cols = self.create_column_list(df = df, 
                                                prefixes = cat_prefixes, 
                                                cols = cat_cols) 
        self.float_cols = self.create_column_list(df = df, 
                                                  prefixes = float_prefixes, 
                                                  cols = float_cols) 
        self.int_cols = self.create_column_list(df = df, 
                                                prefixes = int_prefixes, 
                                                cols = int_cols) 
        self.interact_cols = self.create_column_list(df = df, 
                                                     prefixes = interact_prefixes, 
                                                     cols = interact_cols)
        self.list_cols = self.create_column_list(df = df, 
                                                 prefixes = list_prefixes, 
                                                 cols = list_cols)
        self.str_cols = self.create_column_list(df = df, 
                                                prefixes = str_prefixes, 
                                                cols = str_cols)
        self.num_cols = self.float_cols + self.int_cols
        self.all_cols = df.columns
        self.column_dict = dict.fromkeys(self.bool_cols, bool)
        self.column_dict.update(dict.fromkeys(self.cat_cols, 'category'))
        self.column_dict.update(dict.fromkeys(self.float_cols, float))
        self.column_dict.update(dict.fromkeys(self.int_cols, int))
        self.column_dict.update(dict.fromkeys(self.interact_cols, 'category'))
#        self.column_dict.update(dict.fromkeys(self.list_cols, list))
        self.column_dict.update(dict.fromkeys(self.str_cols, str))
        return self
    
    def create_column_list(self, df = None, prefixes = [], cols = []):
        if not isinstance(df, pd.DataFrame):
            df = self.df
        temp_list = []
        prefixes_list = []
        for prefix in prefixes:
            temp_list = [x for x in df if x.startswith(prefix)]
            prefixes_list.extend(temp_list)
        col_list = cols + prefixes_list 
        return col_list 
    
    def initialize_series(self, df = None, column_dict = None):
        """
        Initializes values in multi-type series to defaults based upon the 
        datatype listed in the columns dictionary.
        """
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        for key, value in column_dict.items():
            if column_dict[key] == bool:
                df[key] = False
            if column_dict[key] == int:
                df[key] = 0
            if column_dict[key] == list:
                df[key] = []
            if column_dict[key] == str:
                df[key] = '' 
            if column_dict[key] == float:
                df[key] = np.nan
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def downcast(self, df = None, ints = [], floats = [], cats = []):
        """
        Method to decrease memory usage by downcasting datatypes.
        """
        print('Downcasting data to decrease memory usage')
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if not ints:
            ints = self.int_cols
        if not floats:
            floats = self.float_cols
        if not cats:
            cats = self.cat_cols
        for col in ints:
            if min(df[col] >= 0):
                df[col] = pd.to_numeric(df[col], downcast = 'unsigned')
            else:
                df[col] = pd.to_numeric(df[col], downcast = 'integer')
        for col in floats:
            df[col] = pd.to_numeric(df[col], downcast = 'float')
        for col in cats:
            df[col] = df[col].astype('category')
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def smart_fill_na(self, df = None):
        """
        Fills na values in dataframe to defaults based upon the 
        datatype listed in the columns dictionary.
        """
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Replacing empty cells with default values')
        if self.column_dict:
            for col, col_type in self.column_dict.items():
                if col in df.columns:
                    if col_type == bool:
                        df[col].fillna(False, inplace = True)
                        df[col].astype(bool, inplace = True)
                    elif col_type == int:
                        df[col].fillna(0, inplace = True, downcast = int)
                    elif col_type == float:
                        df[col].fillna(0.0, inplace = True, downcast  = int)
                    elif col_type == list:
                        df[col].fillna([''], inplace = True)
                    elif col_type == str:
                        df[col].fillna('', inplace = True)
                        df[col].astype(str, inplace = True)
                    elif col_type == 'category':
                        df[col].fillna('', inplace = True)
                        df[col].astype('category', inplace = True)
        else:
            for col in df.columns:
                if df[col].dtype == bool:
                    df[col].fillna(False, inplace = True)
                elif df[col].dtype == int:
                    df[col].fillna(0, inplace = True, downcast = int)
                elif df[col].dtype == float:
                    df[col].fillna(0, inplace = True, downcast  = int)
                elif df[col].dtype == list:
                    df[col].fillna('', inplace = True)
                elif df[col].dtype == str:
                    df[col].fillna('', inplace = True)
                elif df[col].dtype == 'category':
                    df[col].fillna('', inplace = True) 
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def convert_rare_categories(self, df = None, cats = [], threshold = 0):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Replacing infrequently appearing categories')   
        for col in cats:
            df['value_freq'] = df[col].value_counts() / len(df[col])
            df[col] = np.where(df['value_freq'] <= threshold, '', df[col]) 
        df.drop('value_freq', 
                axis = 'columns', 
                inplace = True)
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def drop_infrequent_cols(self, df = None, bools = [], threshold = 0):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Removing boolean variables with low variability')
        for col in bools:
            if df[col].mean() < threshold:
                df.drop(col, 
                        axis = 'columns', 
                        inplace = True)
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def drop_highly_correlated_cols(self, df = None, cols = [], 
                                    threshold = 0.95):
        """
        Drops all but one column from highly correlated groups of columns.
        """
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Removing highly correlated columns')
        for col in cols:
            corr_matrix = df.corr().abs()
            upper = corr_matrix.where(np.triu(np.ones(
                    corr_matrix.shape), k = 1).astype(np.bool))
        drop_cols = [c for c in upper.columns if any(upper[c] > threshold)]
        df.drop(drop_cols,
                axis = 'columns', 
                inplace = True)
        if not_df:
            self.df = df
            return self
        else: 
            return df
        
    def reshape_long(self, df = None, stubs = [], id_col = '', new_col = '', 
                     sep = ''):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Reshaping data to long format')
        df = (pd.wide_to_long(df, 
                              stubnames = stubs, 
                              i = id_col, 
                              j = new_col, 
                              sep = sep).reset_index())
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def reshape_wide(self, df = None, df_index = '', cols = [], values = []):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if self.verbose:
            print('Reshaping data to wide format')
        df = (df.pivot(index = df_index, 
                       columns = cols, 
                       values = values).reset_index())
        if not_df:
            self.df = df
            return self
        else: 
            return df
    
    def summarize(self, df = None, export_path = ''):
        if not isinstance(df, pd.DataFrame):
            df = self.df
        summary_cols = ['variable', 'data_type', 'count', 'min', 'q1', 
                        'median', 'q3', 'max', 'mad', 'mean', 'stan_dev', 
                        'mode', 'sum']
        self.summary = pd.DataFrame(columns = summary_cols)
        for i, col in enumerate(df.columns):
            new_row = pd.Series(index = summary_cols)
            new_row['variable'] = col
            new_row['data_type'] = df[col].dtype
            new_row['count'] = len(df[col])
            if df[col].dtype == bool:
                df[col] = df[col].astype(int)
            if df[col].dtype.kind in 'bifcu':
                new_row['min'] = df[col].min()
                new_row['q1'] = df[col].quantile(0.25)
                new_row['median'] = df[col].median()
                new_row['q3'] = df[col].quantile(0.75)
                new_row['max'] = df[col].max()
                new_row['mad'] = df[col].mad()
                new_row['mean'] = df[col].mean()
                new_row['stan_dev'] = df[col].std()
                new_row['mode'] = df[col].mode()[0]
                new_row['sum'] = df[col].sum()
            self.summary.loc[len(self.summary)] = new_row
        self.summary.sort_values('variable', inplace = True)
        if export_path:
            self.save(df = self.summary, 
                      export_path = export_path, 
                      file_format = 'csv')          
        return self
    
    def split_xy(self, df = None, label = 'label'):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        x = df.drop(label, axis = 'columns')
        y = df[label] 
        if not_df:
            self.x = x
            self.y = y
            return self
        else: 
            return x, y
        
    def add_splice(self, group_name, prefixes = [], columns = []):
        temp_list = self.create_column_list(prefixes = prefixes, 
                                            cols = columns)
        self.splice_options.update({group_name : temp_list})
        return self
                
    def load(self, import_folder = '', file_name = 'data', import_path = '',
             file_format = 'csv', usecols = None, index = False, 
             encoding = 'windows-1252', test_data = False, test_rows = 500, 
             return_df = False, message = 'Importing data'):
        """
        Method to import pandas dataframes from different file formats.
        """
        if not import_path:
            if not import_folder:
                import_folder = self.filer.import_folder
            import_path = self.filer.make_path(folder = import_folder,
                                               file_name = file_name,
                                               file_type = file_format)
        if self.verbose:
            print(message)
        if test_data:
            nrows = test_rows
        else:
            nrows = None
        if file_format == 'csv':
            df = pd.read_csv(import_path, 
                             index_col = index,
                             nrows = nrows,
                             usecols = usecols,
                             encoding = encoding,
                             low_memory = False)
            """
            Removes a common encoding error character from the dataframe.
            """
            df.replace('Â', '', inplace = True)
        elif file_format == 'h5':
            df = pd.read_hdf(import_path, 
                             chunksize = nrows)
        elif file_format == 'feather':
            df = pd.read_feather(import_path, 
                                 nthreads = -1)
        if not return_df:
            self.df = df
            return self
        else: 
            return df
    
    def save(self, df = None, export_folder = '', file_name = 'data', 
             export_path = '', file_format = 'csv', index = False, 
             encoding = 'windows-1252', float_format = '%.4f', 
             boolean_out = True, message = 'Exporting data'):
        """
        Method to export pandas dataframes to different file formats and 
        encoding of boolean variables as True/False or 1/0
        """
        if not export_path:
            if not export_folder:
                export_folder = self.filer.export_folder    
            export_path = self.filer.make_path(folder = export_folder,
                                               name = file_name,
                                               file_type = file_format)
        if not isinstance(df, pd.DataFrame):
            df = self.df
        if self.verbose:
            print(message)
        if not boolean_out:
            df.replace({True : 1, False : 0}, inplace = True)
        if file_format == 'csv':
            df.to_csv(export_path, 
                      encoding = encoding,
                      index = index,
                      header = True,
                      float_format = float_format)
        elif file_format == 'h5':
            df.to_hdf(export_path)
        elif file_format == 'feather':
            if isinstance(df, pd.DataFrame):
                df.reset_index(inplace = True)
                df.to_feather(export_path)
        return