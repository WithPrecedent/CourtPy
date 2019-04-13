"""
Class for using vectorized string matching methods (when possible) across 
pandas dataframes and series with regular expressions and ordinary strings as 
keys.

ReMatch aims to simplify and speed up creating lookup tables 
(pseudo-dictionaries) for use with pandas dataframes and series. This can 
particularly help data scientists munging text data with keywords instead
of natural language processing. ReMatch also adds functionality by having 
"knot" matching. "Knots" (keyword-nots) are terms of exclusion such that 
their presence in a submitted string negate keyword matches.
    
Because the functions iterrate through lookup dataframes, any efficiency gains 
of vectorization in the target dataframes or series is lost with very long 
lookup dataframes. Further, because regular expressions cannot be hashed like 
ordinary dictionary keys, some of the speed advantages of dictionaries 
cannot be replicated. The tipping point for lookup dataframe length versus 
using apply or other non-vectorized options for matching varies and needs to be 
tested based upon the particular use case. The normal use case where ReMatch
has efficiency gains is with a very large dataframe and a relatively small
(< 500 rows) lookup dataframe.

ReMatch allows keys and values to be formed from pandas series, python lists,
or imported from a .csv file into a pandas dataframe.

Using the 'match' method, the user can either:
    1) Pass a string (within a pandas series or freestanding) to find a match 
    and store the result in a pandas series; 
        or;
    2) Pass a series (dataframe column) of strings and store the result in 
    one or more pandas series (dataframe column).

prefix and suffix parameters allow for iterables to be added to column or
index names in the dataframe.

Regular expressions are ordinarily used only as keys, but not values, unless
the reverse_dict option is set to True. Regular expressions may be compliled
with or without any flag selected.

If out_type is 'bool', new dataframe columns are created with headers 
derived from the values in the dictionary. A boolean value is returned.

if out_type is 'match', a single column is used or created with the header 
name passed in 'out_col.' The return is the matched values from the regular
expression lookup table.

If out_type is 'str', 'int' or 'float', a single column is used or created 
with the header name passed in 'out_col.' The return is the matched value of
the key in the lookup table.

If out_type is 'list', a single column is used or created with the 
header name passed in 'out_col.' The return is all matched patterns based 
upon a regular expression stored in a python list within each dataframe 
or series cell.

If knots are provided, the passed dictionary should be a dataframe with
the key and knot columns being strings or regular expressions. The
returned value must be boolean and will be True if the key is matched but 
the knot column is not matched. 
"""
from dataclasses import dataclass
import numpy as np
import pandas as pd
import re

from more_itertools import unique_everseen

@dataclass
class ReMatch(object):
    
    keys : str = 'keys' 
    values : str = 'values' 
    knots : str = 'knots'
    make_dict : bool = False
    reverse_dict : bool = False
    file_path : str = ''
    encoding : str = 'Windows-1252'  
    compile_keys : bool = True
    ignorecase : bool = False
    dotall : bool = False 
    locale : bool = False
    multiline : bool = False
    verbose : bool = False 
    ascii_flag : bool = False
    out_type : str = 'bool' 
    in_col : str = '' 
    in_prefix : str = '' 
    in_suffix : str = ''  
    out_col : str = '' 
    out_prefix : str = '' 
    out_suffix : str = ''
    convert_lists : bool = False
    default : str = ''
    
    def __post_init__(self):
        if self.in_col:
            self.in_col = (self.in_prefix + self.in_col + self.in_suffix)
        else:
            self.in_col = ''
        """
        Creates lookup table from either .csv or passed arguments.
        """
        if self.file_path:
            self.load_lookup()
        else:    
            self.build_lookup()
    
    def load_lookup(self):
        self.lookup = (pd.read_csv(self.file_path, 
                                   index_col = False, 
                                   encoding = self.encoding,
                                   true_values = ['y', 'Y', '1'],
                                   false_values = ['n', 'N', '0'])   
                         .astype(dtype = {self.keys : str}))
        self.make_lookup()
        return self
    
    def build_lookup(self):
        if isinstance(self.knots, pd.Series) or isinstance(self.knots, list):
            zip_data = list(zip(self.keys, self.values, self.knots))    
            self.lookup = pd.DataFrame(zip_data, columns = ['keys', 'values', 
                                                            'knots'])
        else:
            zip_data = list(zip(self.keys, self.values))      
            self.lookup = pd.DataFrame(zip_data, columns = ['keys', 'values'])
        self.keys = 'keys'
        self.values = 'values'
        self.knots = 'knots'
        if self.compile_keys:
            if self.ignorecase:
                self.lookup['IGNORECASE'] = True
            if self.dotall:
                self.lookup['DOTALL'] = True
            if self.locale:
                self.lookup['LOCALE'] = True
            if self.multiline:
                self.lookup['MULTILINE'] = True
            if self.verbose:
                self.lookup['VERBOSE'] = True
            if self.ascii_flag:
                self.lookup['ASCII'] = True
        self.make_lookup()
        return self
 
    def make_lookup(self):
        if self.out_type in ['bool', 'matches']:
            self.lookup[self.values] = (self.out_prefix 
                                        + self.lookup[self.values] 
                                        + self.out_suffix)
        elif self.out_type in ['list', 'str', 'int', 'float', 'match']:
            self.out_col = (self.out_prefix 
                            + self.out_col
                            + self.out_suffix)
        if self.compile_keys:    
            self.compile_lookup()
        if self.knots in self.lookup.columns:
            self.lookup = self.lookup[[self.keys, self.values, self.knots]]
        else:
            self.lookup = self.lookup[[self.keys, self.values]]
            if self.reverse_dict:
                self.lookup = (self.lookup.set_index(self.values)
                                          .to_dict()[self.keys])
        return self
    
    def compile_lookup(self):
        for i, row in self.lookup.iterrows():
            flags = self.compile_flags(row)
            if flags:
                self.lookup.loc[i, self.keys] = (
                    re.compile(self.lookup.loc[i, self.keys], flags = flags))
                if self.knots in self.lookup.columns:
                    self.lookup.loc[i, self.knots] = (
                        re.compile(self.lookup.loc[i, self.knots], 
                                   flags = flags))
            else:
                self.lookup.loc[i, self.keys] = (
                    re.compile(self.lookup.loc[i, self.keys]))
                if self.knots in self.lookup.columns:
                    self.lookup.loc[i, self.knots] = (
                        re.compile(self.lookup.loc[i, self.knots]))
        return self
    
    def compile_flags(self, row):
        flags = None
        first_flag = False
        if 'IGNORECASE' in row.index and row['IGNORECASE']:
            flags = re.IGNORECASE
            first_flag = True
        if 'DOTALL' in row.index and row['DOTALL']:
            if first_flag:
                flags |= re.DOTALL
            else:
                flags = re.DOTALL
                first_flag = True
        if 'LOCALE' in row.index and row['LOCALE']:
            if first_flag:
                flags |= re.LOCALE
            else:
                flags = re.LOCALE
                first_flag = True
        if 'MULTILINE' in row.index and row['MULTILINE']:
            if first_flag:
                flags |= re.MULTILINE
            else:
                flags = re.MULTILINE
                first_flag = True
        if 'VERBOSE' in row.index and row['VERBOSE']:
            if first_flag:
                flags |= re.VERBOSE
            else:
                flags = re.VERBOSE
                first_flag = True
        if 'ASCII' in row.index and row['ASCII']:
            if first_flag:
                flags |= re.ASCII
            else:
                flags = re.ASCII
        return flags
    
    def match(self, df, in_string = '', in_col = '', in_prefix = '', 
              in_suffix = '', out_col = '', out_prefix = '', out_suffix = '',
              default = ''):
        temp_list = []
        if in_col:
            self.in_col = in_col
        if in_prefix:
            self.in_prefix = in_prefix
        if in_suffix:
            self.in_suffix = in_suffix
        if out_col:
            self.out_col = out_col
        if out_prefix:
            self.out_prefix = out_prefix
        if out_suffix :
            self.out_suffix = out_suffix
        if in_col:
            self.in_col = self.complete_col_name(in_column = self.in_col)
        if out_col:
            self.out_col = self.complete_col_name(out_column = self.out_col)
        if isinstance(df, pd.Series):
            if self.in_col:
                self.in_string = df[self.in_col]
            else: 
                self.in_string = in_string
            if self.out_type == 'bool':
                for i, row in self.lookup.iterrows():
                    if self.knots in self.lookup.columns:
                        if (re.search(row[self.keys], self.in_string)
                                and not re.search(row[self.knots], 
                                                  self.in_string)):
                            df[row[self.values]] = True
                        else:
                            df[row[self.values]] = False
                    elif re.search(row[self.keys], self.in_string):
                        df[row[self.values]] = True
                    else:
                        df[row[self.values]] = False
            elif self.out_type in ['str', 'int', 'float']:
                for i, row in self.lookup.iterrows():
                    if re.search(row[self.keys], self.in_string):
                        df[self.out_col] = row[self.values] 
                        break
                    else:
                        df[self.out_col] = self.default
                if self.out_type == 'str' and df[self.out_col]:
                    df[self.out_col] = str(df[self.out_col])
                elif self.out_type == 'int' and df[self.out_col]:
                    df[self.out_col] = int(df[self.out_col])
                elif self.out_type == 'float' and df[self.out_col]:
                    df[self.out_col] = float(df[self.out_col])                   
            elif self.out_type == 'match':
                for i, row in self.lookup.iterrows():
                    if re.search(row[self.keys], self.in_string):
                        df[self.out_col] = row[self.values] 
                        break
            elif self.out_type == 'matches':
                for i, row in self.lookup.iterrows():
                    if re.search(row[self.keys], self.in_string):
                        df[row[self.values]] = (
                            re.search(row[self.keys], self.in_string).group(0))     
            elif self.out_type == 'list':
                for i, row in self.lookup.iterrows():
                    temp_list += re.findall(row[self.keys], self.in_string)               
                if temp_list:
                    temp_list = list(unique_everseen(temp_list))
                    if self.convert_lists:
                        temp_list = self.list_to_string(temp_list)
                    df[self.out_col] = temp_list
                else:    
                    df[self.out_col] = [default]  
        else:
            if self.out_col and not self.out_col in df.columns:
                df[self.out_col] = default
            if self.out_type == 'bool':
                for i, row in self.lookup.iterrows():
                    if self.knots in self.lookup.columns:
                        df[row[self.values]] = np.where(
                            (df[self.in_col].str.contains(row[self.keys]))
                            & ~(df[self.in_col].str.contains(row[self.knots])), 
                            True, False)   
                    else:    
                        df[row[self.values]] = (
                            np.where(df[self.in_col]
                                .str.contains(row[self.keys]), True, False))
            elif self.out_type in ['str', 'int', 'float']:
                for i, row in self.lookup.iterrows():
                    df[self.out_col] = (
                        np.where(df[self.in_col].str.contains(row[self.keys]), 
                                 row[self.values], df[self.out_col]))
                if self.out_type == 'str':
                    df[self.out_col].fillna('').astype(str)
                elif self.out_type == 'int':
                    df[self.out_col] = pd.to_numeric(df[self.out_col],
                                                     errors = 'coerce',
                                                     downcast = 'integer')
                elif self.out_type == 'float':
                    df[self.out_col] = pd.to_numeric(df[self.out_col],
                                                     errors = 'coerce',
                                                     downcast = 'float')  
            elif self.out_type == 'match':
                for i, row in self.lookup.iterrows():
                    df[self.out_col] = df[self.in_col].str.find(row[self.keys])
            elif self.out_type == 'matches':
                for i, row in self.lookup.iterrows():
                    df[row[self.values]] = (
                            df[self.in_col].str.findall(row[self.keys]))
            elif self.out_type == 'list':
                df[self.out_col] = np.empty((len(df), 0)).tolist()
                df = df.apply(self.df_matches, axis = 'columns')
        return df  
    
    def df_matches(self, df_row):
        temp_list = []
        for i, row in self.lookup.iterrows():
            temp_list += re.findall(row[self.keys], df_row[self.in_col])
        if temp_list:
            temp_list = list(unique_everseen(temp_list))  
            df_row[self.out_col].extend(temp_list)
        return df_row
    
    def list_to_string(i_list):
        return ', '.join(i_list)
    
    def complete_col_name(self, in_column = '', out_column = ''):
        if in_column:
            return self.in_prefix + in_column + self.in_suffix
        if out_column:
            return self.out_prefix + out_column + self.out_suffix
