"""
General functions for parsing and manipulating strings.
"""
import pandas as pd
import re

"""
Removes excess text included when parsing text into sections and strips text.
Takes either string, pandas series, or pandas dataframe as input and returns 
the same. 
"""
def remove_excess(in_var, excess, in_col = ''):
    if isinstance(in_var, pd.DataFrame):
        in_var[in_col].str.replace(excess, '')
        in_var[in_col].str.strip()
    elif isinstance(in_var, pd.Series):
        in_var.str.replace(excess, '')
        in_var.str.strip()
    else:
        in_var = re.sub(excess, '', in_var)
        in_var = in_var.strip()
    return in_var

"""
Removes line breaks and replaces them with single spaces. Also, removes
hyphens at the end of a line and connects the surrounding text. Takes either
string, pandas series, or pandas dataframe as input and returns the same.
"""   
def no_breaks(in_var, in_col = ''):
    if isinstance(in_var, pd.DataFrame):
        in_var[in_col].str.replace('[a-z]-\n', '')
        in_var[in_col].str.replace('\n', ' ')
    elif isinstance(in_var, pd.Series):
        in_var.str.replace('[a-z]-\n', '')
        in_var.str.replace('\n', ' ')
    else:
        in_var = re.sub('[a-z]-\n', '', in_var)
        in_var = re.sub('\n', ' ', in_var)
    return in_var

"""
Removes double spaces and replaces them with single spaces from a string.
Takes either string, pandas series, or pandas dataframe as input and returns 
the same.
"""    
def no_double_space(in_var, in_col = ''):
    if isinstance(in_var, pd.DataFrame):
        in_var[in_col].str.replace('  +', ' ')
    elif isinstance(in_var, pd.Series):
        in_var.str.replace('  +', ' ')
    else:
        in_var = in_var.replace('  +', ' ')
    return in_var

def list_to_string(in_value):
    if isinstance(in_value, pd.Series):
        out_value = in_value.apply(', '.join)
    elif isinstance(in_value, list):
        out_value = ', '.join(in_value)
    else:
        msg = 'Value must be a list or pandas series containing lists'
        raise TypeError(msg)
        out_value = in_value
    return out_value

def word_count(i_string):
    words = i_string.split(' ')
    value = len(words)
    return value
