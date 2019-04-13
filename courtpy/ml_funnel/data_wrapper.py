"""
Parent class that contains decorator that either: 
    1) Receives a df and returns a df; or
    2) Doesn't receive a df, uses self.df, and returns self
"""
import functools
import pandas as pd

class FlexiblePanda(object):
    
    def default_df(self, _function):
       @functools.wraps(_function)
       def decorated(self, df = None, *args, **kwargs):
        not_df = False
        if not isinstance(df, pd.DataFrame):
            df = self.df
            not_df = True
        if not_df:
            self.df = df
            return self.df
        else: 
            return df