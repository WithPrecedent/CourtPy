
from dataclasses import dataclass

import pandas as pd


@dataclass
class Entity(object):

    def __post_init__(self):
        pass
        return self

    def _check_df(self, attribute):
        if hasattr(self, attribute):
            return getattr(self, attribute)
        else:
            return pd.DataFrame

    def _check_dict(self, attribute):
        if hasattr(self, attribute):
            return getattr(self, attribute)
        else:
            return {}

    def _check_list(self, attribute):
        if hasattr(self, attribute):
            return getattr(self, attribute)
        else:
            return []

    def _check_series(self, attribute):
        if hasattr(self, attribute):
            return getattr(self, attribute)
        else:
            return pd.Series

    def _check_year(self, year, start_buffer = 0, end_buffer = 0):
        if ((self.start_year - start_buffer)
                < year
                < (self.end_year + end_buffer)):
            return True
        else:
            error = year + ' is out of range for' + self.name
            raise KeyError(error)

    def add_column(self, name, method, class_dict, subclass_dict):
        self.table_columns.update({name : [method, class_dict, subclass_dict]})
        return self