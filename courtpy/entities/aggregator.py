
from dataclasses import dataclass

import pandas as pd

from .entity import Entity


@dataclass
class Aggregator(Entity):

    def __post_init__(self):
        pass
        return self

    def _count(self, variable):
        return len(variable)

    def _sum(self, variable):
        return sum(variable)

    def create_time_table(self, columns_dict):
        self.time_table = pd.DataFrame(columns = self.table_columns.keys(),
                                       index = range(self.start_year,
                                                     self._end_year),
                                       dtype = int)
        for year in range(self.start_year, self.end_year):
            year_value = 0
            for key, value in columns_dict.items():
                _method = getattr(self, '_' + value[0])
                _attribute = getattr(self, value[1])
                for class_key, class_value in _attribute.items():
                    if len(value) > 2:
                        year_value += _method(getattr(class_value,
                                                      value[2]).values())
                    else:
                        year_value += _method(getattr(_attribute).values())
                self.time_table[year, key] = year_value
        return self