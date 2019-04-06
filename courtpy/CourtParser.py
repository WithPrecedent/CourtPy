"""
Primary class for parsing court opinions. This part of the CourtPy pipeline
divides the court opinions into the key sections, extracts a few critical
texts, and exports the results into a .csv file.
"""
import csv
from dataclasses import dataclass
import os
import pandas as pd
import warnings

from library.cases import Cases
from library.dividers import CaseBundle
from library.paths import Paths
from ml_funnel.data import Data
from ml_funnel.settings import Settings
from library.case_tools import CaseTools
from utilities.timer import timer

@timer('Initial case data collection and parsing')
@dataclass
class CourtParser(CaseTools):

    paths : object
    settings : object
    stage : str = 'parse'
    
    def __post_init__(self):
        if self.settings['general']['verbose']:
            print('Beginning court opinion parsing')
        sources = self.check_sources()
        for source in sources:
            """
            The main input/output loop which takes data from source files and 
            outputs to a .csv file one case at a time. During parsing, the 
            data is stored in a pandas series. The loop iterates through a 
            globbed list of import paths.
            """
            self.source = source
            self.paths.stage = self.stage
            self.paths.conform(stage = self.stage, 
                               source = self.source)
            cases = Cases(paths = self.paths, 
                          settings = self.settings,
                          source = self.source,
                          stage = self.stage) 
            data = Data(settings = self.settings)
            bundle = CaseBundle()
            self.create_divider_list(df = data.df, 
                                     cases = cases)
            self.create_munger_list(cases = cases)
            self.create_column_dicts(df = data.df, 
                                     cases = cases)   
            data.df = pd.Series(index = self.column_dict)
            encoding = self.settings['files']['encoding']
            with open(self.paths.export_path, mode = 'w', newline = '',
                      encoding = encoding) as output_file:
                writer = csv.writer(output_file, 
                                    dialect = 'excel')
                column_list = self.column_dict.keys()
                writer.writerow(column_list)
                for c, bundle.a_path in enumerate(self.paths.import_paths):
                    with open(bundle.a_path, 
                              mode = 'r', 
                              errors = 'ignore',
                              encoding = encoding) as a_file:
                        cases_text = a_file.read()
                        data.initialize_series(column_dict = self.column_dict)
                        data.df[cases.index_col] = c + 1
                        data.df, bundle = self.separate_header(
                                df = data.df, 
                                cases_text = cases_text,
                                bundle = bundle)
                        data.df, bundle = self.divide(df = data.df, 
                                                      bundle = bundle)
                        data.df, bundle = self.munge(df = data.df, 
                                                     bundle = bundle)
                        writer.writerow(data.df)
                        if ((c + 1) % 100 == 0 
                            and self.settings['general']['verbose']):
                            print(c + 1, 'cases parsed')               
        return self 
    
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)  
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtParser(paths, settings)  