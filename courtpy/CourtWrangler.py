"""
Primary class for the CourtWrangler. This part of the CourtWrangler pipeline
divides the court opinions into the key sections and extracts a few critical
"""
from dataclasses import dataclass
import os
import warnings

from library.cases import Cases
from library.paths import Paths
from library.case_tools import CaseTools
from ml_funnel.data import Data
from ml_funnel.settings import Settings
from utilities.timer import timer

@timer('Deep parsing and data wrangling')
@dataclass
class CourtWrangler(object):
    
    paths : object
    settings : object
    stage : str = 'wrangle'
    
    def __post_init__(self):
        sources = []
        if self.settings['parser']['lexis_cases']:
            sources.append('lexis_nexis')
        if self.settings['parser']['court_listener_cases']:
            sources.append('court_listener')
        if self.settings['parser']['caselaw_access_cases']:
            sources.append('caselaw_access')
        for source in sources:
            self.source = source
            self.paths.stage = self.stage
            self.paths.conform(stage = self.stage, 
                               source = self.source)
            cases = Cases(paths = self.paths, 
                          settings = self.settings, 
                          source = self.source, 
                          stage = self.stage)
            tools = CaseTools(paths = self.paths,
                              settings = self.settings,
                              source = self.source,
                              stage = self.stage)
            data = Data(settings = self.settings,
                        quick_start = True,
                        import_path = self.paths.import_path)
            data.column_types(bool_prefixes = cases.bool_prefixes,
                              list_prefixes = cases.list_prefixes,
                              float_prefixes = cases.float_prefixes,
                              int_prefixes = cases.int_prefixes,
                              str_prefixes = cases.str_prefixes)
            data.smart_fill_na()
            tools.initialize_judges(cases = cases)
            tools.create_munger_list(cases = cases)
            data.df = tools.munge(df = data.df)
            data.df = tools.combine(df = data.df, cases = cases)
            data.df = tools.add_externals(df = data.df, cases = cases)
            data.df = data.df.loc[:, ~data.df.columns.str.startswith('temp_')]
            data.df = data.df.loc[:, ~data.df.columns.str.startswith('sec_')]
            data.save(export_path = self.paths.export_path)
        return self
    
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtWrangler(paths, settings)   