"""
Primary class for the CourtWrangler. This part of the CourtWrangler pipeline
divides the court opinions into the key sections and extracts a few critical
"""
from dataclasses import dataclass
import os
import warnings

from library.paths import Paths
from library.case_tools import CaseTools
from ml_funnel.settings import Settings
from utilities.timer import timer

@timer('Deep parsing and data wrangling')
@dataclass
class CourtWrangler(CaseTools):
    
    paths : object
    settings : object
    stage : str = 'wrangle'
    
    def __post_init__(self):
        if self.settings['general']['verbose']:
            print('Beginning vectorized data wrangling')
        sources = self.check_sources()
        for source in sources:
            self.source = source
            self.quick_start()
            self.initialize_judges(cases = self.cases)
            self.create_munger_list(cases = self.cases)
            self.data.df = self.munge(df = self.data.df)
            self.data.df = self.combine(df = self.data.df, 
                                        cases = self.cases)
            self.data.df = self.add_externals(df = self.data.df, 
                                              cases = self.cases)
            self.data.df = (
                self.df.loc[:, ~self.data.df.columns.str.startswith('temp_')])
            self.data.df = (
                self.df.loc[:, ~self.data.df.columns.str.startswith('sec_')])
            self.data.save(export_path = self.paths.export_path,
                           file_format = self.settings['files']['data_out'],
                           encoding = self.settings['files']['encoding'],
                           boolean_out = self.settings['files']['boolean_out'])
            self.loop_cleanup()
        return self
    
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'ml_settings.ini'))
    cp_settings = Settings(os.path.join('..', 'cp_settings.ini'))
    settings.config.update(cp_settings.config) 
    paths = Paths(settings)
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtWrangler(paths, settings)   