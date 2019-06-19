"""
Primary class for the CourtWrangler. This part of the CourtWrangler pipeline
divides the court opinions into the key sections and extracts a few critical
"""
from dataclasses import dataclass
import os
import warnings

from simplify import Settings
from simplify import timer

from cases import Cases
from tool import Tool
from utilities.paths import Paths


@timer('Deep parsing and data wrangling')
@dataclass
class Wrangler(Tool):

    paths : object
    settings : object
    name : str = 'wrangle'
    _complete_message = 'Data wrangling complete'

    def __post_init__(self):
        super().__post_init__()
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
                self.data.df.loc[:,
                    ~self.data.df.columns.str.startswith('temp_')])
            self.data.df = (
                self.data.df.loc[:,
                    ~self.data.df.columns.str.startswith('sec_')])
            self.data.save(export_folder = self.filer.data_folder,
                           file_name = self.paths.export_file,
                           file_format = self.settings['files']['data_out'],
                           boolean_out = self.settings['files']['boolean_out'],
                           encoding = self.settings['files']['encoding'])
            self.loop_cleanup()
        return self

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    if not settings['general']['pandas_warnings']:
        warnings.filterwarnings('ignore')
    CourtWrangler(paths, settings)