"""
Primary class for merging court opinion data from different sources.
"""

from dataclasses import dataclass
import warnings

from library.paths import Paths
from library.settings import Settings
from utilities.timer import timer

@timer('Data merging')
@dataclass
class CourtMerger(object):
    
    paths : object
    settings : object
    stage : str = 'merge'
    
    def __post_init__(self):
        self.paths.stage = self.stage
        self.paths.conform()
        """
        Creates list of source formats.
        """
        self.sources = []     
        if self.settings.lexis_cases: 
            self.sources.append('lexis')
        if self.settings.court_listener_cases:
            self.sources.append('cl')
        if self.settings.caselaw_access_cases: 
            self.sources.append('ca')
        self.paths.conform('merge', self.source) 
        self.merge_dataframes()
        return
    
    def merge_dataframes(self):
        self.df = self.df_import(self.paths.import_path, 
                                 nrows = None,
                                 encoding = self.settings.encoding)
        self.df_export(self.df, 
                       export_path = self.paths.export_path, 
                       index = False,
                       boolean = self.settings.boolean_out,
                       encoding = self.settings.encoding)
        return self
    
if __name__ == '__main__':
    settings = Settings()
    paths = Paths(settings)
    if not settings.warnings:
        warnings.filterwarnings('ignore')
    CourtMerger(paths, settings)  