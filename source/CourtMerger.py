"""
Primary class for merging court opinion data from different sources.
"""

from dataclasses import dataclass
import os
import warnings

from library.cases import Cases
from library.case_tools import CaseTools
from library.paths import Paths
from ml_funnel.data import Data
from ml_funnel.settings import Settings
from utilities.timer import timer

@timer('Data merging')
@dataclass
class CourtMerger(CaseTools):
    
    paths : object
    settings : object
    stage : str = 'merge'
    
    def __post_init__(self):
        sources = self.check_sources()
        self.source = sources[0]
        self.quick_start()
        for i in range(1, len(sources)):
            source2 = sources[i]
            paths2 = Paths(settings)
            paths2.stage = self.stage
            paths2.conform(stage = self.stage, 
                           source = source2)
            cases2 = Cases(paths = paths2, 
                           settings = self.settings, 
                           source = source2, 
                           stage = self.stage)
            data2 = Data(settings = self.settings,
                         quick_start = True,
                         import_path = paths2.import_path)
            self.data.df = self.merge_dataframes(df1 = self.data.df,
                                                 cases1 = self.cases,
                                                 df2 = data2.df,
                                                 cases2 = cases2)
        self.data.save(export_path = self.paths.export_path,
                       file_format = self.settings['files']['data_out'],
                       encoding = self.settings['files']['encoding'],
                       boolean_out = self.settings['files']['boolean_out'])
        return
    
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'ml_settings.ini'))
    cp_settings = Settings(os.path.join('..', 'cp_settings.ini'))
    settings.config.update(cp_settings.config) 
    paths = Paths(settings)
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtMerger(paths, settings)  