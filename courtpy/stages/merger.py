"""
Primary class for merging court opinion data from different sources.
"""

from dataclasses import dataclass
import os
import warnings

from simplify import Settings
from simplify import timer

from cases import Cases
from tool import Tool
from utilities.paths import Paths


@timer('Data merging')
@dataclass
class CourtMerger(Tool):

    paths : object
    settings : object
    stage : str = 'merge'
    _complete_message = 'Merging of different court data sources successful'

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
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    if not settings['general']['pandas_warnings']:
        warnings.filterwarnings('ignore')
    CourtMerger(paths, settings)