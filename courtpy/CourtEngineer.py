"""
CourtEngineer feature engineers the data based upon user settings.
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

@timer('Feature engineering')
@dataclass
class CourtEngineer(CaseTools):
    
    paths : object
    settings : object
    stage : str = 'engineer'
    
    def __post_init__(self):       
        if self.settings['general']['verbose']:
            print('Beginning feature engineering')
        sources = self.check_sources()
        for source in sources:
            self.source = source
            self.paths.conform(stage = self.stage, 
                               source = self.source)
            cases = Cases(paths = self.paths, 
                          settings = self.settings, 
                          source = self.source, 
                          stage = self.stage)
            data = Data(settings = self.settings,
                        quick_start = True,
                        import_path = self.paths.import_path)
            data.df['index_universal'] = range(1, len(data.df.index) + 1)
            data.column_types(bool_prefixes = cases.bool_prefixes,
                              cat_prefixes = cases.cat_prefixes,
                              float_prefixes = cases.float_prefixes,
                              int_prefixes = cases.int_prefixes,
                              interact_prefixes = cases.interact_prefixes)
            data.smart_fill_na()
            data = self.cull_data(data = data,
                                  drop_prefixes = cases.drop_prefixes)
            data = self.shape_df(data)
            data.summarize(export_path = os.path.join(self.paths.output,
                                                      'summary_data.csv'))
            data.convert_rare_categories(cats = data.create_column_list(
                        prefixes = cases.cat_prefixes),
                        threshold = self.settings['drops']['cat_threshold'])
            data.remove_infrequent_cols(bools = data.create_column_list(
                        prefixes = cases.bool_prefixes),
                        threshold = self.settings['drops']['drop_threshold'])
            self.engineer_loose_ends(data.df)
            data.save(export_path = self.paths.export_path,
                      file_format = self.settings['files']['data_out'],
                      encoding = self.settings['files']['encoding'],
                      boolean_out = self.settings['files']['boolean_out'])
        return   

if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)   
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtEngineer(paths, settings)  