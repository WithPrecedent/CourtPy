"""
CourtEngineer feature engineers the data based upon user settings.
"""
from dataclasses import dataclass
import os
import warnings

from library.case_tools import CaseTools
from library.paths import Paths
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
            self.quick_start()
            self.data.df['index_universal'] = (
                    range(1, len(self.data.df.index) + 1))
            self.data = self.cull_data(data = self.data,
                    drop_prefixes = self.cases.drop_prefixes)
            self.data = self.shape_df(self.data)
            self.data.summarize(export_path = os.path.join(
                    self.paths.output, 'summary_data.csv'))
            self.data.convert_rare_categories(
                    cats = self.data.create_column_list(
                    prefixes = self.cases.cat_prefixes),
                    threshold = self.settings['drops']['cat_threshold'])
            self.data.remove_infrequent_cols(
                    bools = self.data.create_column_list(
                    prefixes = self.cases.bool_prefixes),
                    threshold = self.settings['drops']['drop_threshold'])
            self.engineer_loose_ends(self.data.df)
            self.data.save(export_path = self.paths.export_path,
                           file_format = self.settings['files']['data_out'],
                           encoding = self.settings['files']['encoding'],
                           boolean_out = self.settings['files']['boolean_out'])
            self.loop_cleanup()
        return   

if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)   
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtEngineer(paths, settings)  