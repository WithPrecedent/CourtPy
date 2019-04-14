"""
CourtAnalyzer module for applying various machine learning algorithms.
"""
from dataclasses import dataclass
import os
import warnings

from library.case_tools import CaseTools
from library.paths import Paths
from ml_funnel.funnel import Funnel
from ml_funnel.settings import Settings
from utilities.timer import timer

@timer('Data and model analysis')
@dataclass
class CourtAnalyzer(CaseTools):
    
    paths : object
    settings : object
    stage : str = 'analyze'
    
    def __post_init__(self):
        if self.settings['general']['verbose']:
            print('Importing data for analysis')
        self.quick_start()
        self.data.df.drop(self.data.create_column_list(prefixes = 'index_'), 
                          axis = 'columns', 
                          inplace = True)        
        self.data = self.create_splices(self.data)
        self.funnel = Funnel(data = self.data, 
                        filer = self.filer)
        self.funnel.create()
        self.funnel.iterate()
        self.funnel.save_everything()
        if self.settings['general']['verbose']:
            print('The best test tube, based upon the', self.funnel.key_metric,
                  'metric with a score of', self.funnel.best_score, 'is:')
            print(self.funnel.best)
        self.data.save(export_folder = self.filer.data_folder,
                       file_name = self.paths.export_file,
                       file_format = self.settings['files']['data_out'],
                       boolean_out = self.settings['files']['boolean_out'],
                       encoding = self.settings['files']['encoding'])          
        return self 
      
if __name__ == '__main__':
    settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    cp_settings = Settings(os.path.join('..', 'settings.ini'))
    settings.config.update(cp_settings.config) 
    paths = Paths(settings)
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtAnalyzer(paths, settings) 