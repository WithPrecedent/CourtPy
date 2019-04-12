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
        funnel = Funnel(data = self.data, 
                        filer = self.filer)
        funnel.create()
        funnel.iterate()
        funnel.save_everything()
        if self.settings['general']['verbose']:
            print('The best test tube, based upon the', funnel.key_metric,
                  'metric with a score of', funnel.best_score, 'is:')
            print(funnel.best)
        self.data.save(export_folder = self.filer.export_folder,
                       file_name = self.paths.export_file,
                       file_format = self.settings['files']['data_out'],
                       boolean_out = self.settings['files']['boolean_out'],
                       encoding = self.settings['files']['encoding'])          
        return self 
      
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)  
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtAnalyzer(paths, settings) 