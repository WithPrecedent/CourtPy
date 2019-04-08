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
        funnel = Funnel(data = self.data)
        funnel.create()
        funnel.iterate()
        if funnel.settings['results']['export']:  
            funnel.save_results(export_path = self.paths.output)
            funnel.save_tube(export_path = os.path.join(self.paths.output,
                                                        'best_tube.pkl'),
                             tube = funnel.best)
            funnel.save_funnel(export_path = os.path.join(self.paths.output,
                                                          'funnel.pkl'))
        if self.settings['general']['verbose']:
            print('The best test tube, based upon the', funnel.key_metric,
                  'metric with a score of', funnel.best_score, 'is:')
            print(funnel.best)
        self.data.save(df = self.data.df,
                       export_path = self.paths.export_path,
                       file_format = self.settings['files']['data_out'],
                       boolean_out = self.settings['files']['boolean_out'],
                       encoding = self.settings['files']['encoding'])
        if 'plot' in self.settings['general']['stages']:
            from CourtPlotter import CourtPlotter
            CourtPlotter(funnel = funnel,
                         paths = self.paths,
                         settings = self.settings,
                         cases = self.cases)            
        return self 
      
if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)  
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtAnalyzer(paths, settings) 