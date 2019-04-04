"""
CourtAnalyzer module for applying various machine learning algorithms.
"""
from dataclasses import dataclass
import os
import warnings

from library.cases import Cases
from library.case_tools import CaseTools
from library.paths import Paths
from ml_funnel.data import Data
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
        self.paths.conform(stage = self.stage)
        cases = Cases(paths = self.paths, 
                      settings = self.settings,
                      stage = self.stage)
        data = Data(settings = self.settings,
                    quick_start = True,
                    import_path = self.paths.import_path)
        data.column_types(bool_prefixes = cases.bool_prefixes,
                          cat_prefixes = cases.cat_prefixes,
                          float_prefixes = cases.float_prefixes,
                          int_prefixes = cases.int_prefixes,
                          interact_prefixes = cases.interact_prefixes)
        data.smart_fill_na()
        data.df.drop(data.create_column_list(prefixes = 'index_'), 
                     axis = 'columns', inplace = True)        
        data.downcast()
        data = self.create_splices(data)
        funnel = Funnel(data = data)
        funnel.create()
        funnel.iterate()
        if funnel.settings['results']['export']:
            if funnel.settings['defaults']['verbose']:
                print('Exporting results data')    
            results_path = os.path.join(self.paths.output, 'results_table.csv')
            data.save(df = funnel.results.table, 
                      export_path = results_path)
        best_model = funnel.best_tube(funnel.results, )
        data.save(data.df,
                  export_path = self.paths.export_path,
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