""" 
CourtPlotter includes some basic charts and graphs for interpreting the 
results of the CourtAnalyzer.
"""
from dataclasses import dataclass
import os
import warnings

from library.cases import Cases
from library.case_tools import CaseTools
from library.paths import Paths
from ml_funnel.settings import Settings
from ml_funnel.data import Data
from ml_funnel.funnel import Funnel
from utilities.timer import timer

@timer('Graphing and plotting')
@dataclass
class CourtPlotter(CaseTools):
    
    data : object = None
    funnel : object = None
    paths : object = None
    settings : object = None
    cases : object = None
    stage : str = 'plot'
   
    def __post_init__(self):
        if self.settings['general']['verbose']:
            print('Plotting results')
        self.paths.stage = self.stage
        self.paths.conform(stage = self.stage)
        if not self.data:
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
            data.downcast()
            data = self.create_splices(data)
        if not self.funnel:
            self.funnel = Funnel(data = data)
            self.funnel.load_funnnel(
                import_path = os.path.join(self.paths.output, 'funnel.pkl'))
        self.funnel.visualize(model = self.funnel.best)
        return

if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings.ini'))
    paths = Paths(settings)
    ml_settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    settings.config.update(ml_settings.config)  
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtPlotter(paths, settings)  