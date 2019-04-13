""" 
CourtPlotter includes some basic charts and graphs for interpreting the 
results of the CourtAnalyzer.
"""
from dataclasses import dataclass
import os
import warnings

from library.case_tools import CaseTools
from library.paths import Paths
from ml_funnel.settings import Settings
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
            self.quick_start()
            self.data = self.create_splices(self.data)
        if not self.funnel:
            self.funnel = Funnel(data = self.data,
                                 filer = self.filer)
            self.funnel.load_funnel(
                import_path = os.path.join(self.paths.results, 'funnel.pkl'))
        self.funnel.visualize(funnel = self.funnel)
        return

if __name__ == '__main__':
    settings = Settings(os.path.join('ml_funnel', 'ml_settings.ini'))
    cp_settings = Settings(os.path.join('..', 'settings.ini'))
    settings.config.update(cp_settings.config) 
    paths = Paths(settings)
    if not settings['general']['warnings']:
        warnings.filterwarnings('ignore')
    CourtPlotter(paths, settings)  