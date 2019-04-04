""" 
CourtPlotter includes some basic charts and graphs for interpreting the 
results of the CourtAnalyzer.
"""
from dataclasses import dataclass
import pickle
import warnings

from library.cases import Cases
from library.case_tools import CaseTools
from library.paths import Paths
from library.settings import Settings
from ml_funnel.data import Data
from ml_utilities.plot import Plotter
from utilities.timer import timer

@timer('Graphing and plotting')
@dataclass
class CourtPlotter(CaseTools):
    
    paths : object
    settings : object
    cases : object = None
    model : object = None
    stage : str = 'plot'
   
    def __post_init__(self):
        if self.settings['general']['verbose']:
            print('Plotting results of data analysis')
        self.paths.stage = self.stage
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
        for m in models:
            if self.settings['general']['verbose']:
                print('Importing fit ' + m.name + ' model')
            model = m(self.paths, self.settings)
            import_path = self.paths.model_path_constructor(m.name)
            model.clf = pickle.load(open(import_path, 'rb'))
            if self.settings['general']['verbose']:
                print('Plotting results for ' + m.name + ' model')
            self.plotter = Plotter(model)
            self.plotter.plots(model, self.cases, self.paths, self.settings)
        return

if __name__ == '__main__':
    settings = Settings()
    paths = Paths(settings)
    if not settings.warnings:
        warnings.filterwarnings('ignore')
    CourtPlotter(paths, settings)  