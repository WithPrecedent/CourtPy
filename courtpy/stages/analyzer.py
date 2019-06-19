
from dataclasses import dataclass
import os
import warnings

from simplify.cookbook import Cookbook
from simplify import Settings, timer

from cases import Cases
from tool import Tool
from utilities.paths import Paths


@timer('Data and model analysis')
@dataclass
class Analyzer(Tool):
    """Applies various machine learning algorithms as part of the CourtPy
    package using classes and methods from the siMpLify package.
    """
    paths : object
    settings : object
    stage : str = 'analyzer'
    _complete_message = 'Data analysis complete'

    def __post_init__(self):
        super().__post_init__()
        self.quick_start()
        self.cases.drop_columns(prefixes = 'index_')
        self.cookbook = Cookbook(data = self.cases,
                                 filer = self.paths,
                                 settings = self.settings)
        self.add_splices()
        self.cookbook.create()
        self.cookbook.iterate()
        self.cookbook.save_everything()
        if self.verbose:
            print('The best test tube, based upon the',
                  self.cookbook.key_metric, 'metric with a score of',
                  self.cookbook.best_score, 'is:')
            print(self.cookbook.best_recipe)
        self.data.save(export_folder = self.paths.data,
                       file_name = self.paths.export_file,
                       file_format = self.export_format,
                       boolean_out = self.boolean_out,
                       encoding = self.encoding)
        return self

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    warnings.filterwarnings('ignore')
    CourtAnalyzer(paths, settings)