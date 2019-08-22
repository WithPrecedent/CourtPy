
from dataclasses import dataclass
import warnings

from simplify import Menu, Inventory, timer
from simplify.cookbook import Cookbook


@timer('Data and model analysis')
@dataclass
class CPCookbook(Cookbook):
    """Applies various machine learning algorithms as part of the CourtPy
    package using classes and methods from the siMpLify package.
    """
    menu : object
    inventory : object = None
    steps : object = None
    ingredients : object = None
    recipes : object = None
    auto_prepare : bool = True
    name : str = 'cookbook'

    def __post_init__(self):
        super().__post_init__()
        # Disables python warnings
        warnings.filterwarnings('ignore')
        # Loads menu from an .ini file if not passed when class is
        # instanced.
        if isinstance(self.menu, str):
            self.menu = Menu(file_path = self.menu)
        # Local attributes are added from the menu instance.
        self.menu.localize(instance = self, sections = ['files', 'general'])
        # Loads default Inventory instance if one is not passed.
        if not self.inventory:
            self.inventory = Inventory(menu = self.menu,
                                       stage = 'cookbook',
                                       import_format = self.import_format,
                                       export_format = self.export_format)
        if not self.stages:
            self.stages = self.menu['cases']['stages']

        # Sets stage options
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

    def add_splices(self, splice_dict = None):
        if not splice_dict:
            splice_dict = self.settings['splicers_params']
        if self.settings['splicers_params']['prefixes']:
            for name, splice in splice_dict.items():
                prefixes = self.funnel._listify(splice)
                if not name in ['include_all', 'prefixes']:
                    self.funnel.add_splice(splice = name, prefixes = prefixes)
        else:
            for name, splice in splice_dict.items():
                cols = self.funnel._listify(splice)
                if not name in ['include_all', 'prefixes']:
                    self.funnel.add_splice(splice = name, cols = cols)
        return self