"""
This module sets up relevant files for later stages of the CourtPy pipeline.
"""
from dataclasses import dataclass
import os
import warnings

from library.paths import Paths
from ml_funnel.settings import Settings
from utilities.timer import timer


@timer('Total preparation')
@dataclass
class CourtPrepper(object):

    paths : object
    settings : object

    def __post_init__ (self):
        ExternalData(paths = self.paths,
                     settings = self.settings)
        LexisSplitter(paths = self.paths,
                     settings = self.settings)
        return self

@timer('Preparation of external data')
@dataclass
class ExternalData(object):

    paths : object
    settings : object

    def __post_init__ (self):
        if self.settings['prepper']['bios']:
            if self.settings['cases']['jurisdiction'] == 'federal':
                from library.judges import FederalJudges
                judges = FederalJudges(self.paths, self.settings, 'prep')
                judges.make_paths()
                judges.make_files()
                if self.settings['general']['verbose']:
                    print('Judicial biography dataset created')

        if self.settings['prepper']['judicial']:
            if self.settings['cases']['jurisdiction'] == 'federal':
                from library.judiciary import SCOTUS
                scotus = SCOTUS(self.paths, self.settings)
                scotus.make_paths()
                scotus.make_file()
            if self.settings['general']['verbose']:
                print('High court variables created')

        if self.settings['prepper']['legis']:
            if self.settings['cases']['jurisdiction'] == 'federal':
                from library.legislature import Congress
                congress = Congress(self.paths, self.settings)
                congress.make_paths()
                congress.make_file()
            if self.settings['general']['verbose']:
                print('Legislative variables created')

        if self.settings['prepper']['executive']:
            if self.settings['cases']['jurisdiction'] == 'federal':
                from library.executive import President
                president = President(self.paths, self.settings)
                president.make_paths()
                president.make_file()
            if self.settings['general']['verbose']:
                print('Executive variables created')
        return

@timer('Lexis case splitter')
@dataclass
class LexisSplitter(object):

    paths : object
    settings : object

    def __post_init__ (self):
        if self.settings['prepper']['lexis_split']:
            from library.lexis_splitter import lexis_splitter
            lexis_splitter(self.paths, self.settings)
            if self.settings['general']['verbose']:
                print('Lexis cases split into individual files')
        return

if __name__ == '__main__':
    settings = Settings(os.path.join('..', 'settings', 'ml_settings.ini'))
    cp_settings = Settings(os.path.join('..', 'settings', 'cp_settings.ini'))
    settings.config.update(cp_settings.config)
    paths = Paths(settings)
    if not settings['general']['pandas_warnings']:
        warnings.filterwarnings('ignore')
    ExternalData(paths, settings)
    LexisSplitter(paths, settings)
