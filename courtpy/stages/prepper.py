"""
This module sets up relevant files for later stages of the CourtPy pipeline.
"""
from dataclasses import dataclass
import os
import warnings

from simplify import Settings
from simplify import timer

from dividers.splitter import Splitter
from externals.biographies import Biographies
from externals.executive import Executive
from externals.judiciary import Judiciary
from externals.legislature import Legislature
from tool import Tool
from utilities.paths import Paths


@timer('External data and source preparation')
@dataclass
class CourtPrepper(Tool):
    """
    Class the acquires and/or prepares external data related to other branches
    of government and biographical information of judges. It also splits files
    from 'lexis_nexis' if that is the source.
    """
    paths : object
    settings : object
    stage : str = 'prepper'
    _complete_message = 'Preparation of external data complete'

    def __post_init__ (self):
        super().__post_init__()
        self.prepper_dict = {'executive' : Executive,
                             'legislature' : Legislature,
                             'judiciary' : Judiciary,
                             'biographies' : Biographies,
                             'lexis_split' : LexisSplit}
        for step in self.externals:
            instance = self.prepper_dict[step](
                    jurisdiction = self.jurisdiction,
                    paths = self.paths,
                    settings = self.settings)
            instance.prepare()
            if self.verbose:
                print(instance.prep_message)
        return

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    warnings.filterwarnings('ignore')
    CourtPrepper(paths, settings)
