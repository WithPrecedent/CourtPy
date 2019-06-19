"""Master control script for CourtPy.

Each major module can be called individually or through this class.
The CourtPy class allows the user to call a group of modules through an ad hoc
workflow or run the complete pipeline. The user determines which parts of the
pipeline to invoke by changing the options in courtpy_settings.ini or by
passing settings when CourtPy is instanced.

If there are any problems with this file or packaged modules, please contact
the creator directly: coreyyung@ku.edu or post on the GitHub page.
Also, if you find any errors or ways to increase efficiency, please email
or contribute.

If you utilize any code for published work, please use the citation included
on the WithPrecedent Github page. Acknowledgement is greatly appreciated.
"""
from dataclasses import dataclass
import os
from pathlib import Path
import warnings

from simplify import Settings
from simplify import timer

from stages.analyzer import Analyzer
from stages.engineer import Engineer
from stages.merger import Merger
from stages.parser import Parser
from stages.prepper import Prepper
from stages.wrangler import Wrangler
from tools.paths import Paths


@timer('CourtPy')
@dataclass
class CourtPy(object):
    """Prepares, parses, wrangles, merges, and/or analyzes court opinion data
    and/or files based upon user selections.

    Attributes:
        paths: an instance of Paths which is a subclass of the Filer class
            from the siMpLify module.
        settings: an instance of Settings from the siMpLify module. Either
            settings or settings_path should be provided when CourtPy is
            instanced. If neither is passed, the class seeks a settings file
            in a default location with a default name.
        settings_path: a path where an appropriate file for a Settings instance
            is located.
    """
    paths : object
    settings : object = None
    settings_path : str = ''

    def __post_init__ (self):
        """Based upon user settings, instances specific step classes from the
        courtpy package.
        """
        # Loads settings from an .ini file if not passed when class is
        # instanced.
        if not self.settings:
            if not self.settings_path:
                self.settings_path = Path(os.path.join('settings',
                                          'courtpy_settings.ini'))
            if self.settings_path.is_file():
                self.settings = Settings(file_path = self.settings_path)
            else:
                error = self.settings_path + ' does not exist'
                raise OSError(error)
        # Local attributes are added from the settings instance.
        self.settings.localize(instance = self, sections = ['cases'])
        # Sets options for stages and associated classes.
        self.stage_options = {'prepper' : Prepper,
                              'parser' : Parser,
                              'wrangler' : Wrangler,
                              'merger' : Merger,
                              'engineer' : Engineer,
                              'analyzer' : Analyzer}
        # Loop through stages based upon stages variable in self.settings
        for stage in self.stages:
            instance = self.stage_options[stage](paths = self.paths,
                                                 settings = self.settings)
            if self.conserve_memory:
                del(instance)
        return self

if __name__ == '__main__':
    settings = Settings(os.path.join('settings', 'courtpy_settings.ini'))
    ml_settings = Settings(os.path.join('settings', 'simplify_settings.ini'))
    settings.update(ml_settings)
    paths = Paths(settings)
    warnings.filterwarnings('ignore')
    CourtPy(paths, settings)