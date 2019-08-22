"""Master control script for CourtPy.

Each major module can be called individually or through this class.
The CourtPy class allows the user to call a group of modules through an ad hoc
workflow or run the complete pipeline. The user determines which parts of the
pipeline to invoke by changing the options in courtpy_settings.ini or by
passing a menu when CourtPy is instanced.

If there are any problems with this file or packaged modules, please contact
the creator directly: coreyrayburnyung@gmail.com or post on the GitHub page.
Also, if you find any errors or ways to increase efficiency, please email
or contribute.

If you utilize any code for published work, please use the citation included
on the WithPrecedent Github page. Acknowledgement is greatly appreciated.
"""
from dataclasses import dataclass

from simplify import Inventory, Menu, timer

from almanac import CPAlmanac
from cookbook import CPCookbook


@timer('CourtPy')
@dataclass
class CourtPy(object):
    """Prepares, parses, wrangles, merges, and/or analyzes court opinion data
    based upon user selections.

    Attributes:
        menu: an instance of Menu from the siMpLify module or string containing
            the path to load such an instance.
        inventory: an instance of the Inventory class from the siMpLify
            package. If not passed when the class is instanced, the default
            Inventory instance will be automatically loaded.
    """
    menu : object
    inventory : object = None
    auto_prepare : bool = True

    def __post_init__ (self):
        """Based upon user menu, instances specific step classes from the
        courtpy package.
        """
        super().__post_init()
        # Calls prepare method if auto_prepare is True (default option)
        if self.auto_prepare:
            prepare()
        return self

#    def _set_almanac_options(self):
#        # Replaces the default Almanac classes with custom classes specifically
#        # designed for court data.
#        self.options = {'cultivate' : CPCultivate,
#                        'reap' : CPReap,
#                        'thresh' : CPThresh,
#                        'bale' : CPBale,
#                        'clean' : CPClean}
#        return self
#
#    def _set_cookbook_options(self):
#        return self
#
#    def create(self):
#        # Loop through stages based upon stages variable in self.menu
#        for stage in self._listify(self.stages):
#            if stage in ['cultivate', 'reap', 'thresh', 'bale', 'clean']:
#                self.
#                self.menu.localize(instance = self, sections = ['almanac'])
#                self.stage = CPAlmanac[stage](menu = self.menu,
#                                              inventory = self.inventory,
#                                              stages = stage)
#
#        # Calls the regular prepare and create methods, now linked to the
#        # custom classes in self.options
#        self.prepare()
#        self.create()
#            else:
#                self.stage = CPCookbook[stage](menu = self.menu,
#                                               inventory = self.inventory)
#            if self.conserve_memory:
#                del(self.stage)
#        return self