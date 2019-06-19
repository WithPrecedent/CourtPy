
from dataclasses import dataclass

#from simplify.ingredients import Ingredient

from cases import Cases
from rules import Rules

@dataclass
class Stage(object):
    """Parent class for CourtPy Parser, Wrangler, Engineer, and Analyzer."""

    paths : object = None
    settings : object = None

    def __post_init__(self):
        if not self.paths:
            error = self.name + ' requires an instance of Paths'
            raise AttributeError(error)
        if not self.settings:
            error = self.name + ' requires an instance of Settings'
            raise AttributeError(error)
        settings_sections = ['general', 'cases', 'files']
        if self.stage in ['analyzer']:
            settings_sections.append('recipes')
        else:
            settings_sections.append(self.stage)
        self.settings.localize(instance = self, sections = settings_sections)
        return self

    def quick_start(self):
        if self.verbose:
            if self.stage in ['parser', 'wrangler']:
                print(self._start_message, self.source, 'court opinions')
        self.paths.conform(source = self.source,
                           stage = self.stage)
        self.rules = Rules(case_type = self.case_type,
                           jurisdiction = self.jurisdiction,
                           source = self.source,
                           stage = self.stage)
        self.rules.create()
        self.cases = Cases(settings = self.settings,
                           filer = self.paths,
                           stage = self.name,
                           quick_start = True)
        self.cases.create()
        if self.stage in ['wrangler', 'engineer', 'analyzer']:
            self.cases.smart_fillna()
            if self.conserve_memory:
                self.cases.downcast()
        return self
