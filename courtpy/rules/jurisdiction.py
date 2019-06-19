

from dataclasses import dataclass

from externals.biographies import Biographies
from externals.executive import Executive
from externals.judiciary import Judiciary
from externals.legislature import Legislature
from rules import Rules

@dataclass
class Jurisdiction(Rules):

    name : str = ''
    params : str = ''

    def __post_init__(self):
        super().__post_init__()
        self.options = {'judge_bios' : Biographies,
                        'legislature' : Legislature,
                        'judiciary': Judiciary,
                        'executive' : Executive}
        return self

    def include(self, instance):
        for var_name, external_class in self.options.items():
            external_instance = external_class(settings = self.settings)
            setattr(instance, var_name, external_instance.mapping)
        return instance

@dataclass
class Federal(Jurisdiction):

    name : str = 'federal'
    file_name = 'federal_courts'
    file_type = 'csv'

    def __post_init__(self):
        self.externals = {'high_court' : 'martin_quinn',
                          'judge_bios' : 'fjc_bios',
                          'legislature' : 'nominate',
                          'executive' : 'president'}
        return self
