
from dataclasses import dataclass

from simplify.managers import Technique


@dataclass
class Executive(Technique):
    """Contains information for loading and mapping executive branch
    ideology variables.
    """
    jurisdiction : str = ''
    paths : object = None
    settings : object = None
    prep_message = 'Executive branch ideology data prepared'
    wrangle_message = 'Executive branch ideology data added to dataframe'

    def __post_init__(self):
        super().__post_init__()
        self.source_files = {'federal' : 'president.csv'}
        self.prepped_files = {'federal' : 'president.csv'}
        self.prepper_options = {}
        self._dicts = {'ideology' : ['year', 'party']}
        return self
