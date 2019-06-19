
from dataclasses import dataclass


@dataclass
class HighCourt(object):

    name : str = 'caselaw_access'
    file_type : str = 'json'

    def __post_init__(self):
        self.missing_sections = ['politics_high_court']
        return self

@dataclass
class AppellateCourt(object):

    name : str = 'court_listener'
    file_type : str = 'json'

    def __post_init__(self):
        self.missing_sections = ['politics_high_court']
        return self

@dataclass
class TrialCourt(object):

    name : str = 'fjc_idb'
    file_type : str = 'csv'

    def __post_init__(self):
        self.case_divider = ''
        self.import_columns = {}
        self.renames = {}
        self.missing_sections = []
        self.meta_columns = {}
        return self

@dataclass
class OtherCourt(object):

    name : str = 'lexis_nexis'
    file_type : str = 'txt'

    def __post_init__(self):
        self.case_divider = '\d* of \d* DOCUMENTS\n'
        self.import_columns = {}
        self.renames = {}
        self.missing_sections = []
        self.meta_columns = {'file_name' : str, 'word_count' : int}
        return self