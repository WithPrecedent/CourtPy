
from dataclasses import dataclass


@dataclass
class CaselawAccess(object):

    source_name : str = 'caselaw_access'
    source_file_type : str = 'json'

    def __post_init__(self):
        self.case_divider = ''
        self.import_columns = {}
        self.renames = {}
        self.missing_sections = ['history', 'future', 'counsel', 'disposition',
                                 'cite', 'notice']
        self.meta_columns = {'file_name' : str, 'word_count' : int}
        return self

@dataclass
class CourtListener(object):

    source_name : str = 'court_listener'
    source_file_type : str = 'json'

    def __post_init__(self):
        self.case_divider = ''
        self.import_columns = {'resource_uri' : str,
                               'id' : int,
                               'author' : str,
                               'plain_text' : str}
        self.renames = {'resource_uri' :'cl_url',
                        'id' : 'cl_id',
                        'plain_text' : 'opinions'}
        self.missing_sections = ['history', 'future', 'counsel', 'disposition',
                                 'cite', 'notice']
        self.meta_columns = {'file_name' : str, 'word_count' : int,
                             'cl_url' : str, 'cl_id' : str}
        return self

@dataclass
class FJCIDB(object):

    source_name : str = 'fjc_idb'
    source_file_type : str = 'csv'

    def __post_init__(self):
        self.case_divider = ''
        self.import_columns = {}
        self.renames = {}
        self.missing_sections = []
        self.meta_columns = {}
        return self

@dataclass
class LexisNexis(object):

    source_name : str = 'lexis_nexis'
    source_file_type : str = 'txt'

    def __post_init__(self):
        self.case_divider = '\d* of \d* DOCUMENTS\n'
        self.import_columns = {}
        self.renames = {}
        self.missing_sections = []
        self.meta_columns = {'file_name' : str, 'word_count' : int}
        return self