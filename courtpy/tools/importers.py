import csv
from dataclasses import dataclass
import json

@dataclass
class Files(object):
    """
    Container for importing original data sources for use by CourtParser.
    """
    settings : object
    paths : object
    data_source : str
    column_dict : object

    def __post_init__(self):
        self.import_columns = {'caselaw_access' : {},
                               'court_listener' : {'resource_uri' : str,
                                                   'id' : int,
                                                   'author' : str,
                                                   'plain_text' : str},
                               'fjc_idb' : {},
                               'lexis_nexis' : {}}
        self.renames = {'caselaw_access' : {},
                        'court_listener' : {'resource_uri' :'cl_url',
                                            'id' : 'cl_id',
                                            'plain_text' : 'bundle.opinions'},
                        'fjc_idb' : {},
                        'lexis_nexis' : {}}
        return self

    def _load_csv(self, import_path):
        with open(import_path, mode = 'r', errors = 'ignore',
                  encoding = self.encoding) as import_file:
            case_text = import_file.read()
        return case_text

    def _init_save_csv(self):
        with open(self.paths.export_path, mode = 'w', newline = '',
                  encoding = self.encoding) as output_file:
            self.writer = csv.writer(output_file, dialect = 'excel')
            self.column_list = self.column_dict.keys()
            self.writer.writerow(self.column_list)
        return self