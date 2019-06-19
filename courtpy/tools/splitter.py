
from dataclasses import dataclass
import os
import re

import pandas as pd

from library.stringer import Stringer

@dataclass
class LexisSplit(Stringer):
    """Divides batch downloaded files from Lexis-Nexis into individual .txt
    files. Each .txt file will contain one court opinion. This is done to
    reduce memory usage in the parsing modules so that one case can be parsed
    at a time without other cases also being in memory. The make_subfolders
    option is included if the user wants the individual case files to be stored
    in separate subdirectories based upon which court the opinion was issued
    in.
    """
    paths : object = None
    settings : object = None
    source : str = 'lexis'
    stage : str = 'prepper'
    prep_message = 'Lexis-Nexis cases split into individual text files'

    def __post_init__(self):
        self.settings.localize(instance = self,
                               sections = ['files', 'general', 'cases',
                                           'prepper', 'wrangler'])
        self.source_files = {'federal' : 'federal_court_nums.csv'}
        self.prepped_files = {'federal' : 'federal_court_nums.csv'}
        self.prepper_options = {}
        self._dicts = {}
        return self

    def _check_folder(self, folder_name):
        if not os.path.exists(folder_name):
            os.makedirs(folder_name)
        return self

    def _cases_export(self, case_list, case_num):
#        """Exports text files into the proper folders."""
#        court_num = 0
#        for i, case in enumerate(case_list):
#            if len(case.strip()) > 0:
#                if re.search(self.divider, case):
#                    court_sec = re.search(self.divider, case)
#                    court_num = match_iter(court_sec.upper, court_nums)
#                else:
#                    court_num = 999
#                subfolder = subfolders.get(court_num)
#                file_name = str(court_num * 10000000 + tot_index) + '.txt'
#                if settings.make_subfolders:
#                    path_name = os.path.join(paths.lexis_cases, subfolder,
#                                             file_name)
#                else:
#                    path_name = os.path.join(paths.lexis_cases, file_name)
#                f = open(path_name, 'w')
#                f.write(case.strip())
#                f.close()
                tot_index += 1
        return tot_index

    def _clean_file(self, text):
        """Removes common anomalies in Lexis-Nexis court opinions."""
        text = text.replace('*', '')
        text = re.sub(r'\[\d*\]', '', text)
        text = re.sub(r'[^\S\n]', ' ', text)
        text = self.no_double_space(text)
        text = re.sub(r'(?m)^Signal:.*\n?\n?', '', text)
        text = re.sub(r'(?m)^SIGNAL:.*\n?\n?', '', text)
        text = re.sub(r'(?m)^As of:.*\n?\n?', '', text)
        text = re.sub(r'(?m)^AS OF:.*\n?\n?', '', text)
        text = re.sub(r'\n ', '\n', text)
        return text

    def _create_court_dicts(self):
        divider_file =  os.path.join(self.paths.dividers,
                                     self.source + '_dividers.csv')
        _dict = (pd.read_csv(divider_file, index_col = False)
                   .set_index('values')
                   .to_dict()['keys'])
        self.divider = _dict['court_div']
        courts_file =  os.path.join(self.paths.courts, 'courts.csv')
        _df = pd.read_csv(courts_file,
                          usecols = ['keys', 'values', 'folder_name'],
                          index_col = False)
        self.subfolders = _df.set_index('values').to_dict()['folder_name']
        self.court_nums = _df.set_index('keys').to_dict()['values']
        return self

    def _set_folder(self):
        self.folder = os.path.join(self.paths.dicts)
        return self

    def _set_paths(self):
        self._set_folder()
        self.prepped_file = os.path.join(self.folder,
                                         self.prepped_files[self.jurisdiction])
        self.source_file = os.path.join(self.folder,
                                        self.source_files[self.jurisdiction])
        return self

    def initialize(self):
        self._create_court_dicts()
        self.paths.conform(source = self.source, stage = self.stage)
        return self

    def prepare(self):
        """Divides Lexis-Nexis court opinions into individual files. If the
        make_subfolders options is selected, text files are divided into
        folders based upon the court in the self.subfolders dictionary of
        court numbers and names.
        """
        for i, each_path in enumerate(self.paths.import_paths):
            with open(each_path, mode = 'r', errors = 'ignore',
                      encoding = self.encoding) as a_file:
                batch_cases = a_file.read()
                batch_cases = self._clean_file(batch_cases)
                case_list = re.split('\d* of \d* DOCUMENTS', batch_cases)
                self._cases_export(case_list, i)
        return