from dataclasses import dataclass
import os

import pandas as pd
import requests


@dataclass
class External(object):
    """Class related to adding externally sourced data into the courtpy
    dataset. External includes shared methods for external data source classes.
    CourtPrepper uses these classes to get and prepare source data.
    CourtWrangler uses these classes to add the external data to a dataframe.
    """

    def __post_init__(self):
        self.settings.localize(instance = self,
                               sections = ['files', 'general', 'cases',
                                           'prepper', 'wrangler'])
        return

    def _export_prepped_file(self):
        self.df.to_csv(self.prepped_file,
                       columns = self.prepped_columns,
                       index = False)
        return

    def file_download(self, file_url, file_path):
        """Downloads file from a URL if the file is available."""
        file_response = requests.get(file_url)
        with open(file_path, 'wb') as file:
            file.write(file_response.content)
        return self

    def _import_prepped_file(self):
        df = pd.read_csv(self.prepped_file,
                         encoding = self.encoding,
                         index_col = False)
        return df

    def _import_source_file(self):
        if not os.path.isfile(self.source_file):
            if self.allow_downloads:
                self.file_download(self.file_url, self.source_file)
            else:
                error = self.source_file + ' not found'
                raise FileNotFoundError(error)
        self.df = (pd.read_csv(self.source_file,
                               usecols = self.source_columns,
                               index_col = False)
                     .rename(columns = self.renames))
        return self

    def _make_dict(self, key_col, value_col, df = None):
        if not df:
            df = self._import_prepped_file()
        _dict = df.set_index(key_col).to_dict()[value_col]
        return _dict

    def _set_folder(self):
        self.folder = os.path.join(self.paths.externals,
                                   self.__class__.__name__.lower(),
                                   self.jurisdiction)
        if not os.path.exists(self.folder):
             os.makedirs(self.folder)
        return self

    def _set_paths(self):
        self._set_folder()
        self.prepped_file = os.path.join(self.folder,
                                         self.prepped_files[self.jurisdiction])
        self.source_file = os.path.join(self.folder,
                                        self.source_files[self.jurisdiction])
        return self

    def create(self):
        self._set_paths()
        for var_name, columns in self._dicts.items():
            setattr(self, var_name, self._make_dict(columns[0], columns[1]))
        return self

    def include(self, instance, prefix = ''):
        self._set_paths()
        for var_name, columns in self._dicts.items():
            setattr(instance, prefix + '_' + var_name,
                    self._make_dict(columns[0], columns[1]))
        return instance

    def mapper(self, df, match_column, out_column, map_dict):
        self.match_columns = {'judge_exp' : 'judge_name',
                              'judge_attr' : 'judge_name',
                              'judge_demo'  : 'judge_name',
                              'judge_ideo' : 'judge_name',
                              'panel_num' : 'judge_name',
                              'panel_exp' : 'judge_name',
                              'panel_attr' : 'judge_name',
                              'panel_demo' : 'judge_name',
                              'panel_ideo' : 'judge_name',
                              'panel_size' : 'sec_panel_judges',
                              'politics' : 'year'}
        df[out_column] = df[match_column].astype(str).map(map_dict)
        return df

    def prepare(self):
        self._set_paths()
        if self.prepper_options:
            self._set_source_variables()
            self._import_source_file()
            self.prepper_options[self.jurisdiction]()
            self._export_prepped_file()
        return self