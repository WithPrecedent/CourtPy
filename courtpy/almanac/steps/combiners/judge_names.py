"""
Classes for judge-related data. The classes provide methods to combine
external data with court opinion data in a consistent manner.
"""

import csv
from dataclasses import dataclass
import re

import pandas as pd
from simlify.managers import Technique


@dataclass
class JudgeNames(Technique):
    """
    Class used for standardized names and biographical information for judges.
    """
    jurisdiction : str = ''

    def __post_init__(self):
        """
        Sets default values for class instances.
        """
        super().__post_init__()
        self.source_files = {'federal' : 'fjc_bios_raw.csv'}
        self.prepped_files = {'federal' : 'fjc_names.csv'}
        self.prepper_options = {'federal' : self._create_federal_names}
        self._dicts = {'names' : ['name', 'name']}
        return self

    @staticmethod
    def convert_judge_name(year, court, name, dict_type):
        if dict_type == 1:
            return str(int(court) * 10000 + int(year)) + name
        elif dict_type == 2:
            return str(int(year)) + name
        else:
            return name

    @staticmethod
    def convert_judge_name_series(year, court, name, dict_type):
        if dict_type == 1:
            return ((court.astype(int) * 10000
                     + year.astype(int)).astype(str)
                     + name)
        if dict_type == 2:
            return year.astype(int).astype(str) + name

    @staticmethod
    def name_perms(row):
        """
        Constructs a consistent set of names for each judge with different
        ordering and formatting.
        """
        row['first_name'] = re.sub("\.|\,|\[|\]|\'", '', row['first_name'])
        row['last_name'] = re.sub("\.|\,|\[|\]|\'", '', row['last_name'])
        row['middle_name'] = re.sub("\.|\,|\[|\]|\'", '', row['middle_name'])
        first = row['first_name'].strip().upper()
        first_init = first[0].upper()
        middle = row['middle_name'].strip().upper()
        if middle:
            middle_init = middle[0].upper()
        else:
            middle_init = ''
        last = row['last_name'].strip().upper()
        row['name_perm1'] = f'{first} {middle} {last}'
        row['name_perm2'] = f'{first} {middle_init} {last}'
        row['name_perm3'] = f'{first} {last}'
        row['name_perm4'] = f'{first_init} {middle} {last}'
        row['name_perm5'] = f'{first_init} {middle_init} {last}'
        row['name_perm6'] = f'{first_init} {last}'
        row['name_perm7'] = f'{last}'
        return row

    def reshape_names(self):
        self.df = self.df[['start_year', 'end_year', 'court_num',
                           'circuit_num', 'name_perm1', 'name_perm2',
                           'name_perm3', 'name_perm4', 'name_perm5',
                           'name_perm6', 'name_perm7', 'full_name']]
        self.df.sort_values(by = ['court_num'])
        self.df['year'] = 0
        """
        Reshapes dataframe to long based upon name_perms
        """
        self.df['nindex'] = range(0, len(self.df))
        self.df = pd.wide_to_long(self.df, stubnames = 'name_perm',
                                  i = 'nindex', j = 'nperm')
        self.df = self.df[self.df.name_perm != '']
        with open(self.fjc_names, mode = 'w', newline = '') as output_file:
            writer = csv.writer(output_file)
            writer.writerow(self.df.columns.values.tolist())
            for i, row in self.df.iterrows():
                for j in range(self.settings['general']['start_year'],
                               self.settings['general']['end_year']):
                    if (row['start_year'] <= j
                        and (j - 2) <= row['end_year']):
                        row['year'] = j
                        row2 = row
                        writer.writerow(row2)
            return

    def include(self, instance, prefix = ''):
        self.name_cols = {'year' : int, 'full_name' : str, 'court_num' : int,
                          'circuit_num' : int, 'name_perm' : str}
        self.name_cols_list = list(self.name_cols.keys())
        self.names_dict_list = []
        self.names = pd.read_csv(self.fjc_names,
                                 usecols = self.name_cols_list,
                                 index_col = False,
                                 encoding = self.settings['files']['encoding'])
        self.names['name_perm'] = self.names['name_perm'].str.upper()
        self.names['concat_name1'] = self.convert_judge_name_series(
                  self.names['year'], self.names['court_num'],
                  self.names['name_perm'], dict_type = 1)
        self.names['concat_name2'] = self.convert_judge_name_series(
                  self.names['year'], self.names['circuit_num'],
                  self.names['name_perm'], dict_type = 1)
        self.names['concat_name3'] = self.convert_judge_name_series(
                  self.names['year'], self.names['court_num'],
                  self.names['name_perm'], dict_type = 2)
        self.names['concat_name4'] = self.convert_judge_name_series(
                  self.names['year'], self.names['circuit_num'],
                  self.names['name_perm'], dict_type = 2)
        self.names_dict_list.append(
                self.names.set_index('concat_name1').to_dict()['full_name'])
        self.names_dict_list.append(
                self.names.set_index('concat_name2').to_dict()['full_name'])
        self.names_dict_list.append(
                self.names.set_index('concat_name3').to_dict()['full_name'])
        self.names_dict_list.append(
                self.names.set_index('concat_name4').to_dict()['full_name'])
        self.names_dict_list.append(
                self.names.set_index('name_perm').to_dict()['full_name'])
        return self