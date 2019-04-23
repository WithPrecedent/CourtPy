"""
Classes for judge-related data. The classes provide methods to combine
external data with court opinion data in a consistent manner.
"""

import csv
from dataclasses import dataclass
import numpy as np
import os
import pandas as pd
import re
from shutil import copyfile

from more_itertools import unique_everseen

from utilities.strings import no_breaks, no_double_space
from utilities.files import file_download
from utilities.rematch import ReMatch


@dataclass
class Judges(object):
    """
    Class used for standardized names and biographical information for judges.
    """
    def __post_init__(self):
        """
        Sets default values for class instances.
        """
        self.column_dict = {}
        self.court_col = 'court_num'
        self.year_col = 'date_year'
        return self

    def clean_panel(self, df, in_col, excess):
        df = (df.pipe(no_double_space, in_col = in_col)
                .pipe(no_breaks, in_col = in_col))
        junk = '(@@|@ @|@ | @)'
        df[in_col] = (df[in_col].str.upper()
                                .str.strip()
                                    .str.replace("[.']", '')
                                    .str.replace(excess, '@')
                                    .str.replace('[,()*;:]', '@')
                                    .str.replace(junk, '@')
                                    .str.replace(junk, '@')
                                    .str.replace(junk, '@'))
        df[in_col] = df[in_col].str.split('@', expand = False)
        return df

    def judge_matcher(self, row, in_col, out_col, year_col, court_num_col,
                      size_col = None):
        final_list = []
        raw_list = row[in_col]
        cleaned_list = [x for x in raw_list if x]
        cleaned_list = [x.strip() for x in cleaned_list]
        for name in cleaned_list:
            name1 = self.convert_judge_name(row[year_col],
                                            row[court_num_col], name, 1)
            name2 = self.convert_judge_name(row[year_col],
                                            row[court_num_col], name, 2)
            if name1 in self.names_dict_list[0]:
                final_list.append(self.names_dict_list[0].get(name1))
            elif name1 in self.names_dict_list[1]:
                final_list.append(self.names_dict_list[1].get(name1))
            elif name2 in self.names_dict_list[2]:
                final_list.append(self.names_dict_list[2].get(name2))
            elif name2 in self.names_dict_list[3]:
                final_list.append(self.names_dict_list[3].get(name2))
            elif name in self.names_dict_list[4]:
                final_list.append(self.names_dict_list[4].get(name))
        final_list = list(unique_everseen(final_list))
        final_list.sort()
        row[out_col] = final_list
        if size_col and final_list:
            row[size_col] = len(final_list)
        return row

    def explode_panel(self, df, in_col, prefix):
        judge_df = df[in_col].apply(pd.Series)
        judge_df = judge_df.rename(columns = lambda x : prefix + str(x + 1))
        new_df = pd.concat([df[:], judge_df[:]], axis=1)
        return new_df

@dataclass
class FederalJudges(Judges):
    """
    Class for utilizing the Federal Judicial Center biographical and
    identifiation information with court opinion data.
    """
    paths : object
    settings : object
    stage : object

    def __post_init__(self):
        super().__post_init__()
        self.judicial_path = os.path.join(self.paths.external, 'judicial')
        self.federal_judicial_path = os.path.join(self.judicial_path,
                                                  'federal')
        self.fjc_bios = os.path.join(self.federal_judicial_path,
                                     'fjc_bios.csv')
        self.fjc_names = os.path.join(self.federal_judicial_path,
                                      'fjc_names.csv')
        self.bios_cols = {'full_name' : str, 'party' : float,
                          'pres_num' : float,
                          'recess' : bool,  'court_num' : int,
                          'circuit_num' : int, 'woman' : bool,
                          'minority' : bool, 'prosecutor' : bool,
                          'public_def' : bool, 'clerk' : bool,
                          'scotus_clerk' : bool, 'sg_office' : bool,
                          'law_prof' : bool, 'birth_year' : int,
                          'senior_status_date' : str, 'senate_per' : float,
                          'jcs' : float}
        self.bios_cols_list = list(self.bios_cols.keys())
        if self.stage == 'prep':
            self.circuit_nums_path = os.path.join(self.paths.dicts,
                                                  'circuit_nums.csv')
            self.court_nums_path = os.path.join(self.paths.dicts,
                                                'court_nums.csv')
            self.jcs_import = os.path.join(self.paths.dicts, 'jcs.csv')
            self.jcs_export =  os.path.join(self.federal_judicial_path,
                                            'jcs.csv')
            copyfile(self.jcs_import, self.jcs_export)
            self.employ_path = os.path.join(self.paths.dicts,
                                            'employ_hist.csv')
            self.circuit_nums = ReMatch(
                    file_path = self.circuit_nums_path,
                    encoding = self.settings['files']['encoding'],
                    out_type = 'int',
                    in_col = 'court',
                    out_col = 'circuit_num')
            self.court_nums = ReMatch(
                    file_path = self.court_nums_path,
                    encoding = self.settings['files']['encoding'],
                    out_type = 'int',
                    in_col = 'court',
                    out_col = 'court_num')
            self.jcs = ReMatch(file_path = self.jcs_export,
                               encoding = self.settings['files']['encoding'],
                               out_type = 'float',
                               in_col = 'nid',
                               out_col = 'jcs')
            self.employ = ReMatch(
                    file_path = self.employ_path,
                    encoding = self.settings['files']['encoding'],
                    in_col = 'career')

    def make_paths(self):
        """
        Due to a bug in pandas Excel importation, the current download paths
        are for .csv files instead of one .xlsx file containing all of the
        data in different sheets. The commented out sections of this module
        reference the .xlsx file which might be used once the pandas bug
        is fixed.
        """
#        self.fjc_url = 'https://www.fjc.gov/sites/default/files/history/categories.xlsx'
        self.career_url = 'https://www.fjc.gov/sites/default/files/history/professional-career.csv'
        self.demo_url = 'https://www.fjc.gov/sites/default/files/history/demographics.csv'
        self.jud_serv_url = 'https://www.fjc.gov/sites/default/files/history/federal-judicial-service.csv'
        self.career_import = os.path.join(self.federal_judicial_path,
                                          'fjc_career.csv')
        self.demo_import = os.path.join(self.federal_judicial_path,
                                        'fjc_demo.csv')
        self.jud_serv_import = os.path.join(self.federal_judicial_path,
                                            'fjc_jud_serv.csv')

        if not os.path.exists(self.judicial_path):
             os.makedirs(self.judicial_path)
        if not os.path.exists(self.federal_judicial_path):
             os.makedirs(self.federal_judicial_path)
        return self

    def make_files(self):

        if self.settings['prepper']['fjc_download']:
            file_download(self.career_url, self.career_import)
            file_download(self.demo_url, self.demo_import)
            file_download(self.jud_serv_url, self.jud_serv_import)

#        if self.settings.fjc_download:
#            file_download(self.fjc_url, self.fjc_import)
        """
        settings and columns for FJC biographical data.
        """
#        self.fjc_main_sheet = 'Federal Judicial Service'
#        self.fjc_career_sheet = 'Professional Career'
#        self.fjc_demographics_sheet = 'Demographics'
        self.demo_cols = ['nid', 'Last Name', 'First Name',
                          'Middle Name', 'Suffix',
                          'Birth Year', 'Gender',
                          'Race or Ethnicity']
        self.demo_renames = {'race_or_ethnicity' : 'race'}
        self.jud_serv_cols = ['nid', 'Judge Name',
                              'Court Name', 'Appointing President',
                              'Party of Appointing President',
                              'ABA Rating',
                              'Recess Appointment Date',
                              'Nomination Date',
                              'Senate Vote Type',
                              'Ayes/Nays',
                              'Commission Date',
                              'Senior Status Date',
                              'Termination Date']
        self.jud_serv_renames = {'judge_name' : 'full_name',
                                 'court_name' : 'court',
                                 'appointing_president' : 'president',
                                 'party_of_appointing_president' : 'party',
                                 'recess_appointment_date' : 'recess',
                                 'nomination_date' : 'nom_date',
                                 'senate_vote_type' : 'sen_vote_type',
                                 'ayes/nays' : 'sen_vote',
                                 'commission_date' : 'start_date',
                                 'senior status_date' : 'senior_date',
                                 'termination_date' : 'term_date'}
        self.career_cols = ['nid', 'Professional Career']
        self.career_renames = {'professional_career' : 'career'}
        self.col_drops = ['last_name', 'first_name', 'middle_name',	'suffix']
        """
        Imports specific sheets from FJC .xlsx file containing biographical
        information about judges.
        """
#        career_df = (pd.read_excel(self.fjc_import,
#                                   sheet_name = self.fjc_career_sheet,
        career_df = (pd.read_csv(self.career_import,
                                 usecols = self.career_cols,
                                 index_col = False,
                                 encoding = self.settings['files']['encoding'])
                        .rename(columns = str.lower)
                        .rename(columns = lambda x: x.replace(' ', '_'))
                        .rename(columns = self.career_renames)
                        .set_index('nid')
                        .groupby('nid')['career']
                        .apply(' '.join))

#        demo_df = (pd.read_excel(self.fjc_import,
#                                 sheet_name = self.fjc_demographics_sheet,
        demo_df = (pd.read_csv(self.demo_import,
                               usecols = self.demo_cols,
                               index_col = False,
                               encoding = self.settings['files']['encoding'])
                     .rename(columns = str.lower)
                     .rename(columns = lambda x: x.replace(' ', '_'))
                     .rename(columns = self.demo_renames)
                     .set_index('nid'))

#        jud_serv_df = (pd.read_excel(self.fjc_import,
#                                     sheet_name = self.fjc_main_sheet,
        self.df = (pd.read_csv(self.jud_serv_import,
                               usecols = self.jud_serv_cols,
                               index_col = False,
                               encoding = self.settings['files']['encoding'])
                     .rename(columns = str.lower)
                     .rename(columns = lambda x: x.replace(' ', '_'))
                     .rename(columns = self.jud_serv_renames)
                     .replace(['None (assignment)', 'None (reassignment)'],
                               method = 'ffill')
                     .pipe(self.fill_empty)
                     .astype(dtype = {'nid' : str})
                     .pipe(self.court_nums.match)
                     .pipe(self.circuit_nums.match)
                     .pipe(self.jcs.match, default = np.nan)
                     .astype(dtype = {'nid' : int})
                     .join(demo_df, on = 'nid', how = 'outer')
                     .join(career_df, on = 'nid', how = 'outer')
                     .astype(dtype = {'sen_vote' : str})
                     .pipe(self.encode_bio_data)
                     .apply(self.encode_senate_vote,
                            axis = 'columns')
                     .fillna('')
                     .pipe(self.employ.match)
                     .pipe(self.time_limit,
                           start_year = self.settings['general']['start_year'],
                           end_year = self.settings['general']['end_year'])
                     .drop(labels = ['career'],
                           axis = 'columns')
                     .set_index('nid')
                     .pipe(self.name_changes)
                     .apply(self.name_perms,
                           axis = 'columns')
                     .reset_index()
                     .drop(columns = self.col_drops))
        self.df.to_csv(self.fjc_bios,
                       encoding = self.settings['files']['encoding'],
                       index = False)
        self.reshape_names()
        return self

    def fill_empty(self, df):
        df['sen_vote_type'].replace(np.nan, method = 'ffill', inplace = True)
        df['sen_vote'].replace(np.nan, method = 'ffill', inplace = True)
        return df

    def encode_bio_data(self, df):
        df['recess'] = np.where(df['recess'].str.len() > 1, True, False)
        df['party'] = np.where(df['party'] == 'Democratic', -1, 1)
        df['aba_rating'] = (np.where(df['aba_rating']
                            == 'Exceptionally Well Qualified', 4,
                            np.where(df['aba_rating']
                                     == 'Well Qualified', 3,
                            np.where(df['aba_rating']
                                     == 'Qualified', 2,
                            np.where(df['aba_rating']
                                     == 'Not Qualified', 1, 0)))))
        df['woman'] = np.where(df['gender'] == 'Female', True, False)
        df['minority'] = (np.where(df['race'].str.contains('American')
                                   | df['race'].str.contains('Hispanic')
                                   | df['race'].str.contains('Pacific'), True,
                                   False))
        pres_dict = {'Barack Obama' : 44, 'Dwight D. Eisenhower' : 34,
                     'Franklin D. Roosevelt' : 32, 'George H.W. Bush' : 41,
                     'George W. Bush' : 43, 'Gerald Ford' : 38,
                     'Harry S Truman' : 33, 'Jimmy Carter' : 39,
                     'John F. Kennedy' : 35, 'Lyndon B. Johnson' : 36,
                     'Richard M. Nixon' : 37, 'Ronald Reagan' : 40,
                     'William J. Clinton' : 42}
        df['pres_num'] = df['president'].map(pres_dict)
        return df

    @staticmethod
    def encode_senate_vote(row):
        if row['sen_vote_type'] == 'Voice':
            row['senate_per'] = 1
        elif '/' in row['sen_vote'] and not '//' in row['sen_vote']:
            yeas, neas = row['sen_vote'].split('/')
            row['senate_per'] = int(yeas)/(int(yeas) + int(neas))
        else:
            row['senate_per'] = 0
        return row

    def time_limit(self, df, start_year, end_year):
        """
        Limits FJC data to time period when judge was on the bench based upon
        passed constants.
        """
        df['term_date'] = pd.to_datetime(df['term_date'])
        df['end_year'] = df['term_date'].dt.year
        df['end_year'] = df['end_year'].replace(np.nan, end_year)

        df['start_date'] = pd.to_datetime(df['start_date'])
        df['start_year'] = df['start_date'].dt.year

        df = df[df['end_year'] > start_year - 2]
        df = df[df['start_year'] <= end_year]
        return df

    def name_changes(self, df):
        """
        Adds rows for known omissions or errors in the FJC data.
        """
        def make_change(df, index_num, col_name, value):
            row = df.loc[index_num]
            row[col_name] = value
            return row
        df = df.append(make_change(df, 1386716, 'last_name', 'Randall'))
        df = df.append(make_change(df, 1382851, 'first_name', 'Sam'))
        return df

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

    def make_name_dicts(self):
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

    def make_bios_dict(self):
        self.bios_df = (pd.read_csv(
                self.fjc_bios,
                usecols = self.bios_cols_list,
                index_col = False,
                encoding = self.settings['files']['encoding'])
                           .fillna(''))
        self.bios_df['senior_year'] = (
                pd.to_datetime(self.bios_df['senior_status_date'],
                               errors = 'coerce').dt.year)
        self.bios_df['senior_year'].fillna(0, inplace = True)
        self.bios_df['senior_year'] = self.bios_df['senior_year'].astype(int)
        self.bios_cols.pop('senior_status_date')
        self.bios_cols.update({'senior_year' : int})
        self.bios_df['jcs'] = self.bios_df['jcs'].fillna(0)
        self.bios_df['jcs'] = pd.to_numeric(self.bios_df['jcs'],
                    errors = 'coerce')
#        self.bios_df['senior_status_date'] = (
#                pd.to_datetime(self.bios_df['senior_status_date'],
#                    format = '%m/%d/%Y', errors = 'ignore').astype(str))
#        self.bios_df['senior_status_date'] = (np.where(
#                self.bios_df['senior_status_date'],
#                self.bios_df['senior_status_date'][-4:], ''))
        self.bios_cols.pop('full_name')
        self.bios_cols_list = self.bios_cols.keys()
        self.bios = []
        temp_dict = {}
        for col in self.bios_cols_list:
            temp_dict = self.bios_df.set_index('full_name').to_dict()[col]
            self.bios.append(temp_dict)
        return self