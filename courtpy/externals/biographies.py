
from dataclasses import dataclass
import os

import numpy as np
import pandas as pd

from .external import External
from utilities.rematch import ReMatch


@dataclass
class Biographies(External):

    jurisdiction : str = ''
    paths : object = None
    settings : object = None
    prep_message = 'Biographical data about judges prepared'
    wrangle_message = 'Biographical data about judges added to dataframe'

    def __post_init__(self):
        super().__post_init__()
        self.source_files = {'federal' : ['fjc_career.csv',
                                          'fjc_demographics.csv',
                                          'fjc_judicial_service.csv']}
        self.prepped_files = {'federal' : 'fjc_bios.csv'}
        self.prepper_options = {'federal' : self._munge_fjc_bios}
        self._dicts = {'career' : ['name', 'values'],
                       'demographics' : ['name', 'values'],
                       'judicial_experience' : ['name', 'values'],
                       'jcs' : ['name', 'score']}
        return self

    def _create_biographies(self):
        self.df = (pd.read_csv(
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

    def _create_rematches(self):
        self.circuit_nums = ReMatch(file_path = self.circuit_nums_path,
                                    encoding = self.encoding,
                                    out_type = 'int',
                                    in_col = 'court',
                                    out_col = 'circuit_num')
        self.court_nums = ReMatch(file_path = self.court_nums_path,
                                  encoding = self.encoding,
                                  out_type = 'int',
                                  in_col = 'court',
                                  out_col = 'court_num')
        self.jcs = ReMatch(file_path = self.jcs_path,
                           encoding = self.encoding,
                           out_type = 'float',
                           in_col = 'nid',
                           out_col = 'jcs')
        self.employ = ReMatch(file_path = self.employ_path,
                              encoding = self.encoding,
                              in_col = 'career')
        return self

    def _encode_bio_data(self, df):
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
    def _encode_senate_vote(row):
        if row['sen_vote_type'] == 'Voice':
            row['senate_per'] = 1
        elif '/' in row['sen_vote'] and not '//' in row['sen_vote']:
            yeas, neas = row['sen_vote'].split('/')
            row['senate_per'] = int(yeas)/(int(yeas) + int(neas))
        else:
            row['senate_per'] = 0
        return row

    def _fill_empty(self, df):
        df['sen_vote_type'].replace(np.nan, method = 'ffill', inplace = True)
        df['sen_vote'].replace(np.nan, method = 'ffill', inplace = True)
        return df


    def _munge_fjc_bios(self):

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
                               usecols = self.import_columns['demographics'],
                               index_col = False,
                               encoding = self.encoding)
                     .rename(columns = str.lower)
                     .rename(columns = lambda x: x.replace(' ', '_'))
                     .rename(columns = self.demo_renames)
                     .set_index('nid'))

#        jud_serv_df = (pd.read_excel(self.fjc_import,
#                                     sheet_name = self.fjc_main_sheet,
        self.df = (pd.read_csv(self.jud_serv_import,
                               usecols = self.jud_serv_cols,
                               index_col = False,
                               encoding = self.encoding)
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
        return self

    def _name_changes(self, df):
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

    def _set_source_variables(self):
        self.file_urls = {
                'career' : 'https://www.fjc.gov/sites/default/files/history/professional-career.csv',
                'demographics' : 'https://www.fjc.gov/sites/default/files/history/demographics.csv',
                'judicial_service' : 'https://www.fjc.gov/sites/default/files/history/federal-judicial-service.csv'}
        self.source_columns = {
                'career' : ['nid', 'Professional Career'],
                'demographics' : ['nid', 'Last Name', 'First Name',
                                  'Middle Name', 'Suffix', 'Birth Year',
                                  'Gender', 'Race or Ethnicity'],
                'judicial_service' : ['nid', 'Judge Name', 'Court Name',
                                      'Appointing President',
                                      'Party of Appointing President',
                                      'ABA Rating', 'Recess Appointment Date',
                                      'Nomination Date', 'Senate Vote Type',
                                      'Ayes/Nays', 'Commission Date',
                                      'Senior Status Date',
                                      'Termination Date']}
        self.renames = {
                'career' : {'professional_career' : 'career'},
                'demographics' : {'race_or_ethnicity' : 'race'},
                'judicial_service' : {
                        'judge_name' : 'full_name',
                        'court_name' : 'court',
                        'appointing_president' : 'president',
                        'party_of_appointing_president' : 'party',
                        'recess_appointment_date' : 'recess',
                        'nomination_date' : 'nom_date',
                        'senate_vote_type' : 'sen_vote_type',
                        'ayes/nays' : 'sen_vote',
                        'commission_date' : 'start_date',
                        'senior status_date' : 'senior_date',
                        'termination_date' : 'term_date'}}
        self.prepped_columns = {'full_name' : str, 'party' : float,
                                'pres_num' : float,
                                'recess' : bool,  'court_num' : int,
                                'circuit_num' : int, 'woman' : bool,
                                'minority' : bool, 'prosecutor' : bool,
                                'public_def' : bool, 'clerk' : bool,
                                'scotus_clerk' : bool, 'sg_office' : bool,
                                'law_prof' : bool, 'birth_year' : int,
                                'senior_status_date' : str,
                                'senate_per' : float, 'jcs' : float}
        self.circuit_nums_path = os.path.join(self.paths.dicts,
                                              'circuit_nums.csv')
        self.court_nums_path = os.path.join(self.paths.dicts,
                                            'court_nums.csv')
        self.jcs_path =  os.path.join(self.folder, 'jcs.csv')
        self.employ_path = os.path.join(self.folder, 'employ_hist.csv')
        return self

    def _time_limit(self, df, start_year, end_year):
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

    def include(self, instance, prefix = ''):
        if self.settings['general']['verbose']:
            print('Adding judicial biography data')
        self.sec_prefix = self.section + '_'
        judges.make_bios_dict()
        judge_cols = [col for col in df if col.startswith('judge_name')]
        for i, i_col in enumerate(judge_cols):
            for j, j_col in enumerate(judges.bios_cols_list):
                if judges.bios_cols.get(j_col) == bool:
                    prefix = 'judge_exp_'
                elif judges.bios_cols.get(j_col) == float:
                    prefix = 'judge_ideo_'
                elif judges.bios_cols.get(j_col) == str:
                    prefix = 'judge_attr_'
                elif judges.bios_cols.get(j_col) == int:
                    prefix = 'judge_demo_'
                df[prefix + j_col + str(i + 1)] = (
                    df['judge_name' + str(i + 1)]
                        .map(judges.bios[j]))
            df['judge_demo_age' + str(i + 1)] = (df['year']
                - df['judge_demo_birth_year' + str(i + 1)])
            df['judge_demo_senior_year' + str(i + 1)] = (
                    df['judge_demo_senior_year' + str(i + 1)]
                        .fillna(0).astype(int))
            df['judge_exp_senior' + str(i + 1)] = (np.where(
                    (df['judge_demo_senior_year' + str(i + 1)] > 0)
                    & (df['judge_demo_senior_year' + str(i + 1)]
                    <= df['year']), True, False))
            df['judge_demo_court_num' + str(i + 1)].fillna(0, inplace = True)
            df['judge_demo_court_num' + str(i + 1)] = (
                    df['judge_demo_court_num' + str(i + 1)].astype(int))
            df['judge_exp_designation' + str(i + 1)] = (
                    np.where((df['judge_demo_court_num' + str(i + 1)]
                             != df['court_num'].astype(int))
                             & (df['judge_demo_court_num' + str(i + 1)] > 0),
                             True, False))
            df['judge_exp_district' + str(i + 1)] = (
                    np.where(df['judge_demo_court_num' + str(i + 1)]
                             > 100, True, False))
        drop_prefixes = ['judge_demo_birth_year', 'judge_demo_senior_year',
                         'judge_demo_circuit_num', 'judge_demo_court_num']
        temp_list = []
        drop_list = []
        for pre in drop_prefixes:
            temp_list = [i for i in df if pre in i]
            drop_list.extend(temp_list)
        for i in drop_list:
                df.drop([col for col in df.columns if i in col],
                        axis = 'columns', inplace = True)
        remove_keys = ['birth_year', 'senior_year', 'circuit_num', 'court_num']
        for key in remove_keys:
            judges.bios_cols.pop(key)
        judges.bios_cols.update({'age' : int, 'senior' : bool,
                                 'designation' : bool, 'district' : bool})
        for key, value in judges.bios_cols.items():
            if value == bool:
                in_prefix = 'judge_exp_'
                out_prefix = 'panel_exp_'
            elif value == float:
                in_prefix = 'judge_ideo_'
                out_prefix = 'panel_ideo_'
            elif value == str:
                in_prefix = 'judge_attr_'
                out_prefix = 'panel_attr_'
            elif value == int:
                in_prefix = 'judge_demo_'
                out_prefix = 'panel_demo_'
            in_cols = [x for x in df if in_prefix + key in x]
            df[out_prefix + key] = (np.where(
                df['panel_size'] > 0,
                df[in_cols].sum(axis = 'columns'), 0))
        return df

    def prepare(self):
        self._set_paths()
        self._create_rematches()
        if self.prepper_options:
            self._import_source_file()
            self.prepper_options[self.jurisdiction]()
            self._export_prepped_file()
        return self

