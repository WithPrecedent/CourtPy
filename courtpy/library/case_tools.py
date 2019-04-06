"""
Parent class for CourtParser, CourtWrangler, CourtMerger, CourtEngineer,
and CourtAnalyzer.
Contains shared methods that can be accessed by all child classes.
"""

from dataclasses import dataclass
from  more_itertools import unique_everseen
import numpy as np
import os
import re

from library.cases import Cases
from library.combiners import Combiner
from library.dividers import Divider
from library.externals import External
from library.mungers import Munger
from ml_funnel.data import Data
from utilities.rematch import ReMatch
from utilities.strings import no_breaks

@dataclass
class CaseTools(object):
    
    paths : object
    settings : object
    source : str = ''
    stage : str = ''   
      
    def __post_init__(self):
        pass
        return
    
    def check_sources(self):
        sources = []
        self.paths.stage = self.stage
        if self.settings['parser']['lexis_cases']:
            sources.append('lexis_nexis')
        if self.settings['parser']['court_listener_cases']:
            sources.append('court_listener')
        if self.settings['parser']['caselaw_access_cases']:
            sources.append('caselaw_access')
        return sources

    def create_divider_list(self, df = None, cases = None):
        self.dividers_table = ReMatch(file_path = cases.dividers_file, 
                                      reverse_dict = True)
        self.dividers = []
        divider_df = cases.rules.loc[
                cases.rules['divide_source'] != 'none']
        divider_df = divider_df.loc[cases.rules[self.source]]
        for i, row in divider_df.iterrows():
            regex = self.dividers_table.lookup[row['key'] + '_div']
            self.dividers.append(Divider(section = row['key'],
                                 source = self.source,
                                 data_type = row['data_type'],
                                 source_section = row['divide_source'],
                                 regex = regex))
        return self
    
    def create_column_dicts(self, df = None, cases = None):
        """
        Creates a complete dictionary for a pandas series index or
        dataframe columns with data type defaults.
        """
        self.column_dict = {cases.index_col : int}
        meta_dict = self.add_prefix_dict(cases.meta_cols, 'meta')
        self.column_dict.update(meta_dict)
        self.column_dict.update({'court_num' : int, 'date_decided' : str, 
                                 'date_filed' : str, 'date_argued' : str, 
                                 'date_amended' : str, 'date_submitted' : str, 
                                 'year' : int})
        for tool in self.dividers:
            self.column_dict.update({tool.sec_col : str})
        self.column_dict.update({'separate_concur' : list, 
                                 'separate_dissent' : list})
        for tool in self.mungers:
            if (tool.data_type in ['bool', 'matches'] 
                    and tool.munge_type == 'general'):
                tool.col_list = list(tool.section_munger.lookup['values'])
                tool.col_dict = {k : bool for k in tool.col_list}  
                self.column_dict.update(tool.col_dict)
        self.column_dict.update({'refer_admin' : list, 
                                 'refer_precedent' : list,
                                 'refer_statute' : list})
        return self
    
    @staticmethod   
    def add_prefix_dict(i_dict, prefix):
        return {f'{prefix}_{k}' : v for k, v in i_dict.items()}  
    
    def separate_header(self, df = None, cases_text = None, bundle = None):
        """
        Divides court opinion into header and opinions divider. To avoid
        data extraction errors, it is essential to parse the header and 
        opinions separately.
        """
        if re.search(self.dividers_table.lookup['op_div'], cases_text):
            op_list = re.split(self.dividers_table.lookup['op_div'], 
                               cases_text)
            if len(op_list) > 0:
                bundle.header = op_list[0]
                if len(op_list) > 1:
                    bundle.opinions = no_breaks(''.join(op_list[1:]))
                    bundle.opinions_breaks = (''.join(op_list[1:]))  
        else:
            bundle.header = cases_text
            bundle.opinions = 'none'
            bundle.opinions_breaks = 'none'
        if self.source == 'lexis_nexis':
            bundle.header = re.sub(self.dividers_table.lookup['lex_pat'], 
                                   '', bundle.header)  
        return df, bundle
    
    def divide(self, df = None, bundle = None):
        for tool in self.dividers:
            df, bundle = (
                    tool.section_divider(df = df, 
                                         bundle = bundle, 
                                         dividers_table = self.dividers_table))
        return df, bundle
    
    def create_munger_list(self, cases = None):
        self.mungers = []
        munger_df = cases.rules.loc[cases.rules['munge_stage'] == self.stage]
        munger_df = munger_df.loc[cases.rules[self.source]]
        if self.stage == 'parse':
            Munger.dividers_table = self.dividers_table
        elif self.stage == 'wrangle':
            Munger.dividers_table = ReMatch(
                    file_path = cases.dividers_file, 
                    reverse_dict = True, 
                    compile_keys = False)
        for i, row in munger_df.iterrows():
            self.mungers.append(Munger(settings = self.settings,
                                       dicts_path = self.paths.dicts,
                                       source = self.source,
                                       section = row['key'],
                                       data_type = row['data_type'],
                                       munge_type = row['munge_type'],
                                       munge_file = row['munge_file'],
                                       source_col = row['munge_source']))                       
        return self  
    
    def quick_start(self):
        self.paths.stage = self.stage
        self.paths.conform(stage = self.stage, 
                           source = self.source)
        self.cases = Cases(paths = self.paths, 
                           settings = self.settings, 
                           source = self.source, 
                           stage = self.stage)
        self.data = Data(settings = self.settings,
                         quick_start = True,
                         import_path = self.paths.import_path)
        if self.stage in ['parse', 'wrangle', 'merge']:
            self.data.column_types(bool_prefixes = self.cases.bool_prefixes,
                                   list_prefixes = self.cases.list_prefixes,
                                   float_prefixes = self.cases.float_prefixes,
                                   int_prefixes = self.cases.int_prefixes,
                                   str_prefixes = self.cases.str_prefixes)
        elif self.stage in ['engineer', 'analyze', 'plot']:
            self.data.column_types(bool_prefixes = self.cases.bool_prefixes,
                                   cat_prefixes = self.cases.cat_prefixes,
                                   float_prefixes = self.cases.float_prefixes,
                                   int_prefixes = self.cases.int_prefixes,
                                   interact_prefixes = (
                                           self.cases.interact_prefixes))
        self.data.smart_fill_na()
        return self
    
    def loop_cleanup(self):
        del(self.data)
        del(self.cases)
        return self
        
    def create_section_list(self, cases = None):
        if self.source:
            self.prefix_secs = (
                cases.rules.loc[cases.rules[self.source]]['key'].tolist())
        else:
            self.prefix_secs = cases.rules['key'].tolist() 
        self.section_list = self.create_column_list(self.prefix_secs) 
        return self 
    
    def munge(self, df = None, bundle = None):
        if self.stage == 'wrangle':
            df['court_num'].fillna(method = 'ffill', inplace = True,
                                   downcast = int)
            df['year'] = df['year'].replace(0, method = 'ffill').astype(int)
        for tool in self.mungers:
            if self.stage == 'parse':
                if tool.munge_type == 'general':
                    if tool.source_col == 'opinions':
                        df = tool.section_munger.match(
                                df = df, 
                                in_string = bundle.opinions)
                    else:
                        df = tool.section_munger.match(df = df)
                elif tool.munge_type == 'specific':
                    df = tool.section_munger(df = df, 
                                             bundle = bundle)
            elif self.stage == 'wrangle':
                if tool.section in ['panel_judges', 'author', 'separate']:
                    df = tool.section_munger(df = df, 
                                             judges = self.judges)
                elif tool.munge_type == 'general':
                    df = tool.section_munger.match(df = df)
                else:
                    df = tool.section_munger(df = df)
        return df, bundle
    
    def initialize_judges(self, cases = None):
        if self.settings['cases']['jurisdiction'] == 'federal':
            from library.judges import FederalJudges
            self.judges = FederalJudges(self.paths, self.settings, self.stage)
            self.judges.make_name_dicts()
        excess_table_file = (
                cases.rules[cases.rules['key'] == 'panel_judges']
                    ['munge_file'].item())
        excess_table_path = os.path.join(self.paths.dicts, excess_table_file)
        self.excess_table = ReMatch(file_path = excess_table_path,
                                    reverse_dict = True)
        return self
    
    def combine(self, df = None, cases = None):
        self.combiners = []
        combiner_df = cases.rules.loc[cases.rules['combiner']]
        for i, row in combiner_df.iterrows():
            self.combiners.append(Combiner(settings = self.settings,
                                           dicts_path = self.paths.dicts,
                                           section = row['key'],
                                           data_type = row['data_type'],
                                           munge_file = row['munge_file'])) 
        for tool in self.combiners:
            df = tool.section_combiner(df)                      
        return df  
    
    def add_externals(self, df = None, cases = None):
        self.externals = []
        external_df = cases.rules.loc[cases.rules['external']]
        for i, row in external_df.iterrows():
            self.externals.append(External(section = row['key'],
                                           paths = self.paths,
                                           settings = self.settings)) 
        for tool in self.externals:
            if tool.section == 'judge_exp':
                    df = tool.section_adder(df = df, 
                                            judges = self.judges)
            else:
                df = tool.section_adder(df)                     
        return df     
    
    def merge_dataframes(self, df1, cases1, df2, cases2):
        df = df1
        return df
    
    def cull_data(self, data = None, drop_prefixes = []):
        if self.settings['drops']['no_judge']:
            data.df.query('panel_size != 0', 
                          inplace = True)
        if self.settings['drops']['en_banc']:
            data.df.query('panel_size < 4', 
                          inplace = True)  
        if self.settings['drops']['small_panels']:
            data.df.query('panel_size > 2', 
                          inplace = True) 
        drop_prefixes.extend(['panel_ideo_pres_num'])
        extra_outcomes = [i for i in data.df if i.startswith('outcome_')]
        extra_outcomes.remove(self.settings['engineer']['label'])
        drop_list = data.create_column_list(data.df, drop_prefixes, 
                                            extra_outcomes)
        data.df.query('court_num < 14', 
                      inplace = True)
        if self.settings['drops']['crim']:
            data.df.query('type_criminal != 0', 
                          inplace = True)
            drop_list.extend(['type_criminal', 'type_crim_d_appeal'])
        elif self.settings['drops']['civ']:
            data.df.query('type_criminal != 1', 
                          inplace = True) 
            drop_list.extend(['type_criminal', 'type_civ_d_appeal'])
        if self.settings['drops']['jcs_unqual']:
            pass
        data.df.drop(drop_list, axis = 'columns', 
                     inplace = True)
        return data
    
    def _judge_stubs(self, df):
        judge_cols = [col for col in df.columns if col.startswith('judge_')]
        judge_cols = [col.rstrip('1234567890.') for col in judge_cols]
        judge_stubs = list(unique_everseen(judge_cols))
        return judge_stubs
    
    def shape_df(self, data):
        stubs = self._judge_stubs(data.df)
        wide_drop_list = []
        if self.settings['engineer']['shape'] == 'long':
            data.reshape_long(stubs = stubs, 
                              id_col = 'index_universal', 
                              new_col = 'panel_position')
            data.df.drop('panel_position', axis = 'columns', inplace = True)
            panel_cols = [i for i in data.df if i.startswith('panel_')]
            panel_drop_cols = ['panel_judges_list', 'panel_size']
            panel_cols = [i for i in panel_cols if i not in panel_drop_cols]
            for col in panel_cols:
                corr_judge_col = 'judge_' + col[len('panel_'):]
                data.df[col] = data.df[col] - data.df[corr_judge_col] 
                data.df[col] = data.df[col] / (data.df['panel_size'] - 1)
            data.df['panel_judges_list'] = data.df.apply(
                    self.remove_judge_name, 
                    axis = 'columns')
            data.df = data.df[data.df['judge_name'].str.len() > 1]
            label = self.settings['funnel']['label']
            if self.settings['engineer']['iso_votes']:
                data.df[label] = np.where(((data.df['judge_vote_dissent']
                                            & data.df[label])
                                            | (~data.df['judge_vote_dissent']
                                            & ~ data.df[label])), 
                                            False, True)
        elif self.settings['engineer']['shape'] == 'wide':
            stubs.remove('judge_name')
            wide_drop_list = data.create_column_list(df = data.df, 
                                                     prefixes = stubs)
        drop_list = data.create_column_list(df = data.df, 
                                            prefixes = ['judge_vote'])
        drop_list.append('panel_size')
        if wide_drop_list:
            drop_list.extend(wide_drop_list)
        data.df.drop(drop_list, 
                     axis = 'columns', 
                     inplace = True)
        return data
    
    def remove_judge_name(self, row):
        temp_list = ''
        if row['judge_name']:
            if str(row['judge_name']) in row['panel_judges_list']:
                temp_list = (
                    row['panel_judges_list'].replace(str(row['judge_name']), 
                                                         ''))
                temp_list = temp_list.replace(",'',", ",").strip()
                temp_list = temp_list.replace("'', ", "").strip()
                temp_list = temp_list.replace(", ''", "").strip()
        return temp_list         
    
    def engineer_loose_ends(self, df = None):
        df.rename({'judge_demo_party' : 'judge_ideo_party',
                   'panel_demo_party' : 'panel_ideo_party'},
                   axis = 'columns', 
                   inplace = True)
        if self.settings['drops']['en_banc']:
            for i in range(4, 30):
                if 'judge_name' + str(i) in df.columns:
                    df.drop('judge_name' + str(i), 
                            axis = 'columns', 
                            inplace = True)
        return df
    
    def create_splices(self, data):
        data.add_splice(group_name = 'panels', 
                        prefixes = ['panel_judges'])
        data.add_splice(group_name = 'jcs', 
                        prefixes = ['panel_ideo_jcs'])
        data.add_splice(group_name = 'presidents', 
                        prefixes = ['panel_ideo_party'])
        data.add_splice(group_name = 'demographics', 
                        prefixes = ['panel_demo_'])
        data.add_splice(group_name = 'experience', 
                        prefixes = ['panel_exp_'])
        data.add_splice(group_name = 'politics', 
                        prefixes = ['pol_'])
        data.add_splice(group_name = 'judges', 
                        prefixes = ['judge_']) 
        return data