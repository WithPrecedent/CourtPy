
from casesclasses import casesclass
from  more_itertools import unique_everseen
import numpy as np

from library.case_tool import CaseTool


@casesclass
class CaseEngineer(CaseTool):

    paths : object
    settings : object
    source : str = ''
    stage : str = ''

    def __post_init__(self):
        super().__post_init__()
        return

    def _drop_extra_labels(self):
        extra_outcomes = [i for i in self.cases.df if i.startswith('outcome_')]
        extra_outcomes.remove(self.label)
        drop_columns = self.cases.create_column_list(columns = extra_outcomes)
        self.cases.drop_columns(columns = drop_columns)
        return self

    def _drop_nonconforming_panels(self):
        if self.drop_no_judge:
            self.cases.df.query('panel_size != 0', inplace = True)
        if self.drop_en_banc:
            self.cases.df.query('panel_size < 4', inplace = True)
        if self.drop_small_panels:
            self.cases.df.query('panel_size > 2', inplace = True)
        return self

    def _drop_nonconforming_courts(self):
        self.cases.df['court_num'] = self.cases.df['court_num'].astype(int)
        court_level = self.jurisdiction + '_' + self.case_type
        min_court_num = ('court_num >= '
                         + self.cases.court_num_range[court_level][0])
        max_court_num = ('court_num <= '
                         + self.cases.court_num_range[court_level][1])
        self.cases.df.query(min_court_num, inplace = True)
        self.cases.df.query(max_court_num, inplace = True)
        self.cases.df['court_num'] = (
                self.cases.df['court_num'].astype('category'))
        return self

    def _drop_crim_or_civ(self):
        drop_cols = []
        if self.drop_crim:
            self.cases.df.query('type_criminal != 0',
                                inplace = True)
            drop_cols.extend(['type_criminal', 'type_crim_d_appeal'])
        elif self.drop_civ:
            self.cases.df.query('type_criminal != 1',
                                inplace = True)
            drop_cols.extend(['type_criminal', 'type_civ_d_appeal'])
        self.cases.drop_columns(columns = drop_cols)
        return self

    def _drop_nonqual_jcs(self):
        if self.drop_jcs_unqual:
            pass
        return self

    def _judge_stubs(self):
        judge_cols = (
                [c for c in self.cases.df.columns if c.startswith('judge_')])
        judge_cols = [col.rstrip('1234567890.') for col in judge_cols]
        judge_stubs = list(unique_everseen(judge_cols))
        return judge_stubs

    def merge_casesframes(self, df1, df2):
        df = df1
        return df

    def cull_cases(self, drop_prefixes = []):
        drop_cols = []
        self._drop_extra_labels()
        self._drop_nonconforming_panels()
        self._drop_nonconforming_courts()
        self._drop_crim_or_civ()
        self._drop_nonqual_jcs()
        drop_prefixes.extend(['panel_ideo_pres_num'])
        drop_cols = self.cases.create_column_list(prefixes = drop_prefixes)
        self.cases.drop_columns(columns = drop_cols)
        return self

    def shape_df(self):
        stubs = self._judge_stubs()
        wide_drop_list = []
        if self.shape == 'long':
            self.cases.reshape_long(stubs = stubs,
                                    id_col = 'index_universal',
                                    new_col = 'panel_position')
            self.cases.drop_columns(columns = ['panel_position'])
            panel_cols = [c for c in self.cases.df if c.startswith('panel_')]
            panel_drop_cols = ['panel_judges_list', 'panel_size']
            panel_cols = [c for c in panel_cols if c not in panel_drop_cols]
            for col in panel_cols:
                corr_judge_col = 'judge_' + col[len('panel_'):]
                self.cases.df[col] = (
                        self.cases.df[col] - self.cases.df[corr_judge_col])
                self.cases.df[col] = (
                        self.cases.df[col] / (self.cases.df['panel_size'] - 1))
            self.cases.df['panel_judges_list'] = self.cases.df.apply(
                    self.remove_judge_name,
                    axis = 'columns')
            self.cases.df = (
                    self.cases.df[self.cases.df['judge_name'].str.len() > 1])
            if self.iso_votes:
                self.cases.df[self.label] = np.where(((
                        self.cases.df['judge_vote_dissent']
                        & self.cases.df[self.label])
                        | (~self.cases.df['judge_vote_dissent']
                        & ~ self.cases.df[self.label])),
                        False, True)
        elif self.shape == 'wide':
            stubs.remove('judge_name')
            wide_drop_list = self.cases.create_column_list(prefixes = stubs)
        drop_list = self.cases.create_column_list(prefixes = ['judge_vote'])
        drop_list.append('panel_size')
        if wide_drop_list:
            drop_list.extend(wide_drop_list)
        self.cases.drop_columns(columns = drop_list)
        return self

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

    def engineer_loose_ends(self):
        self.cases.df.rename({'judge_demo_party' : 'judge_ideo_party',
                              'panel_demo_party' : 'panel_ideo_party'},
                             axis = 'columns',
                             inplace = True)
        if self.en_banc:
            for i in range(4, 30):
                if 'judge_name' + str(i) in self.cases.df.columns:
                    self.cases.drop_columns(columns = 'judge_name' + str(i))
        return self