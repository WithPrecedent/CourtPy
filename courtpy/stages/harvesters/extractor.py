"""
Parent class for CourtParser and CourtWrangler containing shared and specific
munging methods based upon the module using the Extractor.
"""
from dataclasses import dataclass
import os
import re

from library.case_tool import CaseTool
from library.combiners import Combiner
from library.externals import External
from simplify import ReMatch
from utilities.strings import no_breaks

@dataclass
class Extractor(CaseTool):

    paths : object
    settings : object

    def __post_init__(self):
        super().__post_init__()
        return

        return self

    def loop_cleanup(self):
        del(self.cases)
        return self

    def munge(self, df = None, bundle = None):
        if self.stage in ['wrangler']:
            df['court_num'].fillna(method = 'ffill', inplace = True,
                                   downcast = int)
            df['year'] = df['year'].replace(0, method = 'ffill').astype(int)
        for munger in self.cases.mungers:
            if self.stage in ['parser']:
                if munger.munge_type in ['general']:
                    if munger.source_col in ['opinions']:
                        df = munger.section_munger.match(
                                df = df,
                                in_string = bundle['opinions'])
                    else:
                        df = munger.section_munger.match(df = df)
                elif munger.munge_type in ['specific']:
                    df = munger.section_munger(df = df,
                                             bundle = bundle)
            elif self.stage in ['wrangler']:
                if munger.section in ['panel_judges', 'author', 'separate']:
                    df = munger.section_munger(df = df,
                                             judges = self.judges)
                elif munger.munge_type == 'general':
                    df = munger.section_munger.match(df = df)
                else:
                    df = munger.section_munger(df = df)
        if bundle:
            return df, bundle
        else:
            return df

    def initialize_judges(self, cases = None):
        if self.jurisdiction == 'federal':
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
        for combiner in self.combiners:
            df = combiner.section_combiner(df)
        return df

    def add_externals(self, df = None, cases = None):
        self.externals = []
        external_df = cases.rules.loc[cases.rules['external']]
        for i, row in external_df.iterrows():
            self.externals.append(External(section = row['key'],
                                           paths = self.paths,
                                           settings = self.settings))
        for external in self.externals:
            if external.section == 'judge_exp':
                    df = external.section_adder(df = df,
                                            judges = self.judges)
            else:
                df = external.section_adder(df)
        return df
