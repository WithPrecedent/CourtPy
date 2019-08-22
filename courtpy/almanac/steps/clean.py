"""
Primary class for the CourtWrangler. This part of the CourtWrangler pipeline
divides the court opinions into the key sections and extracts a few critical
"""
from dataclasses import dataclass
import os

from simplify import timer
from simplify.almanac.steps import Clean


@timer('Deep parsing and data wrangling')
@dataclass
class CPClean(Clean):

    technique : str = ''
    techniques : object = None
    parameters : object = None
    auto_prepare : bool = True
    name : str = 'cleaner'

    def __post_init__(self):
        super().__post_init__()
        sources = self.check_sources()
        for source in sources:
            self.source = source
            self.quick_start()
            self.initialize_judges(cases = self.cases)
            self.create_munger_list(cases = self.cases)
            self.data.df = self.munge(df = self.data.df)
            self.data.df = self.combine(df = self.data.df,
                                        cases = self.cases)
            self.data.df = self.add_externals(df = self.data.df,
                                              cases = self.cases)
            self.data.df = (
                self.data.df.loc[:,
                    ~self.data.df.columns.str.startswith('temp_')])
            self.data.df = (
                self.data.df.loc[:,
                    ~self.data.df.columns.str.startswith('sec_')])
            self.data.save(export_folder = self.filer.data_folder,
                           file_name = self.inventory.export_file,
                           file_format = self.menu['files']['data_out'],
                           boolean_out = self.menu['files']['boolean_out'],
                           encoding = self.menu['files']['encoding'])
            self.loop_cleanup()
        return self


    def _munge_dates(self, df, bundle):
        """
        Parses dates that a case was decided, submitted, argued, amended, and
        filed. This parsing is only done certain sources of data.
        Most case do not include all dates and dates are formatted
        inconsistently. As a result, the general matcher and munger methods
        cannot be used.
        """
        dates_dict = {'DECIDED' : 'date_decided',
                      'FILED' : 'date_filed',
                      'AMENDED' : 'date_amended',
                      'SUBMITTED' : 'date_submitted',
                      'ARGUED' : 'date_argued'}
        date_list = df['section_' + self.section]
        date_list = [date_list.upper() for date_list in date_list]
        if date_list:
            for a_date in date_list:
                for key, value in dates_dict.items():
                    if key in a_date:
                        df[value] = a_date
        for key, value in dates_dict.items():
            if df[value]:
                df['year'] = str(df[value])[-4:]
                break
        if not df['year']:
            date_string = ''.join(date_list)
            if re.search(self.date_pat, date_string):
                df['year'] = (
                        re.search(self.date_pat, date_string).group(0)[-4:])
        return df


    def _munge_dates(self, df, bundle):
        """
        Parses dates that a case was decided, submitted, argued, amended, and
        filed. This parsing is only done certain sources of data.
        Most case do not include all dates and dates are formatted
        inconsistently. As a result, the general matcher and munger methods
        cannot be used.
        """
        dates_dict = {'DECIDED' : 'date_decided',
                      'FILED' : 'date_filed',
                      'AMENDED' : 'date_amended',
                      'SUBMITTED' : 'date_submitted',
                      'ARGUED' : 'date_argued'}
        date_list = df['section_' + self.section]
        date_list = [date_list.upper() for date_list in date_list]
        if date_list:
            for a_date in date_list:
                for key, value in dates_dict.items():
                    if key in a_date:
                        df[value] = a_date
        for key, value in dates_dict.items():
            if df[value]:
                df['year'] = str(df[value])[-4:]
                break
        if not df['year']:
            date_string = ''.join(date_list)
            if re.search(self.date_pat, date_string):
                df['year'] = (
                        re.search(self.date_pat, date_string).group(0)[-4:])
        return df

    def _munge_parties(self, df):
        if self.settings['general']['verbose']:
            print('Identifying types and roles of litigation parties')
        party_pat = self.dividers_table['party1_div']
        excess = self.dividers_table['party_excess']
        df[[self.section_prefix + 'name1',
                       self.section_prefix + 'name2']] = (
                df['section_' + self.section].str.split(
                        party_pat, expand = True, n = 1))
        df[self.section_prefix + 'name2'].fillna('', inplace = True)
        for i in range(1, 3):
            party_munger = ReMatch(file_path = self.munge_path,
                                   out_type = self.data_type,
                                   in_col = 'party_name' + str(i),
                                   out_prefix = self.section_prefix,
                                   out_suffix = str(i))
            df = party_munger.match(df = df)
        return df

    def _munge_panel(self, df, judges):
        if self.settings['general']['verbose']:
            print('Identifying judges assigned to cases')
        size_column = 'panel_size'
        fixed_column = 'panel_judges_list'
        name_prefix = 'judge_name'
        excess = self.excess_table['judge_excess']
        df = judges.clean_panel(df, self.source_col, excess)
        df = df.apply(judges.judge_matcher,
                      in_col = self.source_col,
                      out_col = fixed_column,
                      court_num_col = 'court_num',
                      year_col = 'year',
                      size_col = size_column,
                      axis = 'columns')
        df = judges.explode_panel(df, fixed_column, name_prefix)
        return df

    def _munge_author(self, df, judges):
        if self.settings['general']['verbose']:
            print('Determining authors of opinions')
        fixed_column = 'temp_author'
        name_prefix = self.section_prefix + 'name'
        excess = self.excess_table['author_excess']
        df = judges.clean_panel(df, self.source_col, excess)
        df = df.apply(judges.judge_matcher,
                       in_col = self.source_col,
                       out_col = fixed_column,
                       court_num_col = 'court_num',
                       year_col = 'year',
                       axis = 'columns')
        df = judges.explode_panel(df, fixed_column, name_prefix)
        return df

    def _clean_panel(self, df, in_col, excess):
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

    def _judge_matcher(self, row, in_col, out_col, year_col, court_num_col,
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

    def _explode_panel(self, df, in_col, prefix):
        judge_df = df[in_col].apply(pd.Series)
        judge_df = judge_df.rename(columns = lambda x : prefix + str(x + 1))
        new_df = pd.concat([df[:], judge_df[:]], axis=1)
        return new_df
        return df

    def _munge_separate(self, df, judges):
        if self.settings['general']['verbose']:
            print('Determining authors of concurring and dissenting opinions')
        concur_source_col = 'separate_concur'
        dissent_source_col = 'separate_dissent'
        concur_fixed_col = 'temp_concur'
        dissent_fixed_col = 'temp_dissent'
        concur_name_prefix = self.section_prefix + 'concur'
        dissent_name_prefix = self.section_prefix + 'dissent'
        excess = self.excess_table['concur_excess']
        df = judges.clean_panel(df, concur_source_col, excess)
        df = df.apply(judges.judge_matcher,
                      in_col = concur_source_col,
                      out_col = concur_fixed_col,
                      court_num_col = 'court_num',
                      year_col = 'year',
                      axis = 'columns')
        df = judges.explode_panel(df, concur_fixed_col, concur_name_prefix)
        df = judges.clean_panel(df,
                                dissent_source_col,
                                excess)
        df = df.apply(judges.judge_matcher,
                      in_col = dissent_source_col,
                      out_col = dissent_fixed_col,
                      court_num_col = 'court_num',
                      year_col = 'year',
                      axis = 'columns')
        df = judges.explode_panel(df, dissent_fixed_col, dissent_name_prefix)
        return df


    def __post_init__(self):
        self.origins = {'judge_vote' : ['judge_name'],
                        'agency' : ['sec_history', 'party_name1',
                                    'party_name2'],
                        'type_published' : ['notice_unpub_rule', 'sec_cite'],
                        'type_criminal' : ['sec_docket', 'sec_history',
                                           'counsel_us_atty', 'criminal_',
                                           'party_us1', 'party_us2'],
                        'outcome' : ['sec_disposition', 'disposition_op'],
                        'references' : ['admin_cites', 'statute_cites',
                                        'case_cites', 'other_cites']}
        self.munge_path = os.path.join(self.dicts_path, self.munge_file)
        self.sec_prefix = self.section + '_'
        if self.section == 'type':
            self.section_combiner = self.determine_type
        elif self.section == 'judge_vote':
            self.section_combiner = self.match_votes
        elif self.section == 'panel_vote':
            self.section_combiner = self.aggregate_votes
        elif self.section == 'agency':
            self.section_combiner = self.determine_agency
        elif self.section == 'outcome':
            self.section_combiner = self.chicken_dinner
        elif self.section == 'refer':
            self.section_combiner = self.unpack_references

    def determine_type(self, df):
        if self.settings['general']['verbose']:
            print('Determining precedental status')
        df[self.sec_prefix + 'published'] = (
                np.where(df['notice_unpub_rule'], False,
                    np.where(df['sec_cite'].str.contains('F\. ?(2d|3d)',
                             flags = re.IGNORECASE), True, False)))
        if self.settings['general']['verbose']:
            print('Classifying cases as criminal or civil')
        crim_civ_docket = ReMatch(file_path = self.munge_path,
                                  in_col = 'sec_docket',
                                  out_type = self.data_type,
                                  out_prefix = 'temp_docket_')
        crim_civ_history = ReMatch(file_path = self.munge_path,
                                   in_col = 'sec_history',
                                   out_type = self.data_type,
                                   out_prefix = 'temp_history_')
        df = crim_civ_docket.match(df = df)
        df = crim_civ_history.match(df = df)
        crim_issues = [col for col in df if col.startswith('crim_')]
        df['temp_crim_issue'] = np.where(
                df[crim_issues].any(axis = 1), True, False)
        df[self.sec_prefix + 'criminal'] = (
                np.where((df['temp_docket_crim']
                   & (df['party_us1'] | df['party_us2'])),
                   True,
                   np.where((df['temp_history_crim']
                   & (df['party_us1'] | df['party_us2'])),
                   True,
                   np.where(df['counsel_us_atty']
                   & (df['party_us1'] | df['party_us2']),
                   True,
                   np.where(df['temp_crim_issue']
                   & (df['party_us1'] | df['party_us2']),
                   True, False)))))
        df['party_pros1'] = (
                np.where(df[self.sec_prefix + 'criminal']
                         & df['party_us1'], True, False))
        df['party_crimd2'] = (
                np.where(df['party_pros1'], True, False))
        df['party_pros2'] = (
                np.where(df[self.sec_prefix + 'criminal']
                         & df['party_us2'], True, False))
        df['party_crimd1'] = (
                np.where(df['party_pros2'], True, False))
        df['party_civp1'] = (
                np.where(~df[self.sec_prefix + 'criminal']
                & df['party_plaint1'], True, False))
        df['party_civd2'] = (
                np.where(df['party_plaint1'], True, False))
        df['party_civp2'] = (
                np.where(~df[self.sec_prefix + 'criminal']
                & df['party_plaint2'], True, False))
        df['party_civd1'] = (
                np.where(df['party_civp2'], True, False))
        return df

    def match_votes(self, df):
        if self.settings['general']['verbose']:
            print('Determining judge voting alignments')
        judge_cols = [col for col in df if col.startswith('judge_name')]
        for i, col in enumerate(judge_cols):
            df[self.sec_prefix + 'author' + str(i + 1)] = (
                np.where(df[col].isin(df['author_name1']
                    .astype(str)), True, False))
            df[self.sec_prefix + 'concur' + str(i + 1)] = (
                np.where(df[col].isin(df['temp_concur']
                    .astype(str)), True, False))
            df[self.sec_prefix + 'dissent' + str(i + 1)] = (
                np.where(df[col].isin(df['temp_dissent']
                    .astype(str)), True, False))
        return df

    def aggregate_votes(self, df):
        pass
        return df

    def determine_agency(self, df):
        if self.settings['general']['verbose']:
            print('Determining executive agency involvement')
        agencies = ReMatch(file_path = self.munge_path,
                           out_type = self.data_type)
        df = agencies.match(df = df,
                            in_col = 'party_name1',
                            out_col = 'temp_agency_party_1',
                            default = 'None')
        df = agencies.match(df = df,
                            in_col = 'party_name2',
                            out_col = 'temp_agency_party_2',
                            default = 'None')
        df = agencies.match(df = df,
                            in_col = 'sec_history',
                            out_col = 'temp_agency_history',
                            default = 'None')
        df[self.sec_prefix + 'name'] = np.where(
                df['temp_agency_history'] != 'None', df['temp_agency_history'],
                    np.where(df['temp_agency_party_1'] != 'None',
                             df['temp_agency_party_1'],
                             np.where(df['temp_agency_party_2'] != 'None',
                                      df['temp_agency_party_2'], 'None')))
        return df

    def linked_cols(self, df, in_col, out_col):
        df[out_col] = np.where(df[in_col], True, df[out_col])
        return df

    def chicken_dinner(self, df):
        win_below = 'temp_won_below'
        win_appeal = 'temp_win_appeal'
        crim_d_appeal = 'type_crim_d_appeal'
        civ_d_appeal = 'type_civ_d_appeal'
        crim_d_win = 'crim_d_win'
        civ_d_win = 'civ_d_win'
        crim_p_win = 'crim_p_win'
        civ_p_win = 'civ_p_win'
        if self.settings['general']['verbose']:
            print('Completing party variable coding')
        df = self.linked_cols(df, 'party_resp2',
                              'party_petit1')
        df = self.linked_cols(df, 'party_resp1',
                              'party_petit2')
        df = self.linked_cols(df, 'party_appnt1',
                              'party_appee2')
        df = self.linked_cols(df, 'party_appnt2',
                              'party_appee1')
        df = self.linked_cols(df, 'party_plaint2',
                              'party_defend1')
        df = self.linked_cols(df, 'party_plaint1',
                              'party_defend2')
        if self.settings['general']['verbose']:
            print('Determining case outcomes')
        df[self.sec_prefix + 'reversal'] = (
                np.where(df['disposition_reverse']
                         | df['disposition_vacate']
                         | df['disposition_remand'],
                         True,
                         np.where(df['disposition_op_reversed']
                                  | df['disposition_op_vacate']
                                  | df['disposition_op_remand'],
                                  True, False)))
        for i in range(1, 3):
            df[win_below + str(i)] = (
                    np.where((df['party_petit' + str(i)]
                              | df['party_appnt' + str(i)])
                              & (~df['party_resp' + str(i)])
                              & (~df['party_appee' + str(i)]),
                              True, False))
            df[win_appeal + str(i)] = (
                    np.where((df[self.sec_prefix + 'reversal']
                              & ~df[win_below + str(i)])
                              | (~df[self.sec_prefix + 'reversal']
                              & df[win_below + str(i)]),
                              True, False))
        df[crim_d_appeal] = (
                np.where(df['type_criminal']
                          & ((df['party_appnt1']
                              & df['party_crimd1'])
                             | (df['party_appnt2']
                                & df['party_crimd2'])), True, False))
        df[civ_d_appeal] = (
                np.where(~df['type_criminal']
                          & ((df['party_appnt1']
                              & df['party_civd1'])
                             | (df['party_appnt2']
                                & df['party_civd2'])), True, False))
        df[self.sec_prefix + 'reversal_' + crim_d_win] = (
                np.where(df['type_criminal']
                          & ((df[self.sec_prefix + 'reversal']
                              & df['party_appnt1']
                              & df['party_crimd1'])
                             | (df[self.sec_prefix + 'reversal']
                                & df['party_appnt2']
                                & df['party_crimd2'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee1']
                                & df['party_crimd1'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee2']
                                & df['party_crimd2'])), True, False))
        df[self.sec_prefix + 'reversal_' + civ_d_win] = (
                np.where(~df['type_criminal']
                          & ((df[self.sec_prefix + 'reversal']
                              & df['party_appnt1']
                              & df['party_civd1'])
                             | (df[self.sec_prefix + 'reversal']
                                & df['party_appnt2']
                                & df['party_civd2'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee1']
                                & df['party_civd1'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee2']
                                & df['party_civd2'])), True, False))
        df[self.sec_prefix + 'reversal_' + crim_p_win] = (
                np.where(df['type_criminal']
                          & ((df[self.sec_prefix + 'reversal']
                              & df['party_appnt1']
                              & df['party_pros1'])
                             | (df[self.sec_prefix + 'reversal']
                                & df['party_appnt2']
                                & df['party_pros2'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee1']
                                & df['party_pros1'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee2']
                                & df['party_pros2'])), True, False))
        df[self.sec_prefix + 'reversal_' + civ_p_win] = (
                np.where(~df['type_criminal']
                          & ((df[self.sec_prefix + 'reversal']
                              & df['party_appnt1']
                              & df['party_civp1'])
                             | (df[self.sec_prefix + 'reversal']
                                & df['party_appnt2']
                                & df['party_civp2'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee1']
                                & df['party_civp1'])
                             | (~df[self.sec_prefix + 'reversal']
                                & df['party_appee2']
                                & df['party_civp2'])), True, False))
        return df

    def unpack_references(self, df):
        pass
        df = df
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
