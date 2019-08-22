from dataclasses import dataclass
import os
import re

from library.stringer import Stringer
from utilities.rematch import ReMatch

@dataclass
class Munger(Stringer):
    """
    Container for holding data mungers used by CourtParser and CourtWrangler.
    """
    settings : object
    dicts_path : str
    source : str
    section : str
    data_type : str
    munge_type : str
    munge_file : str
    source_col : str

    def __post_init__(self):
        self.munge_path = os.path.join(self.dicts_path, self.munge_file)
        if self.section in ['admin', 'precedent', 'statute', 'other_cites']:
            self.sec_prefix = 'refer_'
            self.out_col = self.section
        elif self.section == 'court':
            self.sec_prefix = 'court_'
            self.out_col = 'num'
        else:
            self.sec_prefix = self.section + '_'
            self.out_col = ''
        if self.munge_type == 'general' or self.section == 'court':
            if self.source_col == 'opinions':
                self.section_munger = ReMatch(file_path = self.munge_path,
                                              out_type = self.data_type,
                                              out_prefix = self.sec_prefix,
                                              out_col = self.out_col)
            else:
                self.section_munger = ReMatch(file_path = self.munge_path,
                                              out_type = self.data_type,
                                              in_col = self.source_col,
                                              out_prefix = self.sec_prefix,
                                              out_col = self.out_col)
        elif self.munge_type == 'specific':
            if self.section in ['panel_judges', 'author', 'separate']:
                self.excess_table = ReMatch(file_path = self.munge_path,
                                            reverse_dict = True)
            if self.section == 'meta':
                self.section_munger = self.munge_meta
            elif self.section == 'date':
                self.section_munger = self.munge_dates
            elif self.section == 'year':
                self.section_munger = self.munge_year
            elif self.section == 'party':
                self.section_munger = self.munge_parties
            elif self.section == 'panel_judges':
                self.section_munger = self.munge_panel_judges
            elif self.section == 'author':
                self.section_munger = self.munge_author
            elif self.section == 'separate':
                self.section_munger = self.munge_separate_opinions
        return

    def munge_meta(self, df, bundle):
        df[self.sec_prefix + 'file_name'] = (
                os.path.split(bundle['a_path'])[1])
        df[self.sec_prefix + 'word_count'] = (
                word_count(bundle['opinions']) - 1)
        if self.source == 'court_listener':
            df[self.sec_prefix + 'cl_url'] = bundle['cl_url']
            df[self.sec_prefix + 'cl_id'] = bundle['cl_id']
        return df

    def munge_dates(self, df, bundle):
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
        date_list = df['sec_' + self.section]
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

    def munge_parties(self, df):
        if self.settings['general']['verbose']:
            print('Identifying types and roles of litigation parties')
        party_pat = self.dividers_table['party1_div']
        excess = self.dividers_table['party_excess']
        df[[self.sec_prefix + 'name1',
                       self.sec_prefix + 'name2']] = (
                df['sec_' + self.section].str.split(
                        party_pat, expand = True, n = 1))
        df[self.sec_prefix + 'name2'].fillna('', inplace = True)
        for i in range(1, 3):
            party_munger = ReMatch(file_path = self.munge_path,
                                   out_type = self.data_type,
                                   in_col = 'party_name' + str(i),
                                   out_prefix = self.sec_prefix,
                                   out_suffix = str(i))
            df = party_munger.match(df = df)
        return df

    def munge_panel_judges(self, df, judges):
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

    def munge_author(self, df, judges):
        if self.settings['general']['verbose']:
            print('Determining authors of opinions')
        fixed_column = 'temp_author'
        name_prefix = self.sec_prefix + 'name'
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

    def munge_panel(self, df, judges):
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
        return df

    def munge_separate_opinions(self, df, judges):
        if self.settings['general']['verbose']:
            print('Determining authors of concurring and dissenting opinions')
        concur_source_col = 'separate_concur'
        dissent_source_col = 'separate_dissent'
        concur_fixed_col = 'temp_concur'
        dissent_fixed_col = 'temp_dissent'
        concur_name_prefix = self.sec_prefix + 'concur'
        dissent_name_prefix = self.sec_prefix + 'dissent'
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