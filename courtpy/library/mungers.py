from dataclasses import dataclass
import os
import re

from utilities.strings import word_count
from utilities.rematch import ReMatch

@dataclass
class Munger(object):   
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
                os.path.split(bundle.a_path)[1])
        df[self.sec_prefix + 'word_count'] = (
                word_count(bundle.opinions) - 1)
        if self.source == 'court_listener':
            df[self.sec_prefix + 'cl_url'] = bundle.cl_url
            df[self.sec_prefix + 'cl_id'] = bundle.cl_id
        return df
    
    def munge_dates(self, df, bundle):        
        """
        Parses dates that a case was decided, submitted, argued, amended, and
        filed. This parsing is only done certain sources of data.
        Most case do not include all dates and dates are formatted 
        inconsistently. As a result, the general matcher and munger methods
        cannot be used.
        """
        self.date_list = df['sec_' + self.section]
        self.date_list = [
                self.date_list.upper() for self.date_list in self.date_list]
        self.date_pat = self.dividers_table.lookup['date_pat']
        if self.date_list:
            for each_date in self.date_list:
                if ('SUBMITTED' in each_date 
                        and re.search(self.date_pat, each_date)):
                    df['date_submitted'] = (
                            re.search(self.date_pat, each_date).group(0))
                if ('ARGUED' in each_date 
                        and re.search(self.date_pat, each_date)):
                    df['date_argued'] = (
                            re.search(self.date_pat, each_date).group(0))
                if ('AMENDED' in each_date 
                        and re.search(self.date_pat, each_date)):
                    df['date_amended'] = (
                            re.search(self.date_pat, each_date).group(0))
                if ('FILED' in each_date 
                        and re.search(self.date_pat, each_date)):
                    df['date_filed'] = (
                            re.search(self.date_pat, each_date).group(0))
                if ('DECIDED' in each_date 
                        and re.search(self.date_pat, each_date)):
                    df['date_decided'] = (
                            re.search(self.date_pat, each_date).group(0))
        date_string = ', '.join(self.date_list)
        if df['date_decided']:
            df['year'] = (
                    df['date_decided'][-4:])
        elif df['date_filed']:
            df['year'] = (
                    df['date_filed'][-4:])
        elif df['date_amended']:
            df['year'] = (
                    df['date_amended'][-4:])
        elif df['date_submitted']:
            df['year'] = (
                    df['date_submitted'][-4:])
        elif df['date_argued']:
            df['year'] = (
                    df['date_argued'][-4:])
        elif re.search(self.date_pat, date_string):          
            df['date_decided'] = (
                    re.search(self.date_pat, date_string).group(0))
            df['year'] = (
                df['date_decided'][-4:])
        return df 
    
    def munge_parties(self, df):
        if self.settings['general']['verbose']:
            print('Identifying types and roles of litigation parties')
        party_pat = self.dividers_table.lookup['party1_div']
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
        excess = self.excess_table.lookup['judge_excess']
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
        excess = self.excess_table.lookup['author_excess']
        df = judges.clean_panel(df, self.source_col, excess)
        df = df.apply(judges.judge_matcher,
                       in_col = self.source_col, 
                       out_col = fixed_column,
                       court_num_col = 'court_num',
                       year_col = 'year', 
                       axis = 'columns')
        df = judges.explode_panel(df, fixed_column, name_prefix)
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
        excess = self.excess_table.lookup['concur_excess']
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