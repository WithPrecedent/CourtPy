from dataclasses import dataclass
import numpy as np
import os
import re

from utilities.rematch import ReMatch

@dataclass
class Combiner(object):
    """
    Class for combining data into new variables.
    """
    settings : object
    dicts_path: str
    section : str
    data_type : str
    munge_file : str

    def __post_init__(self):
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