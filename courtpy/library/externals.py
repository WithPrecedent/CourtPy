from dataclasses import dataclass
import numpy as np

@dataclass
class External(object):
    """
    Class related to adding externally sourced data into the CourtPy dataset.
    """
    section : str
    paths : object
    settings : object
    
    def __post_init__(self):
        if self.section == 'pol':
            self.section_adder = self.add_politics
        elif self.section == 'judge_exp':
            self.section_adder = self.add_bios
        return

    def add_politics(self, df):
        if self.settings['general']['verbose']:
            print('Adding political context variables')
        self.sec_prefix = self.section + '_'
        if self.settings['cases']['jurisdiction'] == 'federal':
            from library.executive import President
            from library.judiciary import SCOTUS
            from library.legislature import Congress   
            self.executive = President(self.paths, self.settings)
            self.executive.import_file()
            self.exec_out = 'pres'
            self.legislature = Congress(self.paths, self.settings)
            self.legislature.import_file()
            self.house_out = 'house'
            self.sen_out = 'sen'
            self.judiciary = SCOTUS(self.paths, self.settings)
            self.judiciary.import_file()
            self.jud_out = 'scotus'
        df[self.sec_prefix + self.jud_out + '_med'] = (
                df['year'].astype(str).map(self.judiciary.med_ideo))
        df[self.sec_prefix + self.jud_out + '_min'] = (
                df['year'].astype(str).map(self.judiciary.min_ideo))
        df[self.sec_prefix + self.jud_out + '_max'] = (
                df['year'].astype(str).map(self.judiciary.max_ideo))
        df[self.sec_prefix + self.sen_out + '_med'] = (
                df['year'].astype(str).map(self.legislature.sen_ideo))
        df[self.sec_prefix + self.house_out + '_med'] = (
                df['year'].astype(str).map(self.legislature.house_ideo))
        df[self.sec_prefix + self.exec_out] = (
                df['year'].astype(str).map(self.executive.ideo))
        return df

    def add_bios(self, df, judges):
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