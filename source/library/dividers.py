
from dataclasses import dataclass
import re

from utilities.strings import no_breaks

@dataclass
class Divider(object):
    
    section : str
    source : str
    data_type : str
    source_section : str
    regex : object
    
    def __post_init__(self):
        self.sec_col =  'sec_' + self.section
        if self.section == 'separate':
            self.section_divider = self.divide_separate_opinions
        else:
            self.section_divider = self.general_divider
        return
    
    def general_divider(self, df, bundle, dividers_table):
        if self.source_section == 'header':
            if self.section == 'date':
                if re.search(self.regex, bundle.header):
                    df[self.sec_col] = (
                            re.findall(self.regex, bundle.header))
            elif re.search(self.regex, bundle.header):
                df[self.sec_col] = (
                        re.search(self.regex, bundle.header).group(0))
                df[self.sec_col] = (
                        df[self.sec_col].strip())
                bundle.header = bundle.header.replace(
                        df[self.sec_col], '')
                df[self.sec_col] = no_breaks(
                        df[self.sec_col])             
        elif self.source_section == 'opinions':
            if re.search(self.regex, bundle.opinions):
                df[self.sec_col] = re.search(self.regex, 
                        bundle.opinions).group(0)
        return df, bundle
        
    def divide_separate_opinions(self, df, bundle, dividers_table):
        separate_list = []
        concur_list = []
        dissent_list = []
        mixed_list = []
        if re.search(dividers_table.lookup['separate_div'], 
                     bundle.opinions_breaks):
            separate_list = re.findall(
                    dividers_table.lookup['separate_div'], 
                    bundle.opinions_breaks)
            for i in separate_list:
                if re.search(dividers_table.lookup['concur_div'], i):
                    if re.search(dividers_table.lookup['mixed_div'], i):
                        mixed_list.append(i)     
                    else:
                        concur_list.append(i)
                elif re.search(dividers_table.lookup['dissent_div'], i):
                    if re.search(dividers_table.lookup['mixed_div'], i):
                        mixed_list.append(i)
                    else:
                        dissent_list.append(i)
            df['separate_concur'] = concur_list + mixed_list
            df['separate_dissent'] = dissent_list + mixed_list
        return df, bundle
    
@dataclass
class CaseBundle(object):
    header : str = ''
    opinions : str = ''
    opinions_breaks : str = ''
    a_path : str = ''
    cl_id : int = 0
    cl_url : str = ''