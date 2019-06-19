
from dataclasses import dataclass
import os
import re

from simplify.tools.divider import Divider
from simplify.tools.matcher import ReMatch

from .tool import Tool


@dataclass
class OpinionDivider(Divider, Tool):

    origins : object = None
    techniques : object = None
    regexes : object = None
    prefix : str = 'section_'
    file_path : str = ''
    column_dict : object = None

    def __post_init__(self):
        self.default_origins = {'party' : 'header',
                                'court' : 'header',
                                'docket' : 'header',
                                'cite' : 'header',
                                'notice' : 'header',
                                'date' : 'header',
                                'history' : 'header',
                                'future' : 'header',
                                'counsel' : 'header',
                                'disposition' : 'header',
                                'author' : 'header',
                                'panel_judges' : 'header'}
        self.default_techniques = {'party' : 'extract',
                                   'court' : 'extract',
                                   'docket' : 'extract',
                                   'cite' : 'extract',
                                   'notice' : 'extract',
                                   'date' : 'extract_all',
                                   'history' : 'extract',
                                   'future' : 'extract',
                                   'counsel' : 'extract',
                                   'disposition' : 'extract',
                                   'author' : 'extract',
                                   'panel_judges' : 'extract'}
        self.default_file_path = os.path.join('rules', 'data_sources',
                                              self.case_type + '_dividers.csv')
        return

    def _separate_header(self, regex, case_text):
        """Divides court opinion into header and opinions."""
        if re.search(regex, case_text):
            op_list = re.split(regex, case_text)
            if len(op_list) > 0:
                self.header = op_list[0]
                if len(op_list) > 1:
                    self.opinions = self._no_breaks(''.join(op_list[1:]))
                    self.opinions_breaks = ''.join(op_list[1:])
        else:
            self.header = case_text
            self.opinions = ''
            self.opinions_breaks = ''
        return self

    def _separate_opinions(self, regex, df):
        """Divides concurring, dissenting, and mixed opinions."""
        separate_list = []
        concur_list = []
        dissent_list = []
        mixed_list = []
        if re.search(regex, self.opinions_breaks):
            separate_list = re.findall(regex, self.opinions_breaks)
            for opinion in separate_list:
                if re.search(self.regexes['concur_div'], opinion):
                    if re.search(self.regexes['mixed_div'], opinion):
                        mixed_list.append(opinion)
                    else:
                        concur_list.append(opinion)
                elif re.search(self.regexes['dissent_div'], opinion):
                    if re.search(self.regexes['mixed_div'], opinion):
                        mixed_list.append(opinion)
                    else:
                        dissent_list.append(opinion)
            df['separate_concur'] = concur_list + mixed_list
            df['separate_dissent'] = dissent_list + mixed_list
        return df

    def iterate(self, df, case_text):
        for section, regex in self.regexes.items():
            if section == 'opinion':
                self._separate_header(regex, case_text)
            elif section == 'separate':
                df = self._separate_opinions(regex, df)
            elif section in self.column_dict:
                column = self.prefix + section
                technique = self.options[self.techniques[section]]
                df[column] = self._no_breaks(technique(section, regex))
        return df