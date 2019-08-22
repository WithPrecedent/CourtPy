
from dataclasses import dataclass
import os
import re

import pandas as pd

from simplify import timer
from simplify.almanac.steps import Harvest
from simplify.implements import listify


@timer('Initial case data collection (Harvesting)')
@dataclass
class CPHarvest(Harvest):
    """Divides and parsers court opinions into pandas series and saves results
    as csv file.

    Attributes:


    All data extraction from the opinion text (and not headers or metadata) is
    performed by CPHarvest. Other data is munged by the CPThresh in a vectorized
    manner. Because of the memory necessary to load all court opinions in large
    datasets, CPHarvest has to apply non-vectorized, case-by-case methods.
    """
    technique : str = ''
    techniques : object = None
    parameters : object = None
    auto_prepare : bool = True
    name : str = 'harvester'

    def __post_init__(self):
        """The main input/output loop which takes data from source files and
        outputs to a .csv file one case at a time. During parsing, the data is
        stored in a pandas series. The loop iterates through a globbed list of
        import inventory.
        """
        self._set_defaults()
        for self.source in listify(self.sources):
            self.organizer_file = self.source + '.csv'
            super().__post_init__()
            self.start()
        return self

    def _prepare_concur_dissent(self):
        concur_dissent_df = pd.read_csv(self.separate_opinions_file,
                                        usecols = ['keys', 'values'])
        for i, row in concur_dissent_df:
            if row['value'] == 'separate':
                self.concur_dissent_separator = re.compile(row['key'])
            else:
                setattr(self, row['value'] + '_separator',
                        re.compile(row['key'],
                                   flags = [re.IGNORECASE|re.DOTALL]))
        return self

    def _separate_concur_dissent(self):
        """Divides concurring, dissenting, and mixed opinions."""
        separate_list = []
        concur_list = []
        dissent_list = []
        mixed_list = []
        if re.search(self.concur_dissent_separator, self.opinions_breaks):
            separate_list = re.findall(self.concur_dissent_separator,
                                       self.opinions_breaks)
            for opinion in separate_list:
                if re.search(self.concur_separator, opinion):
                    if re.search(self.mixed_separator, opinion):
                        mixed_list.append(opinion)
                    else:
                        concur_list.append(opinion)
                elif re.search(self.dissent_separator, opinion):
                    if re.search(self.mixed_separator, opinion):
                        mixed_list.append(opinion)
                    else:
                        dissent_list.append(opinion)
            self.cases.df['separate_concur'] = concur_list + mixed_list
            self.cases.df['separate_dissent'] = dissent_list + mixed_list
        return self

    def _separate_header(self, case_text):
        """Divides court opinion into header and opinions."""
        if re.search(self.opinion_divider, case_text):
            op_list = re.split(self.opinion_divider, case_text)
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

    def _set_defaults(self):
        self.opinion_divider = '\nOPINION(?=\n\n)'
        self.separate_opinions_file = os.path.join(self.inventory.organizers,
                                                   'separate_opinions.csv')
        self._prepare_concur_dissent()
        return self

    def start(self, cases = None):
        if not cases:
            cases = self.cases
        self.inventory.initialize_series_writer(
                file_name = 'harvested_cases.csv',
                column_list = cases.column_list)
        cases.create_series()
        for case_num, a_path in enumerate(self.inventory.globbed_paths):
            with open(a_path, mode = 'r', errors = 'ignore',
                      encoding = self.encoding) as a_file:
                case_text = a_file.read()
                self.separate_header(case_text)
                self.separate_concur_dissent()
                cases.add_index(index_number = case_num + 1)
                cases.df, self.header = self.techniques['organizer'].match(
                        df = cases.df, source = self.header)
                cases.df = self.techniques['keyword_search'].match(
                        df = cases.df, source = self.opinion)
                self.inventory.save(variable = self.cases.df)
                if (case_num + 1) % 100 == 0 and self.verbose:
                    print(case_num + 1, 'cases parsed')
        return self