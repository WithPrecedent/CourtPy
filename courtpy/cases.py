
from dataclasses import dataclass
from functools import wraps
from inspect import getfullargspec
import os

from simplify import Ingredients

#from .combiners import (Biographies, Executive, Judges, Judiciary,
#                        Legislature)

@dataclass
class Cases(Ingredients):
    """Cases stores the key attributes of the court opinion data and the data
    itself for use throughout the CourtPy package.

    Cases is a child class to Ingredients from the siMpLify package and makes
    use of its methods.

    Attributes:
        df: a pandas dataframe or series.
        menu: an instance of menu.
        inventory: an instance of inventory.
        quick_start: a boolean variable indicating whether data should
            automatically be loaded into the df attribute.
        default_df: the current default dataframe or series attribute that will
            be used when a specific dataframe is not passed to a class method.
            The value is a string corresponding to the attribute dataframe
            name and is initially set to 'df'.
        x, y, x_train, y_train, x_test, y_test, x_val, y_val: dataframes or
            series. These dataframes (and corresponding columns dictionaries)
            need not be passed when the class is instanced. They are merely
            listed for users who already have divided datasets and still wish
            to use the siMpLify package.
        columns_dict: dictionary containing column names and datatypes for df
            or x (if data has been split) dataframes or series
        source_format: what database the court opinion data is from.
    """
    df : object = None
    menu : object = None
    inventory : object = None
    quick_start : bool = False
    default_df : str = 'df'
    x : object = None
    y : object = None
    x_train : object = None
    y_train : object = None
    x_test : object = None
    y_test : object = None
    x_val : object = None
    y_val : object = None
    columns_dict : object = None
    source_format : str = ''

    def __post_init__(self):
        super().__post_init__()
        return

    def _create_analyzer_rules(self):
        self.special_column_types = ['encoders', 'interactors', 'scalers']
        for column_type in self.special_column_types:
            setattr(self, column_type, self._create_column_type_dict(
                    type_group = getattr(self, column_type),
                    col_type_dict = getattr(self, column_type + '_dict')))
        return self

    def _create_combiner_rules(self):

        return self

    def _create_engineer_rules(self):
        if not self.drops:
            self.drops = self.default_drops
        return self

    def _create_munger_rules(self):
        for section, source in self.mungers_dict.items():
            file_name = section + '_munger.csv'
            if section in self.specific_mungers:
                munger_type = 'specific'
            else:
                munger_type = 'general'
            self.mungers.append(Munger(menu = self.menu,
                                       dicts_path = self.filer.dicts,
                                       source = self.source,
                                       section = section,
                                       data_type = self.prefixes[section],
                                       munge_type = munger_type,
                                       munge_file = file_name,
                                       source_col = source))
        return self

    def _create_parser_column_dict(self):
        """
        Creates a complete dictionary for a pandas series index or
        dataframe columns with data type defaults.
        """
        self.column_dict = {self.index_column : int}
        meta_dict = self._add_prefix_dict(self.meta_columns, 'meta')
        self.column_dict.update(meta_dict)
        self.column_dict.update({'court_num' : int, 'date_decided' : str,
                                 'date_filed' : str, 'date_argued' : str,
                                 'date_amended' : str, 'date_submitted' : str,
                                 'year' : int})
        for divider in self.dividers:
            self.column_dict.update({'sec_' + divider.section : str})
        self.column_dict.update({'separate_concur' : list,
                                 'separate_dissent' : list})
        for munger in self.mungers:
            if (munger.data_type in ['bool', 'matches']
                    and munger.munge_type == 'general'):
                munger.col_list = list(
                        munger.section_munger.expressions['values'])
                munger.col_dict = {k : bool for k in munger.col_list}
                self.column_dict.update(munger.col_dict)
        self.column_dict.update({'refer_admin' : list,
                                 'refer_precedent' : list,
                                 'refer_statute' : list})
        return self

    def _create_parser_rules(self):
        self._create_parser_column_dict()
        return self

    def _create_wrangler_rules(self):
        self._create_munger_rules()
        self.externals.append(External(section = row['key'],
                                       inventory = self.inventory,
                                       menu = self.menu))
        for external in self.externals:
            if external.section == 'judge_exp':
                    df = external.section_adder(df = df,
                                                judges = self.judges)
            else:
                df = external.section_adder(df)
        return self


    def _initialize(self):
        for rules in self.rules_types:
            if rules == 'step':
                step_method_name = '_initialize_' + self.step + '_options'
                getattr(self, step_method_name)()
            method_name = '_initialize_' + rules + '_options'
            getattr(self, method_name)()
            return self

    def _initialize_analyzer_options(self):
        self.scalers = 'numerical'
        self.categoricals = 'category_encoder'
        self.interactors = 'interactor'
        self.scalers_dict = {'numerical' : ['int', 'float', 'scaler'],
                             'float' : 'float',
                             'int' : 'int',
                             'scaler' : 'scaler'}
        self.categoricals_dict = {'category' : 'category',
                                  'category_encoder' : ['category', 'encoder'],
                                  'encoder' : 'encoder'}
        self.interactors_dict = {'category' : 'category',
                                 'interactor' : 'interactor',
                                 'interactor_category' : ['category',
                                                          'encoder']}
        return self

    def _initialize_combiner_options(self):
        self.combiners = []
        combiner_df = cases.rules.loc[cases.rules['combiner']]
        for i, row in combiner_df.iterrows():
            self.combiners.append(Combiner(menu = self.menu,
                                           dicts_path = self.inventory.dicts,
                                           section = row['key'],
                                           data_type = row['data_type'],
                                           munge_file = row['munge_file']))
        for combiner in self.combiners:
            df = combiner.section_combiner(df)
        return self

    def _initialize_extractor_options(self):
        self.specific_mungers = ['meta', 'party', 'date', 'author',
                                 'panel_judges', 'separate']
        if self.source != 'fjc_idb':
            self.dividers_file = os.path.join(self.filer.dicts,
                                              self.source + '_dividers.csv')
        else:
            self.dividers_file = None
#        if self.step == 'parser':

        return self

    def _initialize_engineer_options(self):
        self.default_drops = ['index', 'meta', 'party', 'docket', 'cite',
                              'notice', 'date', 'history', 'future',
                              'disposition', 'author', 'separate',
                              'admin_cites', 'case_cites', 'statute_cites',
                              'other_cites', 'panel_attr', 'reference', 'temp']
        return self

    def _initialize_judge_options(self):
        if self.jurisdiction == 'federal':
            from library.judges import FederalJudges
            self.judges = FederalJudges(self.inventory, self.menu, self.step)
            self.judges.make_name_dicts()
        excess_table_file = (
                cases.rules[cases.rules['key'] == 'panel_judges']
                    ['munge_file'].item())
        excess_table_path = os.path.join(self.inventory.dicts, excess_table_file)
        self.excess_table = ReMatch(file_path = excess_table_path,
                                    reverse_dict = True)
        return self

    def _initialize_munger_options(self):
        self._conform_types()
        self.mungers = []
        if hasattr(self, 'dividers_table'):
            Munger.dividers_table = self.dividers_table
        else:
            Munger.dividers_table = ReMatch(file_path = self.dividers_file,
                                            reverse_dict = True).expressions

        return self

    def _initialize_parser_options(self):
        Divider.menu = self.menu
        Divider.inventory = self.inventory
        self._initialize_munger_options()
        return self

    def _initialize_step_options(self):
        self._dicts_to_check = {'parser' : [self.prefixes, self.dividers,
                                            self.mungers],
                                'wrangler' : [self.prefixes, self.mungers,
                                              self.externals, self.combiners],
                                'engineer' : [self.prefixes],
                                'analyzer' : [self.prefixes]}
        dict_list = self._dicts_to_check[self.step]
        for subdict in dict_list:
            subdict = self._drop_missing(iterable = subdict)
        return self

    def _initialize_wrangler_options(self):
        self._initialize_munger_options()
        External.menu = self.menu
        External.inventory = self.inventory
        self.combiners = []
        self.externals = []
        return self


    def _judge_combiner(self, df, match_column, out_column, map_dict):
        self.match_columns = {'judge_exp' : 'judge_name',
                              'judge_attr' : 'judge_name',
                              'judge_demo'  : 'judge_name',
                              'judge_ideo' : 'judge_name',
                              'panel_num' : 'judge_name',
                              'panel_exp' : 'judge_name',
                              'panel_attr' : 'judge_name',
                              'panel_demo' : 'judge_name',
                              'panel_ideo' : 'judge_name',
                              'panel_size' : 'sec_panel_judges',
                              'politics' : 'year'}
        df[out_column] = df[match_column].astype(str).map(map_dict)
        return df

    def _check_df(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            argspec = getfullargspec(func)
            unpassed_args = argspec.args[len(args):]
            if 'df' in argspec.args and 'df' in unpassed_args:
                kwargs.update({'df' : getattr(self, self.default_df)})
            return func(self, *args, **kwargs)
        return wrapper

    def _create_column_type_dict(self, type_group, col_type_dict):
        new_list = []
        for prefix, data_type in self.prefixes:
            if data_type in self._listify(col_type_dict[type_group]):
                    new_list.append(prefix)
        type_prefixes = self._add_underscore(new_list)
        type_columns = self.create_column_list(prefixes = type_prefixes)
        return type_columns

    def _drop_missing(self, iterable, missing_prefixes = None):
        if not missing_prefixes and self.source:
            missing_prefixes = self.missing_prefixes[self.source]
        if missing_prefixes:
            for prefix in missing_prefixes:
                if prefix in self.prefixes:
                    iterable.pop(prefix)
        return iterable

    def _set_defaults(self):
        super()._set_defaults()
        self.default_organize = {'file_path' : os.path.join(
                self.inventory.organizers, self.data_source + '.csv')}
        self.default_sections = {'index' : int,
                                 'meta' : str,
                                 'party' : str,
                                 'court' : 'category',
                                 'docket' : list,
                                 'cite' : 'pattern',
                                 'notice' : bool,
                                 'date' : str,
                                 'year' : int,
                                 'history' : bool,
                                 'future' : bool,
                                 'counsel' : bool,
                                 'disposition' : bool,
                                 'author' : 'category',
                                 'panel_judges' : list,
                                 'separate' : list,
                                 'criminal' : bool,
                                 'civil' : bool,
                                 'general' : bool,
                                 'procedure' : bool,
                                 'standard' : bool,
                                 'disposition_op' : bool,
                                 'admin_cites' : 'pattern',
                                 'case_cites' : 'pattern',
                                 'statute_cites' : 'pattern',
                                 'other_cites' : 'pattern',
                                 'judge_name' : 'category',
                                 'judge_exp' : float,
                                 'judge_attr' : str,
                                 'judge_demo' : float,
                                 'judge_ideo' : float,
                                 'panel_size' : int,
                                 'judge_vote' : bool,
                                 'agency' : 'category',
                                 'type' : bool,
                                 'politics' : float,
                                 'outcome' : bool,
                                 'reference' : list,
                                 'temp' : str}
#        self.default_keyword = {'index' : None,
#                                'meta' : None,
#                                'criminal' : 'opinions',
#                                'civil' : 'opinions',
#                                'general' : 'opinions',
#                                'procedure' : 'opinions',
#                                'standard' : 'opinions',
#                                'disposition_op' : 'opinions',
#                                'admin_cites' : 'opinions',
#                                'case_cites' : 'opinions',
#                                'statute_cites' : 'opinions',
#                                'other_cites' : 'opinions'}
        self.default_parse = {'party' : 'section_party',
                              'court' : 'section_court',
                              'docket' : 'section_docket',
                              'cite' : 'section_cite',
                              'notice' : 'section_notice',
                              'date' : None,
                              'history' : 'section_history',
                              'future' : 'section_future',
                              'counsel' : 'section_counsel',
                              'disposition' : 'section_disposition',
                              'author' : 'section_author',
                              'panel_judges' : 'section_panel_judges',
                              'separate' : None}
#        self.default_combiner = {'biographies' : Biographies,
#                                 'executive' : Executive,
#                                 'judges' : Judges,
#                                 'judiciary' : Judiciary,
#                                 'legislature' : Legislature}
        self.default_merger = {'docket' : []}
        self.default_shaper = {self.shape : 'judge'}
        self.default_streamliner = {}
        self.rules_types = ['section', 'source', 'step', 'jurisdiction',
                            'case_type']
        return self

    def _set_prefixes(self):
        if not self.prefixes:
            self.prefixes = self.default_prefixes
        return self

    def add_combiner(self, combiner_name, source_sections):
        return self.combiners.sources.update({combiner_name,
                                              self._listify(source_sections)})

    def add_divider(self, divider_name, source_section):
        return self.dividers.sources.update({divider_name, source_section})

    def add_external(self, external_name, source_section):
        return self.externals.sources.update({external_name, source_section})

    def add_munger(self, munger_name, source_section):
        return self.mungers.sources.update({munger_name, source_section})

    def munge(self, df = None, bundle = None):
        if self.step in ['wrangler']:
            df['court_num'].fillna(method = 'ffill', inplace = True,
                                   downcast = int)
            df['year'] = df['year'].replace(0, method = 'ffill').astype(int)
        for munger in self.cases.mungers:
            if self.step in ['parser']:
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
            elif self.step in ['wrangler']:
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