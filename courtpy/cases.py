"""
Cases stores the key attributes of the court opinion data for use throughout
the CourtPy package. Edits to the dictionaries and lists here will change
the functionality of methods and functions throughout.
"""
from dataclasses import dataclass
from functools import wraps
from inspect import getfullargspec
import os

from simplify import Codex
from simplify.tools.matcher import ReMatch

from tools.combiner import Combiner
from tools.divider import Divider
from tools.munger import Munger
from tools.external import External


@dataclass
class Cases(Codex):

    settings : object
    filer : object = None
    df : object = None
    quick_start : bool = False
    default_df = str = 'df'
    column_dict : object = None
    source : str = ''
    stage : str = ''
    prefixes : object = None
    drops : object = None
    _scaler_columns : object = None
    _categorical_columns : object = None
    _interactor_columns : object = None

    def __post_init__(self):
        super().__post_init__()
        self.rules_types = ['section', 'source', 'stage', 'jurisdiction',
                            'case_type']
        self._initialize()
        return

    @property
    def encoder_columns(self):
        if not self._encoder_columns:
            self._create_encoder_columns()
        return self._encoder_columns

    @property
    def interactor_columns(self):
        if not self._interactor_columns:
            self._create_interactor_columns()
        return self._interactor_columns

    @property
    def scaler_columns(self):
        if not self._scaler_columns:
            self._create_scaler_columns()
        return self._scaler_columns

    def _add_prefix_dict(self, i_dict, prefix):
        return {f'{prefix}_{k}' : v for k, v in i_dict.items()}

    def _add_underscore(self, iterable):
        if isinstance(iterable, list):
            return [string + '_' for string in iterable]
        elif isinstance(iterable, dict):
            return {key + '_' : value for key, value in iterable.items()}

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
            self.mungers.append(Munger(settings = self.settings,
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
                                       paths = self.paths,
                                       settings = self.settings))
        for external in self.externals:
            if external.section == 'judge_exp':
                    df = external.section_adder(df = df,
                                                judges = self.judges)
            else:
                df = external.section_adder(df)
        return self

    def _drop_missing(self, iterable, missing_prefixes = None):
        if not missing_prefixes and self.source:
            missing_prefixes = self.missing_prefixes[self.source]
        if missing_prefixes:
            for prefix in missing_prefixes:
                if prefix in self.prefixes:
                    iterable.pop(prefix)
        return iterable

    def _initialize(self):
        for rules in self.rules_types:
            if rules == 'stage':
                stage_method_name = '_initialize_' + self.stage + '_options'
                getattr(self, stage_method_name)()
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
            self.combiners.append(Combiner(settings = self.settings,
                                           dicts_path = self.paths.dicts,
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
#        if self.stage == 'parser':

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
            self.judges = FederalJudges(self.paths, self.settings, self.stage)
            self.judges.make_name_dicts()
        excess_table_file = (
                cases.rules[cases.rules['key'] == 'panel_judges']
                    ['munge_file'].item())
        excess_table_path = os.path.join(self.paths.dicts, excess_table_file)
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
        Divider.settings = self.settings
        Divider.paths = self.paths
        self._initialize_munger_options()
        return self



    def _initialize_stage_options(self):
        self._dicts_to_check = {'parser' : [self.prefixes, self.dividers,
                                            self.mungers],
                                'wrangler' : [self.prefixes, self.mungers,
                                              self.externals, self.combiners],
                                'engineer' : [self.prefixes],
                                'analyzer' : [self.prefixes]}
        dict_list = self._dicts_to_check[self.stage]
        for subdict in dict_list:
            subdict = self._drop_missing(iterable = subdict)
        return self

    def _initialize_wrangler_options(self):
        self._initialize_munger_options()
        External.settings = self.settings
        External.paths = self.paths
        self.combiners = []
        self.externals = []
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

    @_check_df
    def add_index(self, df = None, index_number = None):
        if index_number:
            df[self.index_column] = index_number
        else:
            self.add_unique_index(column = self.index_column)
        return self

    def create(self):
        self._set_prefixes()
        for rules in self.rules_types:
            if rules == 'stage':
                stage_method_name = '_create_' + self.stage + '_rules'
                getattr(self, stage_method_name)()
            method_name = '_create_' + rules + '_rules'
            getattr(self, method_name)()
        return self

    def get_type_prefixes(self, data_type = None):
        prefix_list = []
        for prefix, d_type in self.prefixes.items():
            if d_type == data_type:
                prefix_list.append(prefix)
        return prefix_list


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

    def set_column_type(self, column_type, columns = None, prefixes = None):
        col_type = '_' + column_type + '_columns'
        col_list = self.create_column_list(columns = columns,
                                           prefixes = prefixes)
        setattr(self, col_type, col_list)
        return self