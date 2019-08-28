
from dataclasses import dataclass
import os

from simplify.almanac import Almanac
from simplify.tools import listify


@dataclass
class CPAlmanac(Almanac):
    """Defines rules for cases data dependent upon case_type, jurisdiction,
    CourtPy stage, and data source.

    Attributes:
        menu: an instance of Menu.
        inventory: an instance of Inventory.
        steps: a list of Harvest steps to complete.
        auto_prepare: a boolean value as to whether the prepare method should
            be called when the class is instanced.
        data_source: the data format of the court opinion files.
        jurisdiction: the jurisdiction of the court_opinions.
        case_type: the type of court that is reviewing the cases, 'high_court',
            'appellate_court', 'trial_court', or 'other_court'
    """
    menu : object
    inventory : object = None
    steps : object = None
    ingredients : object = None
    plan : object = None
    auto_prepare : bool = True
    name : str = 'almanac'
    data_source : str = ''
    instructions_folder : object = None
#    jurisdiction : str = ''
#    case_type : str = ''

    def __post_init__(self):
        self.menu.inject(instance = self, sections = ['cases'])
        super().__post_init__()
        return self

    def _set_defaults(self):
        super()._set_defaults()
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
        self.default_organize = [self.data_source]
        self.default_parse = ['opinions_' + self.jurisdiction,
                              'opinions_breaks_' + self.jurisdiction]
        self.default_keyword = [self.jurisdiction]
        self.default_combine = ['biographies',
                                'executive',
                                'judges',
                                'judiciary',
                                'legislature']
        self.default_merge = {'docket' : self.data_sources}
        self.default_shape = {'judge' : self.shape}
        self.default_streamline = {}
#        self.default_parse = {'party' : 'section_party',
#                              'court' : 'section_court',
#                              'docket' : 'section_docket',
#                              'cite' : 'section_cite',
#                              'notice' : 'section_notice',
#                              'date' : None,
#                              'history' : 'section_history',
#                              'future' : 'section_future',
#                              'counsel' : 'section_counsel',
#                              'disposition' : 'section_disposition',
#                              'author' : 'section_author',
#                              'panel_judges' : 'section_panel_judges',
#                              'separate' : None}

#        self.default_merger = {'docket' : []}
#        self.default_shaper = {self.shape : 'judge'}
#        self.default_streamliner = {}
        return self


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

    def _judge_mapper(self, df, match_column, out_column, map_dict):
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
#
#    def prepare(self):
#
#        return self

    def _set_folders(self):
        self._check_steps()
        if self.step in ['sow', 'harvest']:
            self.inventory._add_branch(
                    root_folder = self.inventory.raw,
                    subfolders = [self.jurisdiction, self.case_type,
                                  self.data_source])
        self.inventory.raw = getattr(self.inventory, self.data_source)
        if self.instructions_folder:
            self.inventory.instructions = self.instructions_folder
        else:
            root_folder = os.path.join('.', 'courtpy')
            self.inventory.add_folders(root_folder = root_folder,
                                       subfolders = 'instructions')
        return self
#
#    def prepare(self):
#        super().prepare()
#        self._set_folders()
#        return self
#
#    def start(self, ingredients = None):
#        for plan in self.plans:
#            self.step = plan.name
#            self.conform()
#            self.ingredients = plan.start(ingredients = self.ingredients)
#            self.inventory.save(variable = self.ingredients,
#                                file_name = self.step + '_ingredients')
#        return self