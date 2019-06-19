
from dataclasses import dataclass
import os
import pandas as pd

from simplify.tools.matcher import ReMatch

from .rules.sources import CaselawAccess, CourtListener, FJCIDB, LexisNexis


@dataclass
class Rules(object):
    """Defines rules for cases data dependent upon case_type, jurisdiction, and
    data source.
    """
    case_type : str
    jurisdiction : str
    source : str
    stage : str
    quick_start : bool = True
    prefixes : object = None
    options : object = None

    def __post_init__(self):
        self.settings.localize(instance = self,
                               sections = ['general', 'methods'])
        self.options = {'caselaw_access' : CaselawAccess,
                        'court_listener' : CourtListener,
                        'fjc_idb' : FJCIDB,
                        'lexis_nexis' : LexisNexis}
        self.source_attributes = ['source_file_type', 'case_divider',
                                  'import_columns', 'renames',
                                  'missing_sections', 'meta_columns']
        self.initialize()
        if self.quick_start:
            self.create()
        return self

    def _conform_to_stage(self):
        for prefix, d_type in self.prefixes.items():
            if self.stage in ['parser', 'wrangler']:
                if d_type in ['category', 'encoder', 'interactor']:
                    self.prefixes[prefix] = str
            elif self.stage in ['engineer', 'analyzer']:
                if d_type in ['list', 'pattern']:
                    self.prefixes[prefix] = 'category'
        return self

    def _initialize_court_rules(self):
        self.court_type_rules = {
                'high_court' : {'missing_sections' : ['politics_high_court']},
                'appellate' : {'missing_sections' : []},
                'trial' : {'missing_sections' : ['history', 'standard']}}
        self.court_names_path = os.path.join(
                'rules', 'jurisdictions', self.jurisdiction + '_courts.csv')
        return self

    def _initialize_section_rules(self):
        self.index_column = 'index_' + self.source
        self.default_prefixes = {'index' : int,
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
        return self

    def _localize(self, attribute_list, class_instance):
        for attribute in attribute_list:
            setattr(self, attribute, getattr(class_instance, attribute))
        return self

#    def _make_dict(self, df_dict, key, value):
#        return df_dict.set_index(key).to_dict()[value]

    def _set_court_names(self):
        self.courts_df = pd.read_csv(self.court_names_path)
        self.court_nums = ReMatch(keys = 'regex', values = 'num',
                                  file_path = self.courts_names_path)
        high_court_df = self.courts_df[self.courts_df['num' < 100]]
        self.high_courts = ReMatch(keys = high_court_df['regex'],
                                   values = high_court_df['num'])
        app_court_df = self.courts_df[self.courts_df['num' >= 100]
                                      & self.courts_df['num' < 1000]]
        self.appellate_courts = ReMatch(keys = app_court_df['regex'],
                                        values = app_court_df['num'])
        trial_court_df = self.courts_df[self.courts_df['num' >= 1000]
                                        & self.courts_df['num' < 10000]]
        self.trial_courts = ReMatch(keys = trial_court_df['regex'],
                                    values = trial_court_df['num'])
        other_court_df = self.courts_df[self.courts_df['num' >= 10000]]
        self.other_courts = ReMatch(keys = other_court_df['regex'],
                                    values = other_court_df['num'])
        return self

    def create(self):
        if not self.prefixes:
            self.prefixes = self.default_prefixes
        self._conform_to_stage()
        self._localize(self.source_attributes, self.sources[self.source])
        self._set_court_names()
        return self

    def initialize(self):
        self._initialize_section_rules()
        self._initialize_court_rules()
        return self