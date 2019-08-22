
from dataclasses import dataclass

from .aggregator import Aggregator
from .entity import Entity


@dataclass
class Court(Aggregator, Entity):

    name : str = ''
    start_year : int = 0
    end_year : int = 2100
    jurisdiction : object = None
    level : object = None
    judges : object = None
    lower_courts : object = None
    geography : object = None
    higher_courts : object = None
    judgments : object = None
    time_table : object = None

    def __post_init__(self):
        self.table_columns = {
                'ideology_party' : ['sum', 'judges', 'party'],
                'ideology_special' : ['sum', 'judges', 'ideology'],
                'num_judges' : ['count', 'judges'],
                'num_active_judges' : ['count', 'judges', 'active'],
                'num_senior_judges' : ['count', 'judges', 'senior'],
                'num_judgments' : ['count', 'judgments'],
                'num_affirmances' : ['count', 'judgments', 'affirmance'],
                'num_reversals' : ['count', 'judgments', 'reversal']}
        return self

    @property
    def ideology_party(self, year):
        if not self.time_table:
            self._create_time_table()
        return self.ideology_party_year[year, 'ideology_party']

    @property
    def ideology_special(self, year):
        if not self.time_table:
            self._create_time_table()
        return self.ideology_party_year[year, 'ideology_special']

    def add_higher_court(self, higher_court):
        self.higher_courts = self._check_dict(self.higher_courts)
        self.higher_courts.update({higher_court.name: higher_court})
        return self

    def add_geography(self, geographic_unit):
        self.geography = self._check_dict(self.geography)
        self.geography.update({geographic_unit.name: geographic_unit})
        return self

    def add_judge(self, judge):
        self.judges = self._check_dict(self.judges)
        self.judges.update({judge.name: judge})
        return self

    def add_judgment(self, judgment):
        self.judgments = self._check_dict(self.judgments)
        self.judgments.update({judgment.number : judgment})
        return self

    def add_lower_court(self, lower_court):
        self.lower_courts = self._check_dict(self.lower_courts)
        self.lower_courts.update({lower_court.name: lower_court})
        return self

    def set_jurisdiction(self, jurisdiction):
        self.jurisdiciton = jurisdiction
        return self

    def set_level(self, level):
        self.level = level
        return self
