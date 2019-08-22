
from dataclasses import dataclass
import datetime as dt

from .aggregator import Aggregator
from .entity import Entity


@dataclass
class Judge(Aggregator, Entity):

    name : str = ''
    number : int = 0
    start_year : int = 0
    end_year : int = 2100
    home_court : object = None
    other_courts : object = None
    active_date : object = None
    senior_date : object = None
    end_date : object = None
    appointment : object = None
    behavior : object = None
    demographics : object = None
    experience : object = None
    name_permutations : object = None

    def __post_init__(self):
        self.table_columns = {
                'active' : ['bool_date', 'active_date', 'senior_date'],
                'senior' : ['bool_date', 'senior_date', 'end_date'],
                'ideology_behavior' : ['ideology', 'behavior'],
                'judgments' : ['count', 'behavior', 'judgments'],
                'reversals' : ['count', 'behavior', 'reversals'],
                'affirmances' :  ['count', 'behavior', 'affirmances']}
        return self

    @property
    def ideology_behavior(self, year):
        return self.behavior.ideology

    @property
    def ideology_party(self, year):
        return self.appointment.executive_party

    @property
    def ideology_special(self, year):
        return self.appointment.special_ideology


@dataclass
class Appointment(Entity):

    executive : str = ''
    executive_party : int = 0
    vote_type : str = 'voice'
    vote_percent : float = 0.0
    legis_ideology : float = 0.0
    special_ideology : float = 0.0
    lawyer_rating : str = ''
    recess : bool = False

    def __post_init__(self):

        return self

@dataclass
class Behavior(Entity):

    judgments : int = 0
    ideology : float = 0.0
    reversals : int = 0
    affirmances : int = 0
    subseq_reversals : int = 0
    subseq_affirmances : int = 0
    dissents : int = 0
    concurrences : int = 0
    mixed_opinions : int = 0

    def __post_init__(self):

        return self

@dataclass
class Demographics(Entity):

    woman : bool = False
    african_american : bool = False
    hispanic : bool = False
    asian_american : bool = False
    american_indian : bool = False
    birth_date : dt.datetime = 1/1/1000
    death_date : dt.datetime = 1/1/1000

    def __post_init__(self):

        return self

@dataclass
class Experience(Entity):

    prosecutor : bool = False
    ausa : bool = False
    public_defender : bool = False
    federal_clerk : bool = False
    scotus_clerk : bool = False
    sg_office : bool = False
    lower_judge : bool = False
    military : bool = False
    big_law : bool = False
    law_profs : bool = False
    undergrad : str = 'none'
    law_school : str = 'none'

    def __post_init__(self):

        return self
