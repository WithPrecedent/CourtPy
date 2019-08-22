
import dataclasses as dataclass

import pandas as pd
import sqlalchemy as db
from sqlalchemy import (Table, Boolean, Column, Date, Integer, Numeric, String,
                        MetaData, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm import sessionmaker

from .entity import Entity

Base = declarative_base()
engine = db.create_engine('sqlite:///court_database.db', echo = True)
Session = sessionmaker(bind = engine)
session = Session()

Base.metadata.create_all(engine)

@dataclass
class Sorcery(Entity):

    def __post_init__(self):
        self.connection = engine.connect()
        self.metadata = db.MetaData()
        return self

    def add_records(self, table_name, records):
        query = db.insert(table_name)
        records = self._listify(records)
        sql_set = self.connection.execute(query, self._listify(records))
        return self

    def close_sql(self):
        self.connection.close()

    def convert_to_df(self, sql_table):
        df = pd.DataFrame(sql_table)
        df.columns = sql_table.keys()
        return df

    def create_table(self):

        return self

class Judge(Base):

    __tablename__ = 'judges'

    name = Column(String(50), index = True, primary_key = True)
    first_name = Column(String(20))
    middle_name = Column(String(20))
    last_name = Column(String(20))
    suffix = Column(String(10))
    number = Column(Integer)
    start_year = Column(Integer)
    end_year = Column(Integer)
    home_court = Column(String(100), ForeignKey('courts.name'))
    other_courts = relationship('Court', uselist = True,
                                back_populates = 'name')
    active_date : Column(Date)
    senior_date : Column(Date)
    end_date : Column(Date)
    appointment : object = None
    behavior : object = None
    demographics : object = None
    experience : object = None
    name_permutations : object = None

class Court(Base):

    __tablename__ = 'courts'

    name = Column(String(100), index = True, primary_key = True)
    number = Column(Integer)
    start_year = Column(Integer)
    end_year = Column(Integer)
    jurisdiction : Column(String(20), ForeignKey('jurisdictions.name'))
    level : Column(String(20))
    judges = relationship('Judge', uselist = True, back_populates = 'name')
    lower_courts : object = None
    geography : object = None
    higher_courts : object = None
    judgments : object = None
    time_table : object = None

class Judgment(Entity):

    number = Column(Integer, primary_key = True)
    sections : object = None
    parties : object = None
    court : object = None
    docket_numbers = Column(String(100))
    cites : object = None
    precedental : Column(Boolean)
    notice : object = None
    dates : object = None
    history : object = None
    future : object = None
    counsel : object = None
    disposition_header : object = None
    author : object = None
    judges : object = None
    majority : object = None
    concurrences : object = None
    dissents : object = None
    references : object = None
    issues : object = None