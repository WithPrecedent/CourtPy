
from dataclasses import dataclass

from .entity import Entity


@dataclass
class Judgment(Entity):

    number : int = 0
    sections : object = None
    parties : object = None
    court : object = None
    docket_numbers : object = None
    cites : object = None
    precedental : bool = True
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



    def __post_init__(self):
        self.table_columns = {}
        return self