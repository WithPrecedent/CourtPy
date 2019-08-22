"""
Primary class for merging court opinion data from different sources.
"""

from dataclasses import dataclass

from simplify import timer
from simplify.almanac.steps import Bundle


@timer('Data merging')
@dataclass
class CPBundle(Bundle):

    technique : str = ''
    techniques : object = None
    parameters : object = None
    auto_prepare : bool = True
    name : str = 'bundler'

    def __post_init__(self):
        return