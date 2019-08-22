"""
.. module:: courtpy_almanac_bundlers
  :synopsis: Bundlers for CourtPy Almanac
"""

from .biographies import Biographies
from .executive import Executive
from .judges import Judges
from .judiciary import Judiciary
from .legislature import Legislature


__version__ = '0.1.0'

__author__ = 'Corey Rayburn Yung'

__all__ = ['Biographies',
           'Executive',
           'Judges',
           'Judiciary',
           'Legislature']