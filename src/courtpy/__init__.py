"""Accessible tools for collecting, parsing, and analyzing court opinions"""

from __future__ import annotations

__version__ = '0.3.0'

__author__: str = 'Corey Rayburn Yung'

__all__: list[str] = [
    'BulkData',
    'Case',
    'CaseLoader',
    'CourtListener',
    'Parser',
    'Project',
    'Rule',
    'Rulebook',
    'code',
    'coders',
    "fjc_idb",
    "jcs",
    'judges',
    'load_cases',
    'loaders',
    'parse',
    'politics',
    'save_cases',
    'scdb',
    "secrets"]

# A study is an `amos.Project`, which courtpy offers under the same name.
from amos import Project

# Importing `coders`, `fjc_idb`, `judges`, `loaders`, `politics`, and `scdb`
# adds their techniques to the `amos` library, so they can be named in
# settings as soon as courtpy is imported.
from . import coders, fjc_idb, jcs, judges, loaders, politics, scdb, secrets
from .bulk import BulkData
from .cases import Case, load_cases, save_cases
from .coders import code
from .courtlistener import CourtListener
from .loaders import CaseLoader
from .parsers import Parser, parse
from .rules import Rule, Rulebook
