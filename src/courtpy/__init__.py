"""CourtPy: accessible tools for collecting, parsing, and analyzing court opinions
Corey Rayburn Yung <coreyrayburnyung@gmail.com>
Copyright 2020-2026, Corey Rayburn Yung
License: Apache-2.0

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

"""

# For Developers:
#
# As with all of my packages, I use Google-style docstrings and follow the
# Google Python Style Guide (https://google.github.io/styleguide/pyguide.html)
# with two notable exceptions:
#     1) I always add spaces around '='. This is because I find it more
#         readable and it is practically the norm with type annotations adding
#         the spaces to function and method signatures. I realize that this
#         will seem alien to many coders, but it is far easier on my eyes.
#     2) I've expanded the Google exception for importing multiple items from
#         one package from just 'typing' to also include 'collections.abc'.
#         This is because, as of python 3.9, many of the type annotations in
#         'typing' are being depreciated and have already been combined with
#         the similarly named types in 'collections.abc'.
#
# My packages lean heavily toward over-documentation and verbosity. This is
# designed to make them more accessible to beginning coders and generally more
# usable. The one exception to that general rule is unit tests, which hopefully
# are clear enough to not require further explanation. If there is any area of
# the documentation that could be made clearer, please don't hesitate to email
# me or any other package maintainer - I want to ensure the package is as
# accessible and useful as possible.

from __future__ import annotations

__version__ = '0.2.0'

__author__: str = 'Corey Rayburn Yung'

__all__: list[str] = [
    'BulkData',
    'Case',
    'CourtListener',
    'Parser',
    'Project',
    'Rule',
    'Rulebook',
    'build',
    'code',
    'coders',
    'collect',
    'load_cases',
    'parse',
    'save_cases',
    'secrets',
]

import chrisjen.options

from . import options

# The "cases" section of a project's settings describes its cases. It is not
# a worker, so `chrisjen` must not try to build it.
if options._CASES_SECTION not in chrisjen.options._SPECIAL_SETTINGS:
    chrisjen.options._SPECIAL_SETTINGS.append(options._CASES_SECTION)

# Importing `coders` adds its techniques to the `amos` library, so they can be
# named in settings as soon as courtpy is imported.
from . import coders, secrets
from .bulk import BulkData
from .cases import Case, load_cases, save_cases
from .courtlistener import CourtListener
from .interface import Project, build, code, collect
from .parsers import Parser, parse
from .rules import Rule, Rulebook
