"""The study in "federal_appeals.ini", one step at a time.

Each step can be run on its own, and each saves its work, so the slow steps
(downloading and parsing) need to be run only once.
"""

from __future__ import annotations

import pathlib

import amos

import courtpy

DATA = pathlib.Path('data')
CASES = DATA / 'court_listener'
TABLE = DATA / 'cases.csv'

# 1. Collect the cases.
#
# The bulk data needs no API key and has no limits, but its opinions file is
# more than 50 GB, so extracting cases from it takes a few hours. The bulk
# files are kept (outside this folder) so that they can be used again.
if not CASES.exists():
    courtpy.BulkData().extract(
        courts = 'federal_circuits',
        start_date = '2019-01-01',
        end_date = '2019-12-31',
        folder = CASES)

# For a small or very recent set of cases, use the API instead. Store your key
# once with "courtpy key set" (or courtpy.secrets.set_api_key) and it is found
# automatically. A free account can make 125 requests a day, about 20 cases
# each, and a download that is stopped by the limit resumes where it stopped.
#
#     courtpy.CourtListener().download(
#         'ca1', '2026-09-01', '2026-09-30', CASES, max_cases = 100)

# 2. Parse the cases into a table, using the rules in courtpy's
# "instructions" folder (or your own CSV files of rules), and code the parties,
# case types, and outcomes.
if TABLE.exists():
    table = courtpy.load_cases(TABLE)
else:
    table = courtpy.parse(CASES, rulebooks = 'federal', workers = 4)
    table = courtpy.code(table).data
    courtpy.save_cases(table, TABLE)
print(table['outcome_reversal'].mean(), 'of the decisions reversed')

# 3. Analyze the cases with amos. The settings file's "cases" section is not
# needed, because the table is passed as the item.
project = courtpy.Project.create(
    pathlib.Path(__file__).with_name('federal_appeals.ini'), item = table)
print(project.report.contents)
print(amos.evaluators.Scorecard.create(project.result).to_markdown())
project.export()
