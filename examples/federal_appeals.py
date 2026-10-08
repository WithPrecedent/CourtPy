"""The study in "federal_appeals.ini", from Python.

`whole_study` runs the study as the settings file describes it: its wrangler's
first technique, `load_court_listener`, collects, parses, and codes the cases.
`step_by_step` does the same work one step at a time, which is useful for
looking at the cases before analyzing them.
"""

from __future__ import annotations

import pathlib

import courtpy

HERE = pathlib.Path(__file__).parent
# The settings file's "files" section keeps the cases and the coded table in
# "data" and the results in "results", in the folder of the project's clerk.
CASES = HERE / 'data' / 'court_listener'
TABLE = HERE / 'data' / 'cases.csv'


def whole_study() -> courtpy.Project:
    """Runs the study described by "federal_appeals.ini".

    The first run extracts the cases from CourtListener's bulk data, which
    takes a few hours but needs no API key. Later runs reuse the coded table.

    Returns:
        The applied project.

    """
    project = courtpy.Project.create(
        HERE / 'federal_appeals.ini', clerk = HERE)
    print(project.report.contents)
    project.export()
    return project


def step_by_step() -> None:
    """Collects, parses, codes, and loads the cases one step at a time."""
    # 1. Collect the cases from the bulk data (or, for a small or recent set
    # of cases, with courtpy.CourtListener().download and your API key).
    courtpy.BulkData().extract(
        courts = 'federal_circuits',
        start_date = '2019-01-01',
        end_date = '2019-12-31',
        folder = CASES)
    # 2. Parse them with the federal rules and code the outcomes.
    table = courtpy.code(courtpy.parse(CASES, workers = 4)).data
    courtpy.save_cases(table, TABLE)
    print(table['outcome_reversal'].mean(), 'of the decisions reversed')
    # 3. Load the saved table as an amos dataset, ready for any amos technique.
    dataset = courtpy.loaders.LoadCases().apply(
        source = TABLE, label = 'outcome_reversal')
    print(dataset)


if __name__ == '__main__':
    whole_study()
