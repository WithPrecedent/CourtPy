"""Runs the example study on synthetic cases."""

from __future__ import annotations

import pathlib
import random

import chrisjen
from conftest import make_cluster, make_opinion, write_case

import courtpy

EXAMPLES = pathlib.Path(__file__).parents[1] / 'examples'
OUTCOMES = ('Affirmed.', 'Reversed and remanded.', 'Vacated.', 'Affirmed in part.')
PARTIES = (
    'UNITED STATES of America, Plaintiff-Appellee, v. John DOE, Defendant-Appellant',
    'Mary SMITH, Plaintiff-Appellant, v. ACME CORP., Defendant-Appellee',
    'Pat LEE, Petitioner, v. Merrick GARLAND, Attorney General, Respondent')
ISSUES = (
    'The sentencing guidelines range was correct.',
    'He argues that the search violated the Fourth Amendment.',
    'Our review is de novo.',
    'She appeared pro se and raised a section 1983 claim.',
    'The district court did not abuse its discretion.')


def test_federal_appeals_example(tmp_path: pathlib.Path) -> None:
    rng = random.Random(7)
    folder = tmp_path / 'data' / 'court_listener'
    for number in range(1, 81):
        court = rng.choice(['ca1', 'ca2', 'ca9'])
        text = ' '.join(rng.sample(ISSUES, 2))
        write_case(
            folder,
            make_cluster(
                number, case_name_full = rng.choice(PARTIES),
                disposition = rng.choice(OUTCOMES),
                date_filed = f'2019-{rng.randint(1, 12):02d}-15'),
            [make_opinion(number * 10, number, html_with_citations = f'<p>{text}</p>')],
            court = court)
    idea = chrisjen.Idea.create(EXAMPLES / 'federal_appeals.ini')
    idea['load_court_listener_parameters']['download'] = 'none'
    project = courtpy.Project.create(idea, clerk = tmp_path)
    result = project.result
    assert result.label == 'outcome_reversal'
    assert [e['technique'] for e in result.history][:4] == [
        'load_court_listener', 'code_parties', 'code_case_type', 'code_outcome']
    assert (tmp_path / 'data' / 'cases.csv').is_file()
    assert set(result.tables) >= {'summarize', 'label_balance', 'analyst_comparison', 'scorecard'}
    assert 'panel_judges' not in result.data.columns
    assert 0 <= result.metrics['roc_auc'] <= 1
    folder = project.export()
    assert (folder / 'scorecard.csv').is_file()
