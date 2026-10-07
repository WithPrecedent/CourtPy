"""Tests the coders module."""

from __future__ import annotations

import pathlib

import amos
import pandas as pd
import pytest

import courtpy
from courtpy import coders


@pytest.fixture
def coded(court_listener_folder: pathlib.Path) -> amos.Dataset:
    return courtpy.code(courtpy.parse(court_listener_folder))


def test_coders_are_in_the_library() -> None:
    for name in ('code_parties', 'code_case_type', 'code_outcome', 'drop_text'):
        assert amos.library.classify(name) == 'cleaner'


def test_code_parties(coded: amos.Dataset) -> None:
    data = coded.data
    assert data.loc['1', 'party2_appealing'] and data.loc['1', 'party1_defending']
    assert data.loc['2', 'party1_appealing'] and data.loc['2', 'party2_defending']
    assert not data.loc['3', 'party1_appealing'] and not data.loc['3', 'party1_defending']
    assert [entry['technique'] for entry in coded.history] == [
        'code_parties', 'code_case_type', 'code_outcome']


def test_code_parties_completes_opposites() -> None:
    roles = ['appellant', 'appellee', 'petitioner', 'respondent', 'plaintiff', 'defendant']
    data = pd.DataFrame({f'{s}_{r}': [False] for s in ('party1', 'party2') for r in roles})
    data['party2_appellant'] = True
    data['party1_plaintiff'] = True
    result = coders.CodeParties().apply(amos.Dataset(data)).data
    assert result.loc[0, 'party1_appellee'] and result.loc[0, 'party2_defendant']
    assert result.loc[0, 'party2_appealing'] and result.loc[0, 'party1_defending']


def test_code_case_type(coded: amos.Dataset) -> None:
    data = coded.data
    assert list(data['type_criminal']) == [True, False, False]
    assert data.loc['1', 'party1_prosecution'] and data.loc['1', 'party2_criminal_defendant']
    assert data.loc['2', 'party1_civil_plaintiff'] and data.loc['2', 'party2_civil_defendant']


def test_code_outcome(coded: amos.Dataset) -> None:
    data = coded.data
    # Case 1 is reversed by its disposition, case 2 affirmed (although its
    # dissent says "reverse"), and case 3 has no disposition, so the opinion's
    # "We vacate" is used.
    assert list(data['outcome_reversal']) == [True, False, True]
    assert data.loc['1', 'outcome_party2_won'] and not data.loc['1', 'outcome_party1_won']
    assert data.loc['1', 'outcome_criminal_defendant_won']
    assert not data.loc['1', 'outcome_prosecution_won']
    assert pd.isna(data.loc['1', 'outcome_civil_plaintiff_won'])
    assert data.loc['1', 'appeal_by_defendant']
    assert not data.loc['2', 'outcome_civil_plaintiff_won']
    assert data.loc['2', 'outcome_civil_defendant_won']
    assert not data.loc['2', 'appeal_by_defendant']
    assert pd.isna(data.loc['3', 'outcome_party1_won'])
    assert data['outcome_party1_won'].dtype == 'boolean'


def test_drop_text(coded: amos.Dataset) -> None:
    coded.data['case_name'] = coded.data['case_name'].astype('category')
    coders.DropText().apply(coded, keep = ['court'])
    data = coded.data
    for column in ('party1', 'panel_judges', 'date_filed', 'url', 'references_case'):
        assert column not in data.columns
    for column in ('court', 'case_name', 'court_num', 'published', 'outcome_party1_won', 'year'):
        assert column in data.columns


def test_drop_text_keeps_label() -> None:
    dataset = amos.Dataset(pd.DataFrame({'note': ['a', 'b'], 'x': [1, 2]}), label = 'note')
    coders.DropText().apply(dataset)
    assert list(dataset.data.columns) == ['note', 'x']


def test_missing_columns() -> None:
    with pytest.raises(KeyError, match = 'federal rules'):
        coders.CodeOutcome().apply(amos.Dataset(pd.DataFrame({'x': [1]})))
