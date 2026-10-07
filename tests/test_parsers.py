"""Tests the parsers and cases modules with CourtListener cases."""

from __future__ import annotations

import logging
import pathlib

import pandas as pd
import pytest

import courtpy
from courtpy import cases, parsers


def test_parse_court_listener(court_listener_folder: pathlib.Path) -> None:
    table = courtpy.parse(court_listener_folder)
    assert list(table.index) == ['1', '2', '3']
    assert table.index.name == 'case_id'
    assert list(table.columns[:4]) == ['source', 'cluster_id', 'docket_id', 'court']
    doe = table.loc['1']
    assert doe['court'] == 'ca1' and doe['court_num'] == 1
    assert doe['party1'] == 'UNITED STATES of America, Plaintiff-Appellee,'
    assert doe['party2_appellant'] and doe['party1_united_states']
    assert doe['disposition_reverse'] and doe['disposition_remand']
    assert doe['panel_judges'] == ['LYNCH', 'HOWARD', 'THOMPSON']
    assert doe['panel_size'] == 3 and doe['author'] == 'LYNCH'
    assert doe['references_case'] == ['554 U.S. 570']
    assert doe['references_statute'][0].startswith('18 U.S.C. § 922')
    assert doe['cite_federal_reporter'] and doe['published']
    assert table.loc['2', 'dissenting'] == ['THOMPSON']
    assert table.loc['2', 'civil_titlevii']
    assert pd.api.types.is_bool_dtype(table['published'].dtype)
    assert table['year'].dtype == 'int64'


def test_missing_values_get_nullable_types() -> None:
    data = pd.DataFrame({'published': [True, None], 'year': [2020.0, None], 'x': ['a', None]})
    tidied = cases.tidy(data)
    assert tidied['published'].dtype == 'boolean'
    assert tidied['year'].dtype == 'Int64'
    assert tidied['x'].dtype != 'boolean'


def test_find_files_skips_hidden_folders(court_listener_folder: pathlib.Path) -> None:
    hidden = court_listener_folder / '.courtpy'
    hidden.mkdir()
    (hidden / 'state.json').write_text('{}', encoding = 'utf-8')
    assert len(parsers.find_files(court_listener_folder)) == 3
    with pytest.raises(ValueError, match = 'source must be one of'):
        parsers.find_files(court_listener_folder, 'westlaw')


def test_unreadable_case_is_skipped(
    court_listener_folder: pathlib.Path,
    caplog: pytest.LogCaptureFixture) -> None:
    (court_listener_folder / 'ca1' / '9.json').write_text('not json', encoding = 'utf-8')
    with caplog.at_level(logging.WARNING):
        table = courtpy.parse(court_listener_folder)
    assert len(table) == 3
    assert 'could not be parsed' in caplog.text


def test_parse_with_workers(court_listener_folder: pathlib.Path) -> None:
    table = courtpy.parse(court_listener_folder, workers = 2)
    assert list(table.index) == ['1', '2', '3']


def test_custom_rulebook(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    rules = tmp_path / 'mine.csv'
    rules.write_text(
        'target,variable,kind,pattern,value,ignorecase,dotall,note\n'
        'opinion,mentions_firearm,flag,firearm,,TRUE,FALSE,\n'
        'opinion,we_count,count,\\bwe\\b,,TRUE,FALSE,\n', encoding = 'utf-8')
    table = courtpy.parse(court_listener_folder, rulebooks = [rules])
    assert table.loc['1', 'mentions_firearm'] and not table.loc['2', 'mentions_firearm']
    assert table.loc['1', 'we_count'] == 2
    assert 'party1_appellant' not in table.columns


def test_save_and_load(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    table = courtpy.parse(court_listener_folder)
    path = cases.save_cases(table, tmp_path / 'out' / 'cases.csv')
    assert (tmp_path / 'out' / 'cases.lists.json').is_file()
    loaded = cases.load_cases(path)
    assert list(loaded.index) == ['1', '2', '3']
    assert loaded.loc['1', 'panel_judges'] == ['LYNCH', 'HOWARD', 'THOMPSON']
    assert loaded.loc['3', 'dissenting'] == []
    assert pd.api.types.is_bool_dtype(loaded['published'].dtype)
    pd.testing.assert_series_equal(loaded['court_num'], table['court_num'], check_dtype = False)


def test_parser_metadata_wins(court_listener_folder: pathlib.Path) -> None:
    parser = parsers.Parser.create('federal')
    case = cases.Case(
        id = 'x', source = 'court_listener',
        sections = {'court': 'Court of Appeals for the First Circuit'},
        metadata = {'court_num': 99, 'agency': None})
    row = parser.parse(case)
    assert row['court_num'] == 99
    assert row['agency'] is None
    assert row['word_count'] == 0
