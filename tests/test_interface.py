"""Tests the interface module."""

from __future__ import annotations

import pathlib

import chrisjen
import pytest

import courtpy


def test_cases_section_is_not_a_worker() -> None:
    idea = chrisjen.Idea.create({
        'cases': {'source': 'court_listener'},
        'study_project': {'study_workers': 'explorer'},
        'explorer': {'techniques': 'summarize'}})
    assert 'cases' not in idea.workers


def test_build(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    dataset = courtpy.build(
        {'folder': 'court_listener', 'save': 'tables/cases.csv'}, root = tmp_path)
    assert len(dataset.data) == 3
    assert 'outcome_reversal' in dataset.data.columns
    assert (tmp_path / 'tables' / 'cases.csv').is_file()
    reused = courtpy.build(
        {'folder': 'missing', 'save': 'tables/cases.csv', 'reuse': 'true'}, root = tmp_path)
    assert list(reused.data.index) == ['1', '2', '3']
    plain = courtpy.build({'folder': court_listener_folder, 'coders': 'none'})
    assert 'outcome_reversal' not in plain.data.columns


def test_build_without_cases(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match = 'no court_listener cases were found'):
        courtpy.build({'folder': 'empty'}, root = tmp_path)


def test_collect(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert courtpy.collect({'download': 'none'}, root = tmp_path) == []
    with pytest.raises(ValueError, match = 'only be downloaded from court_listener'):
        courtpy.collect({'source': 'lexis_nexis', 'download': 'api'}, root = tmp_path)
    with pytest.raises(ValueError, match = 'download must be'):
        courtpy.collect({'download': 'ftp', 'courts': 'ca1'}, root = tmp_path)
    calls = []
    monkeypatch.setattr(
        courtpy.bulk.BulkData, 'extract',
        lambda self, **kwargs: calls.append((self.date, kwargs)) or [])
    courtpy.collect({
        'download': 'bulk', 'courts': 'ca1, ca2', 'start_date': '2020-01-01',
        'bulk_date': '2026-09-30', 'max_cases': '5'}, root = tmp_path)
    date, arguments = calls[0]
    assert date == '2026-09-30'
    assert arguments['courts'] == ['ca1', 'ca2']
    assert arguments['max_cases'] == 5
    assert arguments['folder'] == tmp_path / 'court_listener'


def test_collect_splits_lexis_batches(tmp_path: pathlib.Path) -> None:
    fixtures = pathlib.Path(__file__).parent / 'fixtures'
    saved = courtpy.collect(
        {'source': 'lexis_nexis', 'batches': str(fixtures)}, root = tmp_path)
    assert len(saved) == 2
    assert saved[0].parent == tmp_path / 'lexis_nexis'


def test_project(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    settings = {
        'general': {'seed': 43, 'label': 'outcome_reversal'},
        'cases': {'folder': str(court_listener_folder)},
        'appeals_project': {'appeals_workers': 'wrangler, explorer'},
        'wrangler': {'techniques': 'drop_text'},
        'explorer': {'techniques': 'summarize, label_balance'},
        'files': {'root_folder': str(tmp_path)}}
    project = courtpy.Project.create(settings)
    result = project.result
    assert isinstance(project, courtpy.Project)
    assert result.label == 'outcome_reversal'
    assert [e['technique'] for e in result.history][:3] == [
        'code_parties', 'code_case_type', 'code_outcome']
    assert 'drop_text' in [e['technique'] for e in result.history]
    assert 'party1' not in result.data.columns
    assert set(result.tables) == {'summarize', 'label_balance'}


def test_project_from_ini(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'study.ini'
    path.write_text(
        '[general]\nlabel = outcome_reversal\n\n'
        f'[cases]\nfolder = {court_listener_folder.as_posix()}\ncoders = code_parties, code_case_type, code_outcome\n\n'
        '[study_project]\nstudy_workers = explorer\n\n'
        '[explorer]\ntechniques = summarize\n', encoding = 'utf-8')
    project = courtpy.Project.create(path, clerk = tmp_path)
    assert len(project.result.data) == 3
