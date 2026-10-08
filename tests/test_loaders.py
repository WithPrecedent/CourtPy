"""Tests the loaders module."""

from __future__ import annotations

import pathlib

import amos
import nagata
import pytest

import courtpy
from courtpy import loaders

FIXTURES = pathlib.Path(__file__).parent / 'fixtures'


def test_loaders_are_in_the_library() -> None:
    assert sorted(amos.library.get_genre('case_loader')) == [
        'load_cases', 'load_court_listener', 'load_lexis_nexis']
    assert issubclass(loaders.LoadCourtListener, amos.Loader)


def test_load_court_listener(court_listener_folder: pathlib.Path) -> None:
    dataset = loaders.LoadCourtListener().apply(
        source = court_listener_folder, label = 'outcome_reversal')
    assert dataset.label == 'outcome_reversal'
    assert dataset.task == 'classify'
    assert len(dataset.data) == 3
    assert [e['technique'] for e in dataset.history] == [
        'load_court_listener', 'code_parties', 'code_case_type', 'code_outcome']
    assert dataset.history[0]['rows'] == 3


def test_coders_none(court_listener_folder: pathlib.Path) -> None:
    dataset = loaders.LoadCourtListener().apply(
        source = court_listener_folder, coders = 'none')
    assert 'outcome_reversal' not in dataset.data.columns
    with pytest.raises(KeyError, match = 'label'):
        loaders.LoadCourtListener().apply(
            source = court_listener_folder, coders = 'none',
            label = 'outcome_reversal')


def test_save_and_reuse(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    clerk = nagata.FileManager(root_folder = tmp_path, interim_folder = 'interim')
    loader = loaders.LoadCourtListener(clerk = clerk)
    loader.apply(source = court_listener_folder, save = 'cases.csv')
    saved = pathlib.Path(clerk.interim_folder) / 'cases.csv'
    assert saved.is_file()
    reused = loader.apply(
        source = tmp_path / 'missing', save = 'cases.csv', reuse = 'true',
        label = 'outcome_reversal')
    assert reused.history[0]['reused'] == str(saved)
    assert len(reused.history) == 1
    assert reused.data.loc['1', 'panel_judges'] == ['LYNCH', 'HOWARD', 'THOMPSON']


def test_project_with_a_loader(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    folder = tmp_path / 'data' / 'court_listener'
    folder.parent.mkdir()
    court_listener_folder.rename(folder)
    settings = {
        'general': {'seed': 43, 'label': 'outcome_reversal'},
        'files': {'input_folder': 'data'},
        'appeals_project': {'appeals_workers': 'wrangler, explorer'},
        'wrangler': {'techniques': 'load_court_listener, drop_text'},
        'explorer': {'techniques': 'summarize, label_balance'}}
    project = courtpy.Project.create(settings, clerk = tmp_path)
    result = project.result
    assert result.label == 'outcome_reversal'
    assert result.history[0] == {
        'technique': 'load_court_listener', 'source': 'court_listener',
        'rows': 3, 'columns': result.history[0]['columns']}
    assert 'party1' not in result.data.columns
    assert set(result.tables) == {'summarize', 'label_balance'}


def test_no_cases(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match = 'no court_listener cases were found'):
        loaders.LoadCourtListener().apply(source = tmp_path)


def test_download(
    court_listener_folder: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(
        courtpy.bulk.BulkData, 'extract',
        lambda self, **kwargs: calls.append((self.date, kwargs)) or [])
    loaders.LoadCourtListener().apply(
        source = court_listener_folder, download = 'bulk', courts = 'ca1, ca2',
        start_date = '2020-01-01', bulk_date = '2026-09-30', max_cases = '5')
    date, arguments = calls[0]
    assert date == '2026-09-30'
    assert arguments['courts'] == ['ca1', 'ca2']
    assert arguments['max_cases'] == 5
    assert arguments['folder'] == court_listener_folder
    with pytest.raises(ValueError, match = 'download must be'):
        loaders.LoadCourtListener().apply(
            source = court_listener_folder, download = 'ftp', courts = 'ca1')
    with pytest.raises(courtpy.secrets.MissingAPIKeyError):
        loaders.LoadCourtListener().apply(
            source = court_listener_folder, download = 'api', courts = 'ca1')


def test_load_lexis_nexis(tmp_path: pathlib.Path) -> None:
    dataset = loaders.LoadLexisNexis().apply(
        source = tmp_path / 'lexis', batches = str(FIXTURES))
    assert len(dataset.data) == 2
    assert len(list((tmp_path / 'lexis').glob('*.txt'))) == 2
    assert dataset.data.loc['lexis_batch_00001', 'outcome_reversal']


def test_load_cases(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    table = courtpy.code(courtpy.parse(court_listener_folder)).data
    path = courtpy.save_cases(table, tmp_path / 'cases.csv')
    dataset = loaders.LoadCases().apply(source = path, label = 'outcome_reversal')
    assert [e['technique'] for e in dataset.history] == ['load_cases']
    assert dataset.data.loc['1', 'panel_judges'] == ['LYNCH', 'HOWARD', 'THOMPSON']
    with pytest.raises(ValueError, match = 'has nothing to load'):
        loaders.LoadCases().apply()
    with pytest.raises(FileNotFoundError, match = 'no table of cases'):
        loaders.LoadCases().apply(source = tmp_path / 'missing.csv')
