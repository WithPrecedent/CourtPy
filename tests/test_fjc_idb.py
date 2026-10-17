"""Tests the fjc_idb module."""

from __future__ import annotations

import pathlib
from typing import Any

import amos
import fjc_idb
import fjc_idb.main
import pandas as pd
import pytest
from conftest import make_cluster, make_opinion

import courtpy
from courtpy import fjc_idb as idb

# Invented appeals, in the layout of fjc-idb's parsed table of appeals. The
# D.C. Circuit's appeal was remanded by the Supreme Court and ended twice,
# and the Fifth and Eleventh Circuits each have an appeal 18-11574.
APPEALS = [
    ('First Circuit', '0801234', 'No. 08-1234', None, '2008-03-01', '2009-05-01', 'Criminal', 1),
    ('First Circuit', '1701234', 'No. 17-1234', None, '2017-03-01', '2018-05-01', 'Criminal', 1),
    ('D.C. Circuit', '1507034', 'No. 15-7034', None, '2015-04-08', '2019-05-14', 'U.S. civil', 1),
    ('D.C. Circuit', '1507034', 'No. 15-7034', 'Remanded from the Supreme Court', '2020-06-03', '2020-08-18', 'U.S. civil', 0),
    ('Second Circuit', '1803672', 'No. 18-3672', None, '2018-11-01', '2019-06-03', 'Private civil', 0),
    ('Second Circuit', '1803701', 'No. 18-3701', None, '2018-11-05', '2019-06-03', 'Private civil', 0),
    ('Fifth Circuit', '1811574', 'No. 18-11574', None, '2018-10-01', '2019-07-03', 'Criminal', 0),
    ('Eleventh Circuit', '1811574', 'No. 18-11574', None, '2018-04-11', '2019-05-09', 'Prisoner', 0)]
COLUMNS = [
    'circuit', 'docket_num_idb', 'docket_number', 'reopened_or_remanded',
    'docket_date', 'judgment_date', 'appeal_type', 'published']


def table() -> pd.DataFrame:
    """Returns the invented appeals as fjc-idb's table of them."""
    data = pd.DataFrame(APPEALS, columns = COLUMNS)
    for column in ('docket_date', 'judgment_date'):
        data[column] = pd.to_datetime(data[column])
    return data


def cases(**columns: list[Any]) -> amos.Dataset:
    """Returns a dataset of parsed cases with the columns given."""
    return amos.Dataset(pd.DataFrame(columns))


def save(folder: pathlib.Path, number: int, court: str, docket: str, date: str) -> None:
    """Saves a CourtListener case with a docket, as the bulk data does."""
    cluster = make_cluster(number, date_filed = date)
    path = courtpy.courtlistener.case_path(folder, court, cluster['id'])
    courtpy.courtlistener.save_case(path, {
        'source': 'court_listener',
        'court': {'id': court, 'full_name': f'Court of Appeals ({court})'},
        'cluster': cluster,
        'docket': {'id': number * 10, 'court_id': court, 'docket_number': docket},
        'citations': [],
        'opinions': [courtpy.courtlistener.slim_opinion(make_opinion(number * 10, number))]})


def test_code_idb_is_a_merger() -> None:
    assert amos.library.classify('code_idb') == 'merger'


def test_code_idb() -> None:
    dataset = cases(
        court = ['ca1', 'ca5', 'ca11', 'ca2', 'cafc', 'scotus', 'ca1'],
        docket_number = [
            '17-1234', '18-11574', 'No. 18-11574', '18-9999-cv, 18-3701-cv',
            '2018-1234', '17-1234', None],
        date_filed = [
            '2018-05-01', '2019-07-03', '2019-05-09', '2019-06-03',
            '2019-01-01', '2019-01-01', '2018-05-01'])
    result = idb.CodeIdb().apply(dataset, source = table(), indicator = 'found')
    data = result.data
    assert list(data['found']) == [True, True, True, True, False, False, False]
    assert list(data['idb_circuit'].iloc[:4]) == [
        'First Circuit', 'Fifth Circuit', 'Eleventh Circuit', 'Second Circuit']
    # The first docket number that the database has is used.
    assert data['idb_docket_number'].iloc[3] == 'No. 18-3701'
    assert data['idb_appeal_type'].iloc[2] == 'Prisoner'
    assert data['idb_judgment_date'].iloc[:5].isna().tolist() == [False] * 4 + [True]
    assert [c for c in data.columns if c.startswith('idb_')] == [f'idb_{c}' for c in COLUMNS]
    record = result.history[-1]
    assert (record['technique'], record['source'], record['rows'], record['matched']) == (
        'code_idb', 'DataFrame', 7, 4)


def test_reopened_appeals() -> None:
    dataset = cases(
        court = ['cadc'] * 4,
        docket_number = ['15-7034'] * 4,
        date_filed = ['2016-01-01', '2019-05-14', '2020-08-18', '2021-03-01'])
    result = idb.CodeIdb().apply(dataset, source = table(), columns = 'judgment_date')
    # The record whose judgment is nearest the decision is used.
    assert [str(d.date()) for d in result.data['idb_judgment_date']] == [
        '2019-05-14', '2019-05-14', '2020-08-18', '2020-08-18']
    # A record of an appeal docketed after the decision is never used.
    remand = table().drop(index = 2)
    result = idb.CodeIdb().apply(
        cases(court = ['cadc'], docket_number = ['15-7034'], date_filed = ['2019-05-14']),
        source = remand, indicator = 'found')
    assert list(result.data['found']) == [False]
    # Without dates, the latest record is used.
    result = idb.CodeIdb().apply(
        cases(court = ['cadc'], docket_number = ['15-7034']),
        source = table(), columns = 'reopened_or_remanded')
    assert result.data['idb_reopened_or_remanded'].iloc[0] == 'Remanded from the Supreme Court'


def test_rules_columns() -> None:
    # Cases from Lexis-Nexis have the federal rules' numbers for the courts
    # (12 is the D.C. Circuit and 13 the Federal Circuit) and years.
    dataset = cases(
        court_num = [12, 1, 13, None],
        docket_numbers = [['15-7034'], ['17-1234', '17-1235'], ['18-1234'], ['17-1234']],
        year = [2019, 2018, 2019, 2018])
    result = idb.CodeIdb().apply(
        dataset, source = table(), columns = ['judgment_date'], indicator = 'found')
    assert list(result.data['found']) == [True, True, False, False]
    assert str(result.data['idb_judgment_date'].iloc[0].date()) == '2019-05-14'


def test_needed_columns() -> None:
    with pytest.raises(KeyError, match = 'courts'):
        idb.CodeIdb().apply(cases(docket_number = ['17-1234']), source = table())
    with pytest.raises(KeyError, match = 'docket numbers'):
        idb.CodeIdb().apply(cases(court = ['ca1']), source = table())
    with pytest.raises(KeyError, match = 'docket_num_idb'):
        idb.CodeIdb().apply(
            cases(court = ['ca1'], docket_number = ['17-1234']),
            source = table().drop(columns = ['docket_number', 'docket_num_idb']))


def test_a_csv_file(tmp_path: pathlib.Path) -> None:
    # The CSV file that the fjc-idb command makes keeps the docket numbers
    # that begin with 0, and its dates are read as dates.
    path = tmp_path / 'fjc_idb_appellate_data_2008_2026.csv'
    table().drop(columns = 'docket_number').to_csv(path, index = False)
    read = idb.CodeIdb().read(path)
    assert read['docket_num_idb'].iloc[0] == '0801234'
    assert pd.api.types.is_datetime64_any_dtype(read['judgment_date'])
    result = idb.CodeIdb().apply(
        cases(court = ['ca1'], docket_number = ['08-1234'], date_filed = ['2009-05-01']),
        source = path, columns = 'appeal_type', indicator = 'found')
    assert list(result.data['found']) == [True]
    assert result.history[-1]['source'] == str(path)


def test_years_of_the_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[Any, ...]] = []

    def appeals(start: int, stop: int, folder: Any, **kwargs: Any) -> pd.DataFrame:
        calls.append((start, stop, folder, kwargs))
        return table()

    monkeypatch.setattr(idb, 'appeals', appeals)
    dataset = cases(
        court = ['ca1', 'cadc', 'scotus'],
        docket_number = ['17-1234', '15-7034', '1'],
        date_filed = ['2018-05-01', '2019-05-14', '1950-01-01'])
    result = idb.CodeIdb().apply(dataset, district = 'false', download = 'no')
    # The years begin with the database's first and end with the year after
    # the latest decision.
    assert calls[-1] == (2008, 2020, None, {'district': False, 'download': False})
    assert result.history[-1]['source'] == 'FJC IDB appeals, 2008-2020'
    idb.CodeIdb().apply(
        cases(court = ['ca1'], docket_number = ['17-1234'], date_filed = ['2018-05-01']),
        district = True, folder = '~/idb')
    assert calls[-1][:3] == (2018, 2019, pathlib.Path('~/idb').expanduser())
    assert calls[-1][3]['district'] is True
    with pytest.raises(ValueError, match = 'begin in 2008'):
        idb.CodeIdb().apply(
            cases(court = ['ca1'], docket_number = ['05-1234'], date_filed = ['2005-06-01']))


def test_download(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    calls: list[tuple[Any, ...]] = []

    def download(kind: str, start: int, stop: int, root: pathlib.Path, **kwargs: Any) -> list[pathlib.Path]:
        calls.append((kind, start, stop, root, kwargs))
        return [root / f'{kind}{start}']

    monkeypatch.setattr(fjc_idb, 'download', download)
    appellate = tmp_path / 'appellate'
    appellate.mkdir()
    (appellate / 'ap17.sas7bdat').touch()
    (appellate / 'ap18_0.sas7bdat').touch()
    # Only the years that are missing are asked for.
    assert idb.download(2017, 2020, tmp_path) == [tmp_path / 'appellate2019']
    assert calls == [('appellate', 2019, 2020, tmp_path, {'overwrite': False})]
    calls.clear()
    idb.download(2017, 2018, tmp_path)
    assert calls == []
    idb.download(2017, 2018, tmp_path, district = True, overwrite = True)
    assert [call[:3] for call in calls] == [
        ('appellate', 2017, 2018), ('civil', 2017, 2018), ('criminal', 2017, 2018)]
    # The years begin with the database's first.
    calls.clear()
    idb.download(2000, 2008, tmp_path)
    assert calls[0][:3] == ('appellate', 2008, 2008)
    with pytest.raises(ValueError, match = 'begin in 2008'):
        idb.download(2000, 2005, tmp_path)


def test_default_folder(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    calls: list[pathlib.Path] = []
    monkeypatch.setenv('COURTPY_DATA_DIR', str(tmp_path))
    monkeypatch.setattr(
        fjc_idb, 'download',
        lambda kind, start, stop, root, **kwargs: calls.append(root) or [])
    idb.download(2019, 2019)
    assert calls == [tmp_path / 'fjc_idb']


def test_appeals(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    calls: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        fjc_idb.main, 'import_data',
        lambda *args: calls.append(('import', *args)) or 'imported')
    monkeypatch.setattr(
        fjc_idb.main, 'parse_data',
        lambda data, kind, **kwargs: calls.append(('parse', data, kind, kwargs)) or table())
    monkeypatch.setattr(
        fjc_idb, 'merge_district',
        lambda *args, **kwargs: calls.append(('merge', *args, kwargs)) or table())
    monkeypatch.setattr(
        fjc_idb, 'download',
        lambda *args, **kwargs: calls.append(('download', *args[:3])) or [])
    assert idb.appeals(2017, 2018, tmp_path, download = False).equals(table())
    assert calls == [
        ('import', 'appellate', 2017, 2018, tmp_path / 'appellate'),
        ('parse', 'imported', 'appellate', {'fix_city_spellings': True})]
    calls.clear()
    idb.appeals(2017, 2018, tmp_path, district = True)
    assert calls == [
        ('download', 'appellate', 2017, 2018),
        ('download', 'civil', 2017, 2018),
        ('download', 'criminal', 2017, 2018),
        ('merge', 2017, 2018, tmp_path, {'fix_city_spellings': True, 'export': False})]


def test_code_idb_in_a_wrangler(tmp_path: pathlib.Path) -> None:
    # A file of the database that the settings name is found as the clerk
    # finds files: in the input folder.
    folder = tmp_path / 'data' / 'court_listener'
    save(folder, 1, 'ca2', '18-3672-cv, 18-3701-cv', '2019-06-03')
    save(folder, 2, 'cadc', '15-7034', '2020-08-18')
    save(folder, 3, 'cafc', '2019-1234', '2019-06-03')
    table().to_csv(tmp_path / 'data' / 'appeals.csv', index = False)
    settings = tmp_path / 'study.ini'
    settings.write_text(
        '[files]\n'
        'input_folder = data\n'
        '[court_project]\n'
        'court_workers = wrangler\n'
        '[wrangler]\n'
        'techniques = load_court_listener, code_idb\n'
        '[code_idb_parameters]\n'
        'source = appeals.csv\n'
        'columns = appeal_type, judgment_date\n'
        'indicator = in_idb\n', encoding = 'utf-8')
    result = courtpy.Project.create(settings, clerk = tmp_path).result
    assert list(result.data['in_idb']) == [True, True, False]
    assert list(result.data['idb_appeal_type'].iloc[:2]) == ['Private civil', 'U.S. civil']
    assert str(result.data['idb_judgment_date'].iloc[1].date()) == '2020-08-18'
    assert result.history[-1]['source'] == 'appeals.csv'
