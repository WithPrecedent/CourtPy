"""Tests the jcs module."""

from __future__ import annotations

import io
import pathlib
import zipfile
from typing import Any

import pandas as pd
import pytest
from conftest import Site
from test_judges import APPEALS, DEMOGRAPHICS_COLUMNS, SERVICE_COLUMNS, Session, tables

import courtpy
from courtpy import jcs, judges, politics

PAGE = 'https://epstein.wustl.edu/jcs'
ARCHIVE = 'https://epstein.wustl.edu/s/JCS2024.zip'
SEVENTH = 'U.S. Court of Appeals for the Seventh Circuit'
# Two invented judges of one court with the same first and last names, told
# apart by their suffixes.
KERNERS = {
    'service': [
        (113, 1, 'Kerner, Otto, Sr.', APPEALS, SEVENTH, 'Franklin D. Roosevelt', 'Democratic', '', '', 'Voice', '', '1938-12-01', '', '1952-12-13'),
        (114, 1, 'Kerner, Otto, Jr.', APPEALS, SEVENTH, 'Lyndon B. Johnson', 'Democratic', '', '', 'Voice', '', '1968-04-22', '', '1974-07-22')],
    'demographics': [
        (113, 'Kerner', 'Otto', '', 'Sr.', '1884', 'Male', 'White'),
        (114, 'Kerner', 'Otto', '', 'Jr.', '1908', 'Male', 'White')]}
JUDGES = pd.DataFrame({
    '': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    'name': [
        'Lynch, Mary Ann', 'Smith, Milton D., Jr.', 'Smith, N. Roy',
        'Fiélds, Ray Lanier, III', 'Fields, Ray Lanier, III', 'Woods, Dana',
        'Kerner, Otto, Jr.', 'Kerner, Otto', 'Lynhc, Mary', 'Nobody, Ann'],
    'circuit': [1, 9, 9, 5, 11, 1, 7, 7, 1, 1],
    'jcs': [-0.4, 0.5, 0.6, -0.1, -0.1, 0.2, -0.3, -0.3, -0.4, 0.0]})
JUSTICES = pd.DataFrame({
    '': [1, 2, 3],
    'term': [2020, 2021, 2022],
    'justiceName': ['ACBarrett', 'ACBarrett', 'ACBarrett'],
    'jcs': [0.43, 0.44, 0.39]})
MEDIANS = pd.DataFrame({
    '': [1, 2, 3],
    'year': [1924, 1924, 1925],
    'term': [None, None, None],
    'congress': [68, 68, 69],
    'president': [0.4, 0.44, 0.4],
    'house_median': [0.2, 0.2, 0.26],
    'senate_median': [0.22, 0.22, 0.22],
    'sc_median': [None, None, 0.1],
    'circuit_median3': [0.43, 0.43, 0.43],
    'circuit_sd3': [None, None, 0.1]})


def archive(skipped: str | None = None) -> bytes:
    """Returns an archive of scores in the layout of the 2024 release."""
    contents = {
        'coa_judges_2024': JUDGES,
        'jcs medians': MEDIANS,
        'jcs_supreme_court_2024_long': JUSTICES,
        'jcs_supreme_court_2024_wide': pd.DataFrame({'justiceName': ['ACBarrett']})}
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as packed:
        for name, table in contents.items():
            if name == skipped:
                continue
            csv = table.to_csv(index = False).encode('utf-8')
            packed.writestr(f'jcs 2024/csv/{name}.csv', csv)
            packed.writestr(f'jcs 2024/stata/{name}.dta', b'not a csv')
            # The hidden files that a Mac adds have the same names.
            packed.writestr(f'__MACOSX/jcs 2024/csv/._{name}.csv', b'\x00\x05')
    return buffer.getvalue()


@pytest.fixture
def site() -> Site:
    """Returns a session that serves the page and archive of scores."""
    return Site({
        PAGE: (
            '<html><body><a href="/s/JCS.pdf">here</a> for the article. '
            '<a href="/s/JCS2024.zip">here</a> for the data.</body></html>'),
        ARCHIVE: archive()})


@pytest.fixture
def roster() -> judges.Roster:
    """Returns a roster of the invented judges and the Kerners."""
    files = tables()
    files['service'] = pd.concat([
        files['service'], pd.DataFrame(KERNERS['service'], columns = SERVICE_COLUMNS)])
    files['demographics'] = pd.concat([
        files['demographics'], pd.DataFrame(KERNERS['demographics'], columns = DEMOGRAPHICS_COLUMNS)])
    return judges.Roster(judges.build_roster(**files))


def test_download(site: Site, tmp_path: pathlib.Path) -> None:
    paths = jcs.download(tmp_path / 'jcs', session = site)
    assert list(paths) == ['judges', 'justices', 'medians']
    assert [p.name for p in paths.values()] == ['judges.csv', 'justices.csv', 'medians.csv']
    assert (tmp_path / 'jcs' / 'JCS2024.zip').is_file()
    # The archive is found by its link on the page.
    assert site.urls == [PAGE, ARCHIVE]
    assert pd.read_csv(paths['justices'])['justiceName'].tolist() == ['ACBarrett'] * 3
    # Files that are there are not downloaded again unless asked.
    jcs.download(tmp_path / 'jcs', session = site)
    assert len(site.urls) == 2
    jcs.download(tmp_path / 'jcs', kinds = 'judges', overwrite = True, session = site)
    assert site.urls[2:] == [PAGE, ARCHIVE]
    assert list(jcs.download(tmp_path / 'jcs', kinds = ['medians'], session = site)) == ['medians']
    # The default folder is outside of any project.
    assert jcs.download(session = site)['judges'].parent == courtpy.utilities.data_folder() / 'jcs'


def test_download_fails_clearly(site: Site, tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match = 'kinds'):
        jcs.download(tmp_path, kinds = 'presidents', session = site)
    site.pages[ARCHIVE] = archive(skipped = 'jcs medians')
    with pytest.raises(ValueError, match = 'no CSV file of medians'):
        jcs.download(tmp_path, session = site)
    site.pages[PAGE] = '<a href="/s/JCS.pdf">here</a>'
    with pytest.raises(ValueError, match = 'no link to an archive'):
        jcs.download(tmp_path / 'other', session = site)
    with pytest.raises(RuntimeError, match = '503'):
        jcs.download(tmp_path / 'third', session = Site(site.pages, status = 503))


def test_judges(roster: judges.Roster, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    monkeypatch.setitem(jcs._CORRECTIONS, ('Lynhc, Mary', 1), ('Lynch, Mary', 1))
    path = tmp_path / 'judges.csv'
    JUDGES.to_csv(path, index = False)
    table = jcs.judges(path, roster)
    assert list(table.columns) == ['name', 'circuit', 'nid', 'jcs']
    assert table['nid'].astype(object).where(table['nid'].notna(), None).tolist() == [
        101, 104, 105,
        # Accents and suffixes do not keep a name from fitting, and a judge
        # of two courts of appeals has a row for each.
        108, 108,
        # A district judge is not a judge of the court of appeals.
        None,
        # A suffix tells apart judges with the same name, who are otherwise
        # not told apart.
        114, None,
        # A name the file has wrong is corrected.
        101,
        None]
    assert table['jcs'].tolist() == JUDGES['jcs'].tolist()
    with pytest.raises(KeyError, match = 'circuit'):
        jcs.judges(JUDGES.drop(columns = 'circuit'), roster)


def test_from_fjc(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    original = judges.download
    monkeypatch.setattr(
        judges, 'download',
        lambda folder = None, *, overwrite = False, session = None: original(
            folder, overwrite = overwrite, session = Session()))
    calls: list[bool] = []

    def fake(folder: Any = None, *, kinds: Any = None, overwrite: bool = False, session: Any = None) -> dict[str, pathlib.Path]:
        calls.append(overwrite)
        path = tmp_path / 'judges.csv'
        JUDGES.to_csv(path, index = False)
        return {'judges': path}

    monkeypatch.setattr(jcs, 'download', fake)
    assert 'jcs' not in judges.Roster.from_fjc(tmp_path).data.columns
    roster = judges.Roster.from_fjc(tmp_path, jcs = True, update = True)
    assert calls[0] is True
    scores = roster.data.drop_duplicates('nid').set_index('nid')['jcs']
    assert scores[101] == -0.4 and scores[105] == 0.6 and scores[108] == -0.1
    assert pd.isna(scores[106])
    assert 'jcs' in roster.attributes


def test_justices(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'justices.csv'
    JUSTICES.to_csv(path, index = False)
    table = jcs.justices(path)
    assert list(table.columns) == ['term', 'justiceName', 'jcs']
    assert table['jcs'].tolist() == [0.43, 0.44, 0.39]
    with pytest.raises(KeyError, match = 'justiceName'):
        jcs.justices(JUSTICES.drop(columns = 'justiceName'))


def test_medians(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'medians.csv'
    MEDIANS.to_csv(path, index = False)
    table = jcs.medians(path).set_index('year')
    assert list(table.columns) == [
        'jcs_president', 'jcs_house_median', 'jcs_senate_median',
        'jcs_supreme_court_median', 'jcs_circuit3_median', 'jcs_circuit3_sd']
    # A year with two rows has their means.
    assert table.loc[1924, 'jcs_president'] == pytest.approx(0.42)
    assert pd.isna(table.loc[1924, 'jcs_supreme_court_median'])
    assert table.loc[1925, 'jcs_circuit3_sd'] == 0.1
    with pytest.raises(KeyError, match = 'year'):
        jcs.medians(MEDIANS.drop(columns = 'year'))


def test_build_table_adds_medians() -> None:
    table = politics.build_table(False, False, common_space = MEDIANS).set_index('year')
    assert 'jcs_house_median' in table.columns and 'president_party' in table.columns
    assert table.loc[1933, 'president_party'] == -1
    assert table.loc[1925, 'jcs_house_median'] == 0.26
