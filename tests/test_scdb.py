"""Tests the scdb module."""

from __future__ import annotations

import io
import pathlib
import zipfile

import amos
import pandas as pd
import pytest
from conftest import Site, make_cluster, make_opinion, write_case

import courtpy
from courtpy import scdb

PAGE = 'https://scdb.la.psu.edu/data/'
RELEASE = 'https://scdb.la.psu.edu/data/2026-release-01/'
CASES = 'https://scdb.la.psu.edu/?jet_download=aaa'
VOTES = 'https://scdb.la.psu.edu/?jet_download=ccc'
# The database's page lists its releases, the latest first, and the page of
# a release links to its files with the same words for both kinds of data.
RELEASES = (
    '<h2>Latest Releases</h2>'
    f'<a href="{RELEASE}">2026 Release 01</a>'
    '<a href="https://scdb.la.psu.edu/data/scdb-legacy-07/">SCDB Legacy 07</a>'
    '<a href="https://scdb.la.psu.edu/data/2025-release-01/">2025 Release 01</a>')
FILES = (
    '<h3>Case Centered Data</h3>'
    f'<a href="{CASES}"><span>Download CSV Organized by Supreme Court Citation</span>'
    '<span>File size: 698 KB</span></a>'
    '<a href="https://scdb.la.psu.edu/?jet_download=bbb">Download DTA Organized by Supreme Court Citation</a>'
    '<a href="https://scdb.la.psu.edu/?jet_download=ddd">Download CSV Organized by Docket</a>'
    '<h3>Justice Centered Data</h3>'
    f'<a href="{VOTES}">Download CSV Organized by Supreme Court Citation</a>')
# Invented cases, in the layout of the database's case centered file. The
# last two are short orders on one page of each reporter.
ROWS = [
    ('1953-069', '347 U.S. 483', '74 S. Ct. 686', '98 L. Ed. 873', 1953, 'BROWN v. BOARD', 2, 2, 9, 0),
    ('2007-059', '554 U.S. 570', '128 S. Ct. 2783', '171 L. Ed. 2d 637', 2007, 'D.C. v. HELLER', 2, 1, 5, 4),
    ('2025-068', None, '146 S. Ct. 1873', '225 L. Ed. 2d 342', 2025, 'MCCARTHY v. HERNANDEZ', 1, 1, 6, 3),
    ('1960-101', '365 U.S. 1', '81 S. Ct. 1', '5 L. Ed. 2d 1', 1960, 'ROE v. ONE', 9, 2, 9, 0),
    ('1960-102', '365 U.S. 1', '81 S. Ct. 1', '5 L. Ed. 2d 1', 1960, 'ROE v. TWO', 9, 1, 9, 0)]
COLUMNS = [
    'caseId', 'usCite', 'sctCite', 'ledCite', 'term', 'caseName', 'issueArea',
    'decisionDirection', 'majVotes', 'minVotes']


def table() -> pd.DataFrame:
    """Returns the invented cases as the database's table of them."""
    return pd.DataFrame(ROWS, columns = COLUMNS)


def justices() -> pd.DataFrame:
    """Returns two invented votes in each of the invented cases."""
    votes = table().loc[table().index.repeat(2)].reset_index(drop = True)
    return votes.assign(
        justice = [111, 117] * len(ROWS),
        justiceName = ['JGRoberts', 'ACBarrett'] * len(ROWS),
        vote = [1, 2] * len(ROWS))


def archive(data: pd.DataFrame, name: str, encoding: str = 'utf-8') -> bytes:
    """Returns a zip archive of a table, as the database's files are."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as file:
        file.writestr(name, data.to_csv(index = False).encode(encoding))
    return buffer.getvalue()


@pytest.fixture
def site() -> Site:
    """Returns a session that serves the database's pages and files."""
    return Site({
        PAGE: RELEASES,
        RELEASE: FILES,
        CASES: archive(table(), 'SCDB_2026_01_caseCentered_Citation.csv'),
        VOTES: archive(justices(), 'SCDB_2026_01_justiceCentered_Citation.csv')})


def test_technique_is_in_the_library() -> None:
    assert amos.library.classify('code_scdb') == 'merger'


def test_download(site: Site, tmp_path: pathlib.Path) -> None:
    paths = scdb.download(tmp_path / 'files', session = site)
    assert list(paths) == ['cases', 'votes']
    # A file is named for its release.
    assert [p.name for p in paths.values()] == [
        'SCDB_2026_01_caseCentered_Citation.csv.zip',
        'SCDB_2026_01_justiceCentered_Citation.csv.zip']
    # The latest release is found once, and each file by its link.
    assert site.urls == [PAGE, RELEASE, CASES, VOTES]
    # Files that are there are not looked for again unless asked.
    assert scdb.download(tmp_path / 'files', session = site) == paths
    assert len(site.urls) == 4
    scdb.download(tmp_path / 'files', kinds = 'votes', overwrite = True, session = site)
    assert site.urls[4:] == [PAGE, RELEASE, VOTES]
    assert list(scdb.download(tmp_path / 'one', kinds = ['cases'], session = site)) == ['cases']
    # The default folder is outside of any project.
    assert scdb.download(session = site)['cases'].parent == courtpy.utilities.data_folder() / 'scdb'


def test_download_uses_the_latest_release(site: Site, tmp_path: pathlib.Path) -> None:
    earlier = tmp_path / 'SCDB_2025_01_caseCentered_Citation.csv.zip'
    earlier.write_bytes(archive(table().iloc[:1], 'SCDB_2025_01_caseCentered_Citation.csv'))
    assert scdb.download(tmp_path, kinds = 'cases', session = site)['cases'] == earlier
    assert site.urls == []
    latest = scdb.download(tmp_path, kinds = 'cases', overwrite = True, session = site)['cases']
    assert latest.name == 'SCDB_2026_01_caseCentered_Citation.csv.zip'
    assert earlier.is_file()
    assert scdb.download(tmp_path, kinds = 'cases', session = site)['cases'] == latest


def test_download_fails_clearly(site: Site, tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match = 'kinds'):
        scdb.download(tmp_path, kinds = 'dockets', session = site)
    # A page whose first link is not to the case centered data.
    site.pages[CASES] = site.pages[VOTES]
    with pytest.raises(ValueError, match = 'caseCentered_Citation'):
        scdb.download(tmp_path, kinds = 'cases', session = site)
    site.pages[CASES] = b'not a zip archive'
    with pytest.raises(ValueError, match = 'caseCentered_Citation'):
        scdb.download(tmp_path, kinds = 'cases', session = site)
    assert list(tmp_path.iterdir()) == []
    site.pages[RELEASE] = '<a href="x">Download CSV Organized by Docket</a>'
    with pytest.raises(ValueError, match = 'does not link'):
        scdb.download(tmp_path, session = site)
    site.pages[PAGE] = '<a href="https://scdb.la.psu.edu/data/scdb-legacy-07/">SCDB Legacy 07</a>'
    with pytest.raises(ValueError, match = 'no release'):
        scdb.download(tmp_path, session = site)
    with pytest.raises(RuntimeError, match = '503'):
        scdb.download(tmp_path, session = Site(site.pages, status = 503))


def test_cases_and_votes(site: Site, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = scdb.download(tmp_path, session = site)
    cases = scdb.cases(paths['cases'])
    assert list(cases.columns) == COLUMNS
    assert list(cases['caseId']) == [row[0] for row in ROWS]
    assert cases.loc[1, 'decisionDirection'] == 1
    votes = scdb.votes(paths['votes'])
    assert len(votes) == 2 * len(ROWS)
    assert {'justice', 'justiceName', 'vote'} <= set(votes.columns)
    assert scdb.cases(cases) is not cases and scdb.cases(cases).equals(cases)
    # Earlier releases were not in UTF-8.
    named = table().assign(caseName = 'PEÑA v. BOARD')
    old = tmp_path / 'old.zip'
    old.write_bytes(archive(named, 'old.csv', encoding = 'cp1252'))
    assert scdb.cases(old)['caseName'].iloc[0] == 'PEÑA v. BOARD'
    # With no file, the latest release is used, and downloaded once.
    monkeypatch.setattr(courtpy.utilities.requests, 'Session', lambda: site)
    asked = len(site.urls)
    assert scdb.cases().equals(cases)
    assert site.urls[asked:] == [PAGE, RELEASE, CASES]
    assert scdb.cases().equals(cases)
    assert len(site.urls) == asked + 3


def test_code_scdb() -> None:
    data = pd.DataFrame({
        'scdb_id': ['2007-059', None, None, '1953-069', '9999-001', None],
        'citation': [
            'ignored', '74 S.Ct. 686; 98 L.Ed. 873', ['146 S. Ct. 1873'], None,
            '554 U.S. 570', '365 U.S. 1'],
        'year': [2008, 1954, 2026, 1954, 2008, 1961]}, index = list('abcdef'))
    dataset = scdb.CodeScdb().apply(
        amos.Dataset(data.copy()), source = table(), indicator = 'in_scdb',
        columns = 'caseId, issueArea, decisionDirection')
    result = dataset.data
    assert list(result.index) == list(data.index)
    assert list(result.columns) == [
        'scdb_id', 'citation', 'year', 'scdb_caseId', 'scdb_issueArea',
        'scdb_decisionDirection', 'in_scdb']
    # A case is matched by its id, or else by a citation. An id that is not
    # in the database leaves the citation to match by, and a citation that
    # two cases share matches neither.
    assert list(result['scdb_caseId'].iloc[:5]) == [
        '2007-059', '1953-069', '2025-068', '1953-069', '2007-059']
    assert list(result['in_scdb']) == [True, True, True, True, True, False]
    assert pd.isna(result.loc['f', 'scdb_issueArea'])
    assert result.loc['a', 'scdb_decisionDirection'] == 1
    record = dataset.history[0]
    assert (record['technique'], record['rows'], record['matched']) == ('code_scdb', 6, 5)
    # By default, every column of the database is added.
    everything = scdb.CodeScdb().apply(amos.Dataset(data.copy()), source = table()).data
    assert [c for c in everything.columns if c.startswith('scdb_') and c != 'scdb_id'] == [
        f'scdb_{c}' for c in COLUMNS]


def test_code_scdb_keys() -> None:
    # Either the ids or the citations are enough, under any names.
    ids = pd.DataFrame({'spaeth': ['1953-069', None]})
    result = scdb.CodeScdb().apply(
        amos.Dataset(ids), source = table(), on = 'spaeth', columns = 'term').data
    assert result['scdb_term'].tolist()[0] == 1953 and pd.isna(result['scdb_term'].iloc[1])
    cites = pd.DataFrame({'cites': [['1 F.2d 3', '347 U. S. 483'], [], None]})
    result = scdb.CodeScdb().apply(
        amos.Dataset(cites), source = table(), citations = 'cites', columns = 'term',
        prefix = None).data
    assert result['term'].tolist()[0] == 1953 and result['term'].iloc[1:].isna().all()
    with pytest.raises(KeyError, match = 'scdb_id'):
        scdb.CodeScdb().apply(amos.Dataset(pd.DataFrame({'year': [2000]})), source = table())
    with pytest.raises(KeyError, match = 'caseId'):
        scdb.CodeScdb().apply(amos.Dataset(ids.rename(columns = {'spaeth': 'scdb_id'})), source = table()[['term']])
    with pytest.raises(KeyError, match = 'no_such'):
        scdb.CodeScdb().apply(amos.Dataset(ids), source = table(), on = 'spaeth', columns = 'no_such')


def test_code_scdb_with_court_listener(
    site: Site,
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch) -> None:
    # CourtListener records the database's id of a case of the Supreme Court,
    # which becomes a column of the parsed cases.
    folder = tmp_path / 'data' / 'court_listener'
    write_case(
        folder,
        make_cluster(
            7, scdb_id = '2007-059', case_name = 'District of Columbia v. Heller',
            citations = [{'volume': 554, 'reporter': 'U.S.', 'page': '570', 'type': 1}]),
        [make_opinion(71, 7)], court = 'scotus')
    write_case(
        folder,
        make_cluster(
            8, scdb_id = '', case_name = 'McCarthy v. Hernandez',
            citations = [{'volume': 146, 'reporter': 'S. Ct.', 'page': '1873', 'type': 1}]),
        [make_opinion(81, 8)], court = 'scotus')
    parsed = courtpy.parse(folder)
    assert list(parsed['scdb_id'].fillna('')) == ['2007-059', '']
    # As one of a loader's coders, it uses the latest release, which it
    # downloads, and the history records the release.
    monkeypatch.setattr(courtpy.utilities.requests, 'Session', lambda: site)
    dataset = courtpy.loaders.LoadCourtListener().apply(
        source = folder, coders = 'code_outcome, code_scdb', label = 'outcome_reversal')
    assert list(dataset.data['scdb_caseName']) == ['D.C. v. HELLER', 'MCCARTHY v. HERNANDEZ']
    assert list(dataset.data['scdb_majVotes']) == [5, 6]
    record = dataset.history[-1]
    assert record['source'].endswith('SCDB_2026_01_caseCentered_Citation.csv.zip')
    assert (record['rows'], record['matched']) == (2, 2)


def test_other_cases_have_no_id(court_listener_folder: pathlib.Path) -> None:
    assert 'scdb_id' not in courtpy.parse(court_listener_folder).columns


def test_code_scdb_in_a_wrangler(
    court_listener_folder: pathlib.Path,
    tmp_path: pathlib.Path) -> None:
    # A file of the database that the settings name is found as the clerk
    # finds files: in the input folder.
    folder = tmp_path / 'data' / 'court_listener'
    folder.parent.mkdir()
    court_listener_folder.rename(folder)
    cited = table().assign(usCite = ['950 F.3d 12', *table()['usCite'].iloc[1:]])
    (tmp_path / 'data' / 'mine.zip').write_bytes(archive(cited, 'mine.csv'))
    settings = tmp_path / 'study.ini'
    settings.write_text(
        '[general]\n'
        'label = outcome_reversal\n'
        '[files]\n'
        'input_folder = data\n'
        '[court_project]\n'
        'court_workers = wrangler\n'
        '[wrangler]\n'
        'techniques = load_court_listener, code_scdb\n'
        '[code_scdb_parameters]\n'
        'source = mine.zip\n'
        'columns = issueArea, majVotes\n'
        'indicator = found\n', encoding = 'utf-8')
    result = courtpy.Project.create(settings, clerk = tmp_path).result
    # All three cases have the citation that was given to the table's first
    # row.
    assert list(result.data['found']) == [True, True, True]
    assert list(result.data['scdb_issueArea']) == [2, 2, 2]
    assert result.history[-1]['source'] == 'mine.zip'
