"""Tests the judges module."""

from __future__ import annotations

import pathlib
import random
from typing import Any

import amos
import chrisjen
import pandas as pd
import pytest
from conftest import make_cluster, make_opinion, write_case

import courtpy
from courtpy import judges, loaders

EXAMPLES = pathlib.Path(__file__).parents[1] / 'examples'
FIRST = 'U.S. Court of Appeals for the First Circuit'
NINTH = 'U.S. Court of Appeals for the Ninth Circuit'
APPEALS = 'U.S. Court of Appeals'
DISTRICT = 'U.S. District Court'
# Invented judges, in the layout of the Federal Judicial Center's file of
# federal judicial service: nid, sequence, name, court type, court,
# president, party, ABA rating, recess appointment, vote type, ayes/nays,
# commission, senior status, and termination.
SERVICE = [
    (101, 1, 'Lynch, Mary Ann', APPEALS, FIRST, 'William J. Clinton', 'Democratic', 'Well Qualified', '', 'Voice', '', '1995-03-17', '', ''),
    (102, 1, 'Howard, John Q.', APPEALS, FIRST, 'George W. Bush', 'Republican', 'Qualified', '', 'Roll Call', ' 99/1', '2002-04-23', '', ''),
    (103, 1, 'Thompson, Rita Mae', APPEALS, FIRST, 'Barack Obama', 'Democratic', 'Exceptionally Well Qualified', '', 'Roll Call', ' 98/0', '2010-03-17', '', ''),
    (104, 1, 'Smith, Milton Dale, Jr.', APPEALS, NINTH, 'George W. Bush', 'Republican', 'Well Qualified', '', 'Voice', '', '2006-05-18', '', ''),
    (105, 1, 'Smith, N[orman] Roy', APPEALS, NINTH, 'George W. Bush', 'Republican', '', '', 'Roll Call', ' 94/0', '2007-03-19', '', ''),
    (106, 1, 'Woods, Dana Paul', DISTRICT, 'U.S. District Court for the District of Massachusetts', 'Ronald Reagan', 'Republican', 'Not Qualified', '', 'Voice', '', '1986-06-16', '2015-06-01', ''),
    (107, 2, 'Stone, Nora H.', APPEALS, FIRST, 'George H.W. Bush', 'Republican', 'Well Qualified', '', 'Voice', '', '1992-04-30', '2001-04-16', ''),
    (107, 1, 'Stone, Nora H.', DISTRICT, 'U.S. District Court for the District of New Hampshire', 'George H.W. Bush', 'Republican', 'Qualified', '', 'Voice', '', '1990-04-27', '', '1992-04-30'),
    (108, 1, 'Fields, R[ay] Lanier III', APPEALS, 'U.S. Court of Appeals for the Fifth Circuit', 'Jimmy Carter', 'Democratic', 'Well Qualified', '', 'Roll Call', ' 60/30', '1979-07-13', '', '1981-10-01'),
    (108, 2, 'Fields, R[ay] Lanier III', APPEALS, 'U.S. Court of Appeals for the Eleventh Circuit', 'None (reassignment)', 'None (reassignment)', '', '', '', '', '1981-10-01', '', ''),
    (109, 1, 'Rake, Jed Saul', DISTRICT, 'U.S. District Court for the Southern District of New York', 'William J. Clinton', 'Democratic', 'Well Qualified', '', 'Voice', '', '1996-03-01', '', ''),
    (110, 1, 'Poser, Richard Allen', APPEALS, 'U.S. Court of Appeals for the Seventh Circuit', 'Ronald Reagan', 'Republican', 'Qualified', '', 'Voice', '', '1981-12-01', '', '2017-09-02'),
    (111, 1, 'Gray, Roger L.', APPEALS, 'U.S. Court of Appeals for the Fourth Circuit', 'William J. Clinton', 'Democratic', '', '2000-12-27', 'Unknown', '', '', '', '2001-07-20'),
    (112, 1, 'Marsh, John', 'Supreme Court', 'Supreme Court of the United States', 'John Adams', 'Federalist', '', '', 'Voice', '', '1801-01-31', '', '1835-07-06')]
SERVICE_COLUMNS = [
    'nid', 'Sequence', 'Judge Name', 'Court Type', 'Court Name',
    'Appointing President', 'Party of Appointing President', 'ABA Rating',
    'Recess Appointment Date', 'Senate Vote Type', 'Ayes/Nays',
    'Commission Date', 'Senior Status Date', 'Termination Date']
DEMOGRAPHICS = [
    (101, 'Lynch', 'Mary', 'Ann', '', '1946', 'Female', 'White'),
    (102, 'Howard', 'John', 'Q.', '', '1955', 'Male', 'White'),
    (103, 'Thompson', 'Rita', 'Mae', '', '1951', 'Female', 'African American'),
    (104, 'Smith', 'Milton', 'Dale', 'Jr.', '1942', 'Male', 'White'),
    (105, 'Smith', 'N[orman]', 'Roy', '', '1949', 'Male', 'Hispanic/White'),
    (106, 'Woods', 'Dana', 'Paul', '', '1947', 'Male', 'White'),
    (107, 'Stone', 'Nora', 'H.', '', '1931', 'Female', 'White'),
    (108, 'Fields', 'R[ay]', 'Lanier', 'III', '1936', 'Male', 'White'),
    (109, 'Rake', 'Jed', 'Saul', '', '1943', 'Male', 'White'),
    (110, 'Poser', 'Richard', 'Allen', '', '1939', 'Male', 'White'),
    (111, 'Gray', 'Roger', 'L.', '', '1953', 'Male', 'African American'),
    (112, 'Marsh', 'John', ' ', '', '1755', '', '')]
DEMOGRAPHICS_COLUMNS = [
    'nid', 'Last Name', 'First Name', 'Middle Name', 'Suffix', 'Birth Year',
    'Gender', 'Race or Ethnicity']
CAREER = [
    (101, 'Law clerk, Hon. A. Judge, U.S. District Court, District of Rhode Island, 1971-1973'),
    (101, 'Assistant attorney general, State of Massachusetts, 1973-1974'),
    (102, 'Assistant U.S. attorney, District of New Hampshire, 1981-1987'),
    (103, 'Assistant public defender, Rhode Island, 1976-1979'),
    (104, 'Law clerk, Hon. B. Justice, Supreme Court of the United States, 1970-1971'),
    (104, 'Professor, University of the West School of Law, 1980-1990'),
    (105, 'Reporter of decisions, Supreme Court of the United States, 1990-1991'),
    (105, 'Law clerk, Hon. C. Judge, U.S. District Court, District of Idaho, 1977-1978')]


def tables() -> dict[str, pd.DataFrame]:
    """Returns the three files of invented judges."""
    return {
        'service': pd.DataFrame(SERVICE, columns = SERVICE_COLUMNS),
        'demographics': pd.DataFrame(DEMOGRAPHICS, columns = DEMOGRAPHICS_COLUMNS),
        'career': pd.DataFrame(CAREER, columns = ['nid', 'Professional Career'])}


@pytest.fixture
def table() -> pd.DataFrame:
    """Returns the table of a roster of the invented judges."""
    return judges.build_roster(**tables()).set_index('court', drop = False)


@pytest.fixture
def roster() -> judges.Roster:
    """Returns a roster of the invented judges."""
    return judges.Roster(judges.build_roster(**tables()))


def found(roster: judges.Roster, name: str, court: Any, year: Any) -> tuple[int, str] | None:
    """Returns the id and court of the judge that a name is matched to."""
    row = roster.match(name, court, year)
    if row is None:
        return None
    return int(roster.data['nid'].iloc[row]), str(roster.data['court'].iloc[row])


def test_techniques_are_in_the_library() -> None:
    # Each is the genre of `amos` that says what it does to the cases.
    assert amos.library.classify('merge_judges') == 'merger'
    assert amos.library.classify('code_judges') == 'munger'
    assert amos.library.classify('judge_votes') == 'shaper'


def test_build_roster(table: pd.DataFrame) -> None:
    assert len(table) == len(SERVICE)
    # Rows are sorted by judge and then by the order of the judge's service.
    assert list(table.loc[table['nid'] == 107, 'court_type']) == [DISTRICT, APPEALS]
    lynch = table.loc[FIRST].iloc[0]
    assert lynch['judge'] == 'Lynch, Mary Ann'
    assert (lynch['last_name'], lynch['first_name'], lynch['middle_name']) == ('Lynch', 'Mary', 'Ann')
    assert (lynch['court_num'], lynch['circuit_num']) == (1, 1)
    assert (lynch['start_year'], lynch['birth_year']) == (1995, 1946)
    assert pd.isna(lynch['end_year']) and pd.isna(lynch['senior_year'])
    assert (lynch['party'], lynch['aba_rating'], lynch['senate_vote']) == (-1, 3, 1.0)
    assert lynch['woman'] and not lynch['minority'] and not lynch['recess']
    by_nid = table.drop_duplicates('nid', keep = 'last').set_index('nid')
    assert list(by_nid.loc[[102, 103, 106], 'aba_rating']) == [2, 4, 1]
    assert pd.isna(by_nid.loc[105, 'aba_rating'])
    assert by_nid.loc[102, 'senate_vote'] == pytest.approx(0.99)
    assert by_nid.loc[103, 'minority'] and by_nid.loc[105, 'minority']
    assert by_nid.loc[110, 'end_year'] == 2017
    assert by_nid.loc[106, 'senior_year'] == 2015


def test_build_roster_courts(table: pd.DataFrame) -> None:
    numbers = table.drop_duplicates('court').set_index('court')
    assert numbers.loc[NINTH, 'court_num'] == 9
    assert numbers.loc['U.S. Court of Appeals for the Eleventh Circuit', 'court_num'] == 11
    assert numbers.loc['Supreme Court of the United States', 'court_num'] == 99
    # A district court has no number of its own, but it has a circuit.
    massachusetts = numbers.loc['U.S. District Court for the District of Massachusetts']
    assert pd.isna(massachusetts['court_num']) and massachusetts['circuit_num'] == 1
    assert numbers.loc['U.S. District Court for the Southern District of New York', 'circuit_num'] == 2
    assert judges._circuit('U.S. District Court for the District of Columbia (Supreme Court)') == 12
    assert judges._circuit('U.S. District Court for the Edenton & Wilmington Districts of North Carolina') == 4
    assert judges._circuit('U.S. District Court for the District of Orleans') is None


def test_build_roster_appointments(table: pd.DataFrame) -> None:
    by_court = table.set_index('court')
    fifth = by_court.loc['U.S. Court of Appeals for the Fifth Circuit']
    eleventh = by_court.loc['U.S. Court of Appeals for the Eleventh Circuit']
    # A reassigned judge keeps the appointment of the judge's earlier service.
    assert (fifth['start_year'], fifth['end_year'], eleventh['start_year']) == (1979, 1981, 1981)
    for column in ('president', 'party', 'aba_rating', 'senate_vote'):
        assert eleventh[column] == fifth[column]
    assert eleventh['president'] == 'Jimmy Carter'
    assert eleventh['senate_vote'] == pytest.approx(60 / 90)
    # A recess appointment starts a judge's service, and a party other than
    # the two of today, an unrecorded vote, and an unknown gender are missing.
    fourth = by_court.loc['U.S. Court of Appeals for the Fourth Circuit']
    assert fourth['recess'] and fourth['start_year'] == 2000
    assert pd.isna(fourth['senate_vote'])
    supreme = by_court.loc['Supreme Court of the United States']
    assert pd.isna(supreme['party']) and pd.isna(supreme['woman']) and pd.isna(supreme['minority'])


def test_build_roster_careers(table: pd.DataFrame) -> None:
    careers = table.drop_duplicates('nid').set_index('nid')
    flags = ['prosecutor', 'public_defender', 'law_clerk', 'supreme_court_clerk', 'solicitor_general', 'law_professor']
    assert list(careers.loc[101, flags]) == [True, False, True, False, False, False]
    assert list(careers.loc[102, flags]) == [True, False, False, False, False, False]
    assert list(careers.loc[103, flags]) == [False, True, False, False, False, False]
    assert list(careers.loc[104, flags]) == [False, False, True, True, False, True]
    # A clerkship and the Supreme Court in different positions are not a
    # clerkship at the Supreme Court, and "District" alone is not a prosecutor.
    assert list(careers.loc[105, flags]) == [False, False, True, False, False, False]
    # A judge with no career in the file has no flags.
    assert not careers.loc[106, flags].any()
    assert careers['prosecutor'].dtype == bool


def test_build_roster_options(tmp_path: pathlib.Path) -> None:
    files = tables()
    scores = pd.DataFrame({'nid': [101, 104, 999], 'jcs': [-0.3, 0.4, 0.0]})
    table = judges.build_roster(files['service'], files['demographics'], scores = scores)
    assert 'prosecutor' not in table.columns
    assert table.loc[table['nid'] == 101, 'jcs'].iloc[0] == -0.3
    assert pd.isna(table.loc[table['nid'] == 102, 'jcs'].iloc[0])
    assert 'jcs' in judges.Roster(table).attributes
    # Files are read from paths too, and rules can be the user's own.
    paths = {}
    for kind, frame in files.items():
        paths[kind] = tmp_path / f'{kind}.csv'
        frame.to_csv(paths[kind], index = False)
    rules = tmp_path / 'careers.csv'
    rules.write_text(
        'target,variable,kind,pattern,value,ignorecase,dotall,note\n'
        'career,positions,count,\\d{4}-\\d{4},,FALSE,FALSE,\n', encoding = 'utf-8')
    table = judges.build_roster(**paths, rulebook = rules)
    assert list(table.loc[table['nid'].isin([101, 106]), 'positions']) == [2, 0]
    # A file that is not in UTF-8 is read as Windows-1252.
    files['demographics'].loc[0, 'Last Name'] = 'Peña'
    files['demographics'].to_csv(paths['demographics'], index = False, encoding = 'cp1252')
    table = judges.build_roster(paths['service'], paths['demographics'])
    assert table.loc[table['nid'] == 101, 'last_name'].iloc[0] == 'Peña'


def test_build_roster_missing_columns() -> None:
    files = tables()
    with pytest.raises(KeyError, match = 'federal judicial service'):
        judges.build_roster(files['service'].drop(columns = 'Court Name'), files['demographics'])
    with pytest.raises(KeyError, match = 'demographics'):
        judges.build_roster(files['service'], files['demographics'].drop(columns = 'Gender'))
    with pytest.raises(KeyError, match = 'careers'):
        judges.build_roster(files['service'], files['demographics'], files['career'].drop(columns = 'nid'))
    with pytest.raises(KeyError, match = 'scores'):
        judges.build_roster(files['service'], files['demographics'], scores = pd.DataFrame({'jcs': [1.0]}))
    with pytest.raises(KeyError, match = 'roster'):
        judges.Roster(pd.DataFrame({'judge': ['Lynch, Mary Ann']}))


def test_name_forms() -> None:
    assert judges.name_forms('Sandra', 'Lea', 'Lynch') == [
        'SANDRA LEA LYNCH', 'SANDRA L LYNCH', 'SANDRA LYNCH', 'S LEA LYNCH',
        'S L LYNCH', 'SL LYNCH', 'S LYNCH', 'LEA LYNCH', 'LYNCH']
    assert judges.name_forms('Ronnie', '', 'Abrams') == ['RONNIE ABRAMS', 'R ABRAMS', 'ABRAMS']
    assert 'R LANIER FIELDS' in judges.name_forms('R[ay]', 'Lanier', 'Fields')
    assert "D OSCANNLAIN" in judges.name_forms('Diarmuid', 'F.', "O'Scannlain")
    assert 'F OSCANNLAIN' not in judges.name_forms('Diarmuid', 'F.', "O'Scannlain")
    assert judges.name_forms('John', '', '') == []
    assert judges.clean_name(" N.R.  Smith, Jr. ") == 'NR SMITH JR'


def test_match_prefers_the_court(roster: judges.Roster) -> None:
    assert found(roster, 'LYNCH', 1, 2020) == (101, FIRST)
    assert found(roster, 'Mary A. Lynch', 1, 2020) == (101, FIRST)
    # Two judges of a court share a name, so only their initials tell them apart.
    assert found(roster, 'SMITH', 9, 2010) is None
    assert found(roster, 'M SMITH', 9, 2010) == (104, NINTH)
    assert found(roster, 'N.R. Smith', 9, 2010) == (105, NINTH)
    # One of them was not yet a judge in 2006.
    assert found(roster, 'SMITH', 9, 2006) == (104, NINTH)
    # A district judge of the circuit sits by designation.
    assert found(roster, 'WOODS', 1, 2010) == (106, 'U.S. District Court for the District of Massachusetts')
    # A judge of another circuit visits.
    assert found(roster, 'RAKE', 9, 2015) == (109, 'U.S. District Court for the Southern District of New York')
    assert found(roster, 'NOBODY', 1, 2020) is None


def test_match_uses_the_year(roster: judges.Roster, monkeypatch: pytest.MonkeyPatch) -> None:
    district = 'U.S. District Court for the District of New Hampshire'
    assert found(roster, 'STONE', 1, 1991) == (107, district)
    assert found(roster, 'STONE', 1, 1993) == (107, FIRST)
    assert found(roster, 'STONE', 1, 1989) is None
    # An opinion may name a judge for two years after the judge left.
    seventh = 'U.S. Court of Appeals for the Seventh Circuit'
    assert found(roster, 'POSER', 7, 2019) == (110, seventh)
    assert found(roster, 'POSER', 7, 2020) is None
    assert judges.Roster(roster.data, grace = 0).match('POSER', 7, 2018) is None
    monkeypatch.setattr(courtpy.options, '_JUDGE_GRACE', 5)
    assert judges.Roster(roster.data).match('POSER', 7, 2022) is not None
    assert found(roster, 'R LANIER FIELDS', 5, 1980) == (108, 'U.S. Court of Appeals for the Fifth Circuit')
    assert found(roster, 'FIELDS', 11, 1990) == (108, 'U.S. Court of Appeals for the Eleventh Circuit')
    # Without a year, any year fits, and the latest service is used.
    assert found(roster, 'STONE', 1, None) == (107, FIRST)
    assert found(roster, 'POSER', None, None) == (110, seventh)
    assert found(roster, 'SMITH', None, 2010) is None


def test_match_does_not_guess(roster: judges.Roster) -> None:
    # A bankruptcy appellate panel (999) has no judges in the roster.
    assert found(roster, 'RAKE', 999, 2015) is None
    assert found(roster, 'LYNCH', 999, 2020) is None


def test_aliases(table: pd.DataFrame) -> None:
    table.loc[table['nid'] == 101, 'aliases'] = 'Mary Ann Lane; M. A. Lyn'
    roster = judges.Roster(table)
    assert found(roster, 'LANE', 1, 2000) == (101, FIRST)
    assert found(roster, 'MA LYN', 1, 2000) == (101, FIRST)
    assert found(roster, 'LYNCH', 1, 2000) == (101, FIRST)
    built = judges.build_roster(
        pd.DataFrame([(1386716, 1, 'King, Carolyn Dineen', APPEALS, 'U.S. Court of Appeals for the Fifth Circuit', 'Jimmy Carter', 'Democratic', '', '', 'Voice', '', '1979-07-13', '', '')], columns = SERVICE_COLUMNS),
        pd.DataFrame([(1386716, 'King', 'Carolyn', 'Dineen', '', '1938', 'Female', 'White')], columns = DEMOGRAPHICS_COLUMNS))
    assert judges.Roster(built).match('RANDALL', 5, 1985) == 0


def test_identify_and_describe(roster: judges.Roster) -> None:
    rows = roster.identify(['LYNCH', 'HOWARD', 'NOBODY', 'MARY LYNCH', 'WOODS'], 1, 2016)
    assert roster.nids(rows) == [101, 102, 106]
    assert roster.names(rows) == ['Lynch, Mary Ann', 'Howard, John Q.', 'Woods, Dana Paul']
    lynch, _, woods = (roster.describe(row, 1, 2016) for row in rows)
    assert list(lynch) == roster.described
    assert roster.described[-4:] == ['age', 'senior', 'designated', 'district_judge']
    assert (lynch['party'], lynch['woman'], lynch['prosecutor']) == (-1.0, 1.0, 1.0)
    assert (lynch['age'], lynch['senior'], lynch['designated'], lynch['district_judge']) == (70.0, 0.0, 0.0, 0.0)
    assert (woods['senior'], woods['designated'], woods['district_judge']) == (1.0, 1.0, 1.0)
    assert roster.describe(rows[2], 1, 2014)['senior'] == 0.0
    unknown = roster.describe(rows[0])
    assert unknown['age'] is None and unknown['senior'] is None and unknown['designated'] is None
    # A judge with no rating has none, rather than a rating of zero.
    assert roster.describe(roster.match('N R SMITH', 9, 2010), 9, 2010)['aba_rating'] is None


def test_save_and_load(roster: judges.Roster, tmp_path: pathlib.Path) -> None:
    with pytest.raises(FileNotFoundError, match = 'from_fjc'):
        judges.Roster.load()
    saved = roster.save()
    assert saved == courtpy.utilities.data_folder() / 'judges' / 'roster.csv'
    loaded = judges.Roster.load()
    assert loaded.attributes == roster.attributes
    assert found(loaded, 'N.R. SMITH', 9, 2010) == (105, NINTH)
    assert loaded.describe(loaded.match('LYNCH', 1, 2016), 1, 2016) == roster.describe(roster.match('LYNCH', 1, 2016), 1, 2016)
    assert loaded.data['woman'].dtype == 'boolean'
    elsewhere = roster.save(tmp_path / 'mine' / 'judges.csv')
    assert judges.Roster.create(elsewhere).match('HOWARD', 1, 2020) is not None
    assert judges.Roster.create(roster) is roster
    assert judges.Roster.create(roster.data).match('HOWARD', 1, 2020) is not None


class Response:
    """A response like those from `requests`."""

    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.status = status

    def raise_for_status(self) -> None:
        """Raises an error for a response that failed."""
        if self.status >= 400:
            raise RuntimeError(f'status {self.status}')


class Session:
    """A session that serves the invented judges' files."""

    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.headers: dict[str, str] = {}
        self.urls: list[str] = []
        self.files = {
            courtpy.options._FJC_FILES[kind]: frame.to_csv(index = False).encode()
            for kind, frame in tables().items()}

    def get(self, url: str, **kwargs: Any) -> Response:
        """Records the request and returns the file it names."""
        self.urls.append(url)
        return Response(self.files[url.rsplit('/', 1)[-1]], self.status)


def test_download(tmp_path: pathlib.Path) -> None:
    session = Session()
    paths = judges.download(tmp_path / 'fjc', session = session)
    assert list(paths) == ['service', 'demographics', 'career']
    assert [p.name for p in paths.values()] == list(courtpy.options._FJC_FILES.values())
    assert session.urls[0] == 'https://www.fjc.gov/sites/default/files/history/federal-judicial-service.csv'
    assert len(session.urls) == 3
    # Files that are there are not downloaded again unless asked.
    judges.download(tmp_path / 'fjc', session = session)
    assert len(session.urls) == 3
    judges.download(tmp_path / 'fjc', session = session, overwrite = True)
    assert len(session.urls) == 6
    assert judges.Roster(judges.build_roster(**paths)).match('LYNCH', 1, 2020) is not None
    with pytest.raises(RuntimeError, match = '503'):
        judges.download(tmp_path / 'other', session = Session(status = 503))
    # The default folder is outside of any project.
    assert judges.download(session = session)['service'].parent == (
        courtpy.utilities.data_folder() / 'judges')


def test_download_identifies_courtpy(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    made = []

    def make() -> Session:
        made.append(Session())
        return made[-1]

    monkeypatch.setattr(courtpy.utilities.requests, 'Session', make)
    judges.download(tmp_path)
    assert made[0].headers == {'User-Agent': courtpy.options._USER_AGENT}
    assert len(made[0].urls) == 3


def test_from_fjc(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    calls = []

    def fake(folder: Any = None, *, overwrite: bool = False, session: Any = None) -> dict[str, pathlib.Path]:
        calls.append(overwrite)
        return original(folder, overwrite = overwrite, session = Session())

    original = judges.download
    monkeypatch.setattr(judges, 'download', fake)
    roster = judges.Roster.from_fjc(tmp_path, update = True)
    assert calls == [True]
    assert found(roster, 'LYNCH', 1, 2020) == (101, FIRST)
    assert 'prosecutor' in roster.attributes


def test_merge_judges(roster: judges.Roster, tmp_path: pathlib.Path) -> None:
    # A row for each judge on each case, as `lists_to_rows` makes them.
    data = pd.DataFrame({
        'judge': ['LYNCH', 'WOODS', 'NOBODY', None, 'SMITH', 'M. SMITH'],
        'court_num': [1, 1, 1, 1, 9, 9],
        'year': [2016, 2016, 2016, 2016, 2010, 2010],
        'reversed': [True, True, True, False, False, False]}, index = list('abcdef'))
    dataset = judges.MergeJudges().apply(
        amos.Dataset(data.copy(), label = 'reversed'), source = roster, indicator = 'found')
    merged = dataset.data
    # A merger never adds, removes, or reorders rows.
    assert list(merged.index) == list(data.index) and dataset.label == 'reversed'
    assert list(merged['found']) == [True, True, False, False, False, True]
    assert list(merged.columns[4:-1]) == [f'judge_{name}' for name in roster.described]
    assert merged.loc['a', ['judge_party', 'judge_woman', 'judge_age', 'judge_designated']].tolist() == [-1.0, 1.0, 70.0, 0.0]
    assert merged.loc['b', ['judge_senior', 'judge_designated', 'judge_district_judge']].tolist() == [1.0, 1.0, 1.0]
    assert merged.loc[['c', 'd', 'e'], 'judge_party'].isna().all()
    assert all(merged[c].dtype == 'float64' for c in merged.columns[4:-1])
    record = dataset.history[0]
    assert (record['technique'], record['source'], record['rows'], record['matched']) == ('merge_judges', 'Roster', 6, 3)
    assert record['created'] == list(merged.columns[4:])
    # Other columns of the roster can be added, under another prefix, and
    # the judge can be named by another column, such as a case's author.
    cases = data.rename(columns = {'judge': 'author'})
    path = roster.save(tmp_path / 'roster.csv')
    dataset = judges.MergeJudges().apply(
        amos.Dataset(cases), source = str(path), name = 'author', prefix = 'author_',
        columns = 'judge, nid, president, party, age')
    merged = dataset.data
    assert list(merged.columns[4:]) == ['author_judge', 'author_nid', 'author_president', 'author_party', 'author_age']
    assert merged.loc['f', ['author_judge', 'author_nid', 'author_president']].tolist() == [
        'Smith, Milton Dale, Jr.', 104, 'George W. Bush']
    assert pd.isna(merged.loc['c', 'author_judge'])
    assert dataset.history[0]['source'] == str(path)
    assert judges.MergeJudges().match(data, roster).tolist() == [0, 5, -1, -1, -1, 3]


def test_merge_judges_fails_clearly(roster: judges.Roster) -> None:
    data = pd.DataFrame({'judge': ['LYNCH'], 'court_num': [1], 'year': [2016], 'judge_party': [0]})
    with pytest.raises(ValueError, match = 'prefix'):
        judges.MergeJudges().apply(amos.Dataset(data), source = roster)
    with pytest.raises(KeyError, match = 'no_such'):
        judges.MergeJudges().apply(amos.Dataset(data), source = roster, columns = ['party', 'no_such'])
    with pytest.raises(KeyError, match = 'merge_judges'):
        judges.MergeJudges().apply(amos.Dataset(data.drop(columns = 'year')), source = roster)
    with pytest.raises(FileNotFoundError, match = 'no roster'):
        judges.MergeJudges().apply(amos.Dataset(data))


def test_code_judges(court_listener_folder: pathlib.Path, roster: judges.Roster) -> None:
    parsed = courtpy.parse(court_listener_folder)
    dataset = courtpy.code(
        parsed, ['code_judges'], parameters = {'code_judges': {'roster': roster}})
    data = dataset.data
    assert data.loc['1', 'panel_names'] == ['Lynch, Mary Ann', 'Howard, John Q.', 'Thompson, Rita Mae']
    assert list(data['panel_found']) == [3, 3, 3]
    assert list(data['panel_found']) == list(data['panel_size'])
    assert data.loc['1', 'author_name'] == 'Lynch, Mary Ann'
    assert data.loc['1', 'dissenting_names'] == []
    assert data.loc['2', 'dissenting_names'] == ['Thompson, Rita Mae']
    assert data.loc['1', 'panel_party'] == pytest.approx(-1 / 3)
    assert data.loc['1', 'panel_woman'] == pytest.approx(2 / 3)
    assert data.loc['1', 'panel_age'] == pytest.approx((74 + 65 + 69) / 3)
    assert data.loc['1', 'panel_designated'] == 0.0
    assert data['panel_prosecutor'].dtype == 'float64'
    created = dataset.history[0]['created']
    assert created[:2] == ['panel_names', 'panel_found']
    assert {'author_name', 'concurring_names', 'panel_senior', 'panel_district_judge'} <= set(created)


def test_code_judges_with_little(roster: judges.Roster, tmp_path: pathlib.Path) -> None:
    # Names can be the text of a saved table, and judges and years missing.
    data = pd.DataFrame({
        'panel_judges': ['LYNCH; WOODS', None, 'NOBODY', 'HOWARD'],
        'court_num': pd.array([1, 1, 1, pd.NA], dtype = 'Int64'),
        'year': pd.array([2016, 2016, 2016, pd.NA], dtype = 'Int64')})
    path = roster.save(tmp_path / 'roster.csv')
    result = judges.CodeJudges().apply(amos.Dataset(data), roster = str(path)).data
    assert list(result['panel_found']) == [2, 0, 0, 1]
    assert result.loc[0, 'panel_district_judge'] == 0.5
    assert pd.isna(result.loc[1, 'panel_party']) and pd.isna(result.loc[3, 'panel_age'])
    assert 'author_name' not in result.columns
    with pytest.raises(KeyError, match = 'panel_judges'):
        judges.CodeJudges().apply(amos.Dataset(pd.DataFrame({'year': [2016]})), roster = roster)
    with pytest.raises(FileNotFoundError, match = 'no roster'):
        judges.CodeJudges().apply(amos.Dataset(data))


def test_vote_table(court_listener_folder: pathlib.Path, roster: judges.Roster) -> None:
    coded = courtpy.code(courtpy.parse(court_listener_folder)).data
    votes = judges.vote_table(coded, roster)
    assert votes.index.name == 'vote_id'
    assert list(votes.index[:4]) == ['1-1', '1-2', '1-3', '2-1']
    assert list(votes['case_id']) == ['1'] * 3 + ['2'] * 3 + ['3'] * 3
    assert list(votes.columns[:3]) == ['case_id', 'judge', 'judge_nid']
    assert list(votes.loc[['1-1', '1-2', '1-3'], 'judge_nid']) == [101, 102, 103]
    # The case's columns are repeated for each of its judges.
    assert list(votes.loc[votes['case_id'] == '2', 'case_name']) == ['Smith v. Acme Corp.'] * 3
    lynch = votes.loc['1-1']
    assert (lynch['judge_party'], lynch['colleagues_party']) == (-1.0, 0.0)
    assert (lynch['judge_woman'], lynch['colleagues_woman']) == (1.0, 0.5)
    assert lynch['judge_author'] and not lynch['judge_concurred'] and not lynch['judge_dissented']
    assert not votes.loc['1-2', 'judge_author']
    # Case 1 was reversed with no dissent, and case 2 affirmed over one.
    assert list(votes.loc[votes['case_id'] == '1', 'vote_reversal']) == [True, True, True]
    assert list(votes.loc[votes['case_id'] == '2', 'judge_dissented']) == [False, False, True]
    assert list(votes.loc[votes['case_id'] == '2', 'vote_reversal']) == [False, False, True]
    assert not votes.loc['2-3', 'outcome_reversal']
    # The winner of case 3 is not known, so neither is how its judges voted.
    assert votes.loc[votes['case_id'] == '3', 'vote_party1_won'].isna().all()
    assert votes['vote_reversal'].dtype == 'boolean'
    assert votes['judge_dissented'].dtype == bool
    assert votes['judge_age'].dtype == 'float64'


def test_vote_table_options(roster: judges.Roster) -> None:
    data = pd.DataFrame({
        'panel_judges': [['LYNCH', 'NOBODY'], [], ['SMITH'], ['HOWARD', 'LYNCH']],
        'dissenting': [[], [], [], ['LYNCH']],
        'concurring': [[], [], [], ['LYNCH']],
        'court_num': [1, 1, 9, 1],
        'year': [2016, 2016, 2010, 2016],
        'judge': ['kept', 'out', 'out', 'replaced'],
        'reversed': [True, True, True, False]}, index = pd.Index(['a', 'b', 'c', 'd'], name = 'case_id'))
    votes = judges.vote_table(data, roster, outcomes = ['reversed'])
    # Cases with no judge who is found are left out.
    assert list(votes.index) == ['a-1', 'd-1', 'd-2']
    assert pd.isna(votes.loc['a-1', 'colleagues_party'])
    assert votes.loc['d-1', 'colleagues_party'] == -1.0
    assert list(votes['judge']) == ['Lynch, Mary Ann', 'Howard, John Q.', 'Lynch, Mary Ann']
    assert list(votes['vote_reversed']) == [True, False, True]
    assert votes.loc['d-2', 'judge_concurred'] and votes.loc['d-2', 'judge_dissented']
    assert not votes['judge_author'].any()
    assert [c for c in votes.columns if c.startswith('vote_')] == ['vote_reversed']
    with pytest.raises(KeyError, match = 'no_such'):
        judges.vote_table(data, roster, outcomes = ['no_such'])
    with pytest.raises(KeyError, match = 'year'):
        judges.vote_table(data.drop(columns = 'year'), roster)
    assert judges.vote_table(data.iloc[1:3], roster).empty


def test_judge_votes_in_a_loader(court_listener_folder: pathlib.Path, roster: judges.Roster) -> None:
    # With a roster saved where the techniques look for it, a loader can make
    # the judges' votes, and a vote can be the label of a study.
    roster.save()
    dataset = loaders.LoadCourtListener().apply(
        source = court_listener_folder,
        coders = 'code_parties, code_case_type, code_outcome, code_judges, judge_votes',
        label = 'vote_reversal', groups = 'case_id')
    assert dataset.label == 'vote_reversal'
    assert dataset.groups == ['case_id']
    assert dataset.data.shape[0] == 9
    assert [e['technique'] for e in dataset.history][-2:] == ['code_judges', 'judge_votes']
    assert dataset.history[-1]['rows'] == [3, 9]
    assert {'panel_party', 'judge_party', 'colleagues_party'} <= set(dataset.data.columns)
    shaped = judges.JudgeVotes().apply(
        amos.Dataset(courtpy.code(courtpy.parse(court_listener_folder)).data),
        outcomes = 'outcome_reversal')
    assert [c for c in shaped.data.columns if c.startswith('vote_')] == ['vote_reversal']


def test_judge_votes_is_a_shaper(court_listener_folder: pathlib.Path, roster: judges.Roster) -> None:
    coded = courtpy.code(courtpy.parse(court_listener_folder)).data
    # Like every shaper, it names the label and groups of the table it makes.
    dataset = judges.JudgeVotes().apply(
        amos.Dataset(coded.copy(), label = 'outcome_reversal'), roster = roster,
        label = 'vote_reversal', groups = 'case_id')
    assert (dataset.label, dataset.task, dataset.groups) == ('vote_reversal', 'classify', ['case_id'])
    record = dataset.history[-1]
    assert record['rows'] == [3, 9] and record['columns'][1] == dataset.data.shape[1]
    # It changes what a row is, so it must come before the data is split.
    split = amos.Dataset(coded.copy(), label = 'outcome_reversal', test = coded.index[:1])
    with pytest.raises(ValueError, match = 'before the data is split'):
        judges.JudgeVotes().apply(split, roster = roster)
    with pytest.raises(KeyError, match = 'no_such'):
        judges.JudgeVotes().apply(amos.Dataset(coded.copy()), roster = roster, label = 'no_such')


def test_build_roster_merges_by_judge() -> None:
    files = tables()
    # A judge who is not in the file of demographics has blank ones.
    table = judges.build_roster(files['service'], files['demographics'].iloc[1:])
    lynch = table.loc[table['nid'] == 101].iloc[0]
    assert (lynch['last_name'], lynch['first_name']) == ('', '') and pd.isna(lynch['woman'])
    assert table.loc[table['nid'] == 102, 'last_name'].iloc[0] == 'Howard'
    # A judge can only be one row of the demographics.
    twice = pd.concat([files['demographics'], files['demographics'].iloc[:1]])
    with pytest.raises(ValueError, match = 'same'):
        judges.build_roster(files['service'], twice)
    # The first of a judge's scores is used, and a judge without a number or
    # a column that the roster already has is left out.
    scores = pd.DataFrame({
        'nid': [101, 101, None, '104'], 'jcs': [0.1, 0.9, 0.5, 0.4], 'party': [9, 9, 9, 9]})
    table = judges.build_roster(files['service'], files['demographics'], scores = scores)
    by_nid = table.drop_duplicates('nid').set_index('nid')
    assert (by_nid.loc[101, 'jcs'], by_nid.loc[104, 'jcs']) == (0.1, 0.4)
    assert by_nid.loc[101, 'party'] == -1


def test_judge_votes_example(roster: judges.Roster, tmp_path: pathlib.Path) -> None:
    # The example study of judges' votes, on invented cases.
    rng = random.Random(7)
    folder = tmp_path / 'data' / 'court_listener'
    bench = ['Lynch', 'Howard', 'Thompson', 'Stone', 'Woods']
    for number in range(1, 91):
        panel = rng.sample(bench, 3)
        reversed_ = rng.random() < 0.4
        dissenter = rng.choice(panel[1:]) if rng.random() < 0.3 else None
        opinions = [make_opinion(
            number * 10, number, author_str = panel[0],
            html_with_citations = '<p>He appeals his sentence for possession of a firearm.</p>')]
        if dissenter:
            opinions.append(make_opinion(
                number * 10 + 1, number, type = '040dissent', author_str = dissenter,
                html_with_citations = f'<p>{dissenter.upper()}, Circuit Judge, dissenting.</p>'))
        write_case(
            folder,
            make_cluster(
                number, judges = f'Before {panel[0]}, {panel[1]}, and {panel[2]}, Circuit Judges.',
                disposition = 'Reversed and remanded.' if reversed_ else 'Affirmed.',
                date_filed = f'{rng.randint(2011, 2014)}-06-15'),
            opinions)
    roster.save()
    idea = chrisjen.Idea.create(EXAMPLES / 'judge_votes.ini')
    idea['load_court_listener_parameters']['download'] = 'none'
    project = courtpy.Project.create(idea, clerk = tmp_path)
    result = project.result
    assert result.label == 'vote_reversal' and result.groups == ['case_id']
    assert [e['technique'] for e in result.history][:7] == [
        'load_court_listener', 'code_parties', 'code_case_type', 'code_outcome',
        'code_judges', 'code_politics', 'judge_votes']
    assert (tmp_path / 'data' / 'votes.csv').is_file()
    assert len(result.data) == 270
    assert {'vote_reversal', 'case_id', 'judge_party', 'colleagues_woman',
            'politics_president_party'} <= set(result.data.columns)
    assert 'outcome_reversal' not in result.data.columns
    assert set(result.tables) >= {'summarize', 'label_balance', 'analyst_comparison', 'scorecard'}
    assert 0 <= result.metrics['roc_auc'] <= 1
    # The judges of a case are all in the training rows or all in the test rows.
    training = set(result.data.loc[result.train, 'case_id'])
    assert not training & set(result.data.loc[result.test, 'case_id'])
    # A dissenter's vote is the opposite of the case's outcome.
    votes = judges.vote_table(courtpy.code(courtpy.parse(folder)).data, roster)
    dissents = votes[votes['judge_dissented']]
    assert len(dissents) > 5
    assert (dissents['vote_reversal'] != dissents['outcome_reversal']).all()
    # A second run loads the saved table of votes instead of making it again.
    again = courtpy.Project.create(idea, clerk = tmp_path).result
    assert 'reused' in again.history[0] and len(again.data) == 270
    assert list(again.train) == list(result.train)
    assert again.metrics['roc_auc'] == result.metrics['roc_auc']


def test_techniques_in_a_wrangler(
    court_listener_folder: pathlib.Path,
    roster: judges.Roster,
    tmp_path: pathlib.Path) -> None:
    # As techniques of the wrangler (after the loader), they take parameters,
    # such as a roster and a table of years that are not in the usual places.
    folder = tmp_path / 'data' / 'court_listener'
    folder.parent.mkdir()
    court_listener_folder.rename(folder)
    # The mergers find the files that their "source" names as the clerk finds
    # files: in the current folder, and then in the input folder.
    named = roster.save(tmp_path / 'data' / 'my_roster.csv')
    (tmp_path / 'data' / 'years.csv').write_text('year,chief\n2020,Roberts\n', encoding = 'utf-8')
    settings = tmp_path / 'study.ini'
    settings.write_text(
        '[general]\n'
        'label = outcome_reversal\n'
        '[files]\n'
        'input_folder = data\n'
        '[panels_project]\n'
        'panels_workers = wrangler\n'
        '[wrangler]\n'
        'techniques = load_court_listener, merge_judges, code_judges, code_politics, judge_votes\n'
        '[merge_judges_parameters]\n'
        'source = my_roster.csv\n'
        'name = author\n'
        'prefix = author_\n'
        'columns = judge, party, age\n'
        'indicator = author_found\n'
        '[code_judges_parameters]\n'
        f'roster = {named.as_posix()}\n'
        '[code_politics_parameters]\n'
        'source = years.csv\n'
        'prefix = court_\n'
        '[judge_votes_parameters]\n'
        f'roster = {named.as_posix()}\n'
        'outcomes = outcome_reversal\n'
        'label = vote_reversal\n'
        'groups = case_id\n', encoding = 'utf-8')
    result = courtpy.Project.create(settings, clerk = tmp_path).result
    history = {e['technique']: e for e in result.history}
    assert list(history)[-4:] == ['merge_judges', 'code_judges', 'code_politics', 'judge_votes']
    # Each merger records its source and how many rows it matched.
    assert (history['merge_judges']['source'], history['merge_judges']['matched']) == ('my_roster.csv', 3)
    assert (history['code_politics']['source'], history['code_politics']['matched']) == ('years.csv', 3)
    assert history['judge_votes']['rows'] == [3, 9]
    # The shaper names the label and groups of the table that it makes.
    assert (result.label, result.task, result.groups) == ('vote_reversal', 'classify', ['case_id'])
    assert len(result.data) == 9
    assert list(result.data['court_chief'].unique()) == ['Roberts']
    assert list(result.data['author_judge'].unique()) == ['Lynch, Mary Ann']
    assert result.data['author_found'].all() and result.data.loc['1-1', 'author_age'] == 74.0
    assert result.data.loc['1-1', 'panel_found'] == 3
    assert [c for c in result.data.columns if c.startswith('vote_')] == ['vote_reversal']
