"""Tests the politics module."""

from __future__ import annotations

import pathlib

import amos
import pandas as pd
import pytest
from conftest import Site

import courtpy
from courtpy import politics

MQ_PAGE = 'https://mqscores.wustl.edu/measures.php'
MQ_COURT = 'https://mqscores.wustl.edu/media/2024/court.csv'
MQ_JUSTICES = 'https://mqscores.wustl.edu/media/2024/justices.csv'
VOTEVIEW = 'https://voteview.com/static/data/out/members/HSall_members.csv'
# The links of the page of Martin-Quinn scores, which are to a folder named
# for the year of the release.
MQ_LINKS = (
    '<html><body><h3>2024 JUSTICE DATA FILES</h3>'
    '<a href="media/2024/justices.csv">justices.csv</a>'
    '<a href="media/2024/justices.dta">justices.dta</a>'
    '<h3>2024 COURT DATA FILES</h3>'
    '<a href="media/2024/court.csv">court.csv</a>'
    '<a href="media/2023/2023_MQscores.zip">2023 MQ Scores Data</a></body></html>')


@pytest.fixture
def court() -> pd.DataFrame:
    """Returns scores of the Supreme Court like the Martin-Quinn "court" file."""
    return pd.DataFrame({
        'term': ['2004', '2005a', '2005b', '2006'],
        'med': [0.1, 0.0, 0.5, 0.6],
        'med_sd': [0.2, 0.2, 0.2, 0.2],
        'min': [-2.0, -2.5, -2.5, -3.0],
        'max': [3.0, 3.5, 3.5, 4.0],
        'justice': ['SDOConnor', 'SDOConnor', 'AMKennedy', 'AMKennedy'],
        'just_pr': [0.9, 0.8, 0.9, 0.99]})


@pytest.fixture
def scores() -> pd.DataFrame:
    """Returns scores of justices like the Martin-Quinn "justices" file."""
    return pd.DataFrame({
        'term': [2020, 2021, 2020],
        'justice': [117, 117, 111],
        'justiceName': ['ACBarrett', 'ACBarrett', 'JGRoberts'],
        'post_mn': [0.961, 1.004, 0.3],
        'post_sd': [0.309, 0.259, 0.2],
        'post_med': [0.954, 1.002, 0.3],
        'post_025': [0.394, 0.518, 0.0],
        'post_975': [1.601, 1.514, 0.6]})


@pytest.fixture
def members() -> pd.DataFrame:
    """Returns members of Congress like the Voteview file of their ideology."""
    return pd.DataFrame({
        'congress': [97, 97, 97, 97, 97, 97, 98, 98, 98],
        'chamber': ['President', 'Senate', 'Senate', 'Senate', 'House', 'House', 'Senate', 'House', 'House'],
        'nominate_dim1': [0.9, -0.4, 0.1, 0.3, -0.2, 0.4, 0.2, -0.5, -0.1],
        'nominate_dim2': [0.0, 0.5, 0.5, 0.5, 0.1, 0.1, 0.3, 0.2, 0.2]})


@pytest.fixture
def site(court: pd.DataFrame, scores: pd.DataFrame, members: pd.DataFrame) -> Site:
    """Returns a session that serves the sources' pages and files."""
    return Site({
        MQ_PAGE: MQ_LINKS,
        MQ_COURT: court.to_csv(index = False),
        MQ_JUSTICES: scores.to_csv(index = False),
        VOTEVIEW: members.to_csv(index = False)})


def test_technique_is_in_the_library() -> None:
    assert amos.library.classify('code_politics') == 'merger'
    assert issubclass(politics.CodePolitics, amos.mergers.MergeKeys)


def test_presidents() -> None:
    table = politics.presidents()
    assert list(table.columns) == ['year', 'president_party']
    parties = table.set_index('year')['president_party']
    assert parties.index[0] == 1933
    # A president who takes office in a year is the president of that year.
    assert [parties[y] for y in (1980, 1981, 1992, 1993, 2008, 2009, 2020, 2021, 2025)] == [
        -1, 1, 1, -1, 1, -1, 1, -1, 1]
    assert list(politics.presidents(2000, 2002)['year']) == [2000, 2001, 2002]
    with pytest.raises(ValueError, match = 'knows the party'):
        politics.presidents(1900)
    with pytest.raises(ValueError, match = 'knows the party'):
        politics.presidents(end = parties.index[-1] + 1)


def test_martin_quinn(court: pd.DataFrame) -> None:
    table = politics.martin_quinn(court)
    assert list(table.columns) == [
        'year', 'supreme_court_median', 'supreme_court_min', 'supreme_court_max']
    assert list(table['year']) == [2004, 2005, 2006]
    # The two parts of the 2005 term are one year, with their mean.
    assert list(table['supreme_court_median']) == [0.1, 0.25, 0.6]
    assert list(table['supreme_court_min']) == [-2.0, -2.5, -3.0]
    with pytest.raises(KeyError, match = 'Martin-Quinn'):
        politics.martin_quinn(court.drop(columns = 'med'))


def test_justices(scores: pd.DataFrame, tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'justices.csv'
    scores.to_csv(path, index = False)
    table = politics.justices(path)
    assert list(table.columns) == ['term', 'justice', 'justiceName', 'martin_quinn', 'martin_quinn_sd']
    assert list(table['martin_quinn']) == [0.961, 1.004, 0.3]
    with pytest.raises(KeyError, match = 'justices'):
        politics.justices(scores.drop(columns = 'post_mn'))
    # The scores have the Supreme Court Database's numbers for the justices,
    # so a merger adds them to its votes.
    votes = pd.DataFrame({'term': [2021, 2020, 2019], 'justice': [117, 111, 117], 'vote': [1, 2, 1]})
    merged = amos.mergers.MergeKeys().apply(votes, source = table, on = ['term', 'justice'])
    assert merged.data['martin_quinn'].tolist()[:2] == [1.004, 0.3]
    assert pd.isna(merged.data['martin_quinn'].iloc[2])
    assert merged.history[-1]['matched'] == 2


def test_nominate(members: pd.DataFrame) -> None:
    table = politics.nominate(members)
    assert list(table.columns) == ['year', 'senate_median', 'house_median']
    # The 97th Congress sat in 1981 and 1982, and the 98th in 1983 and 1984.
    assert list(table['year']) == [1981, 1982, 1983, 1984]
    assert list(table['senate_median']) == pytest.approx([0.1, 0.1, 0.2, 0.2])
    assert list(table['house_median']) == pytest.approx([0.1, 0.1, -0.3, -0.3])
    assert list(politics.nominate(members, score = 'nominate_dim2')['senate_median']) == [0.5, 0.5, 0.3, 0.3]
    with pytest.raises(KeyError, match = 'members of Congress'):
        politics.nominate(members, score = 'no_such_score')


def test_download(site: Site, tmp_path: pathlib.Path) -> None:
    paths = politics.download(tmp_path / 'scores', session = site)
    assert list(paths) == ['supreme_court', 'justices', 'congress']
    assert [p.name for p in paths.values()] == ['court.csv', 'justices.csv', 'HSall_members.csv']
    assert all(p.is_file() for p in paths.values())
    # The Martin-Quinn files are found by the links on their page.
    assert site.urls == [MQ_PAGE, MQ_COURT, MQ_PAGE, MQ_JUSTICES, VOTEVIEW]
    # Files that are there are not downloaded again unless asked.
    politics.download(tmp_path / 'scores', session = site)
    assert len(site.urls) == 5
    politics.download(tmp_path / 'scores', kinds = 'congress', overwrite = True, session = site)
    assert site.urls[5:] == [VOTEVIEW]
    assert list(politics.download(tmp_path / 'one', kinds = ['justices'], session = site)) == ['justices']
    # The default folder is outside of any project.
    assert politics.download(session = site)['congress'].parent == (
        courtpy.utilities.data_folder() / 'politics')


def test_download_fails_clearly(site: Site, tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match = 'kinds'):
        politics.download(tmp_path, kinds = 'presidents', session = site)
    site.pages[MQ_PAGE] = '<a href="media/2024/justices.csv">justices.csv</a>'
    with pytest.raises(ValueError, match = "no link to 'court.csv'"):
        politics.download(tmp_path, kinds = 'supreme_court', session = site)
    assert not (tmp_path / 'court.csv').exists()
    with pytest.raises(RuntimeError, match = '503'):
        politics.download(tmp_path, session = Site(site.pages, status = 503))


def test_build_table(court: pd.DataFrame, members: pd.DataFrame, tmp_path: pathlib.Path) -> None:
    assert politics.build_table(False, False).equals(politics.presidents())
    path = tmp_path / 'court.csv'
    court.to_csv(path, index = False)
    table = politics.build_table(path, members).set_index('year')
    assert list(table.columns) == [
        'president_party', 'supreme_court_median', 'supreme_court_min',
        'supreme_court_max', 'senate_median', 'house_median']
    assert table.index.is_monotonic_increasing and table.index.is_unique
    assert table.loc[2005, 'supreme_court_median'] == 0.25
    assert table.loc[1981, 'senate_median'] == pytest.approx(0.1)
    assert pd.isna(table.loc[1981, 'supreme_court_median'])
    assert table.loc[1981, 'president_party'] == 1
    assert 'house_median' not in politics.build_table(court, False).columns


def test_build_table_downloads_its_sources(site: Site, monkeypatch: pytest.MonkeyPatch) -> None:
    # With no files, the sources' current files are downloaded, once.
    monkeypatch.setattr(courtpy.utilities.requests, 'Session', lambda: site)
    table = politics.build_table().set_index('year')
    assert table.loc[2005, 'supreme_court_median'] == 0.25
    assert table.loc[1983, 'house_median'] == pytest.approx(-0.3)
    assert site.headers == {'User-Agent': courtpy.options._USER_AGENT}
    folder = courtpy.utilities.data_folder() / 'politics'
    assert sorted(p.name for p in folder.iterdir()) == ['HSall_members.csv', 'court.csv']
    asked = len(site.urls)
    assert politics.build_table(True, True).set_index('year').equals(table)
    assert len(site.urls) == asked
    assert list(politics.justices()['justice']) == [117, 117, 111]


def test_code_politics(court: pd.DataFrame, members: pd.DataFrame, tmp_path: pathlib.Path) -> None:
    cases = pd.DataFrame({'year': pd.array([1981, 2005, pd.NA, 2100], dtype = 'Int64'), 'x': [1, 2, 3, 4]})
    # With no table, only the party of the president is added.
    dataset = politics.CodePolitics().apply(amos.Dataset(cases.copy()))
    record = dataset.history[0]
    assert record['created'] == ['politics_president_party']
    assert (record['rows'], record['matched']) == (4, 2)
    assert list(dataset.data['politics_president_party'].iloc[:2]) == [1, 1]
    assert dataset.data['politics_president_party'].iloc[2:].isna().all()
    assert list(dataset.data.index) == list(cases.index)
    path = tmp_path / 'politics.csv'
    politics.build_table(court, members).to_csv(path, index = False)
    dataset = courtpy.code(cases.copy(), 'code_politics', parameters = {
        'code_politics': {'source': str(path)}})
    data = dataset.data
    assert data.loc[1, 'politics_supreme_court_median'] == 0.25
    assert data.loc[0, 'politics_house_median'] == pytest.approx(0.1)
    assert pd.isna(data.loc[0, 'politics_supreme_court_max'])
    assert len(dataset.history[0]['created']) == 6
    assert dataset.history[0]['source'] == str(path)


def test_code_politics_parameters() -> None:
    # It takes the parameters of the merge_keys technique of amos.
    cases = pd.DataFrame({'term': pd.array([1981, 2005, pd.NA, 2100], dtype = 'Int64')})
    mine = pd.DataFrame({'began': [2005, 1981], 'chief': ['Roberts', 'Burger'], 'seats': [9, 9]})
    data = politics.CodePolitics().apply(
        amos.Dataset(cases.copy()), source = mine, on = 'term', other_on = 'began',
        prefix = 'court_', columns = 'chief', indicator = 'known').data
    assert list(data.columns) == ['term', 'court_chief', 'known']
    assert list(data['court_chief'].iloc[:2]) == ['Burger', 'Roberts']
    assert list(data['known']) == [True, True, False, False]
    # A table with a year twice says two things about its cases, which the
    # earlier version's table of Martin-Quinn scores did.
    twice = pd.DataFrame({'term': [2005, 2005, 1981], 'chief': ['Roberts', 'Other', 'Burger']})
    with pytest.raises(ValueError, match = 'duplicates'):
        politics.CodePolitics().apply(amos.Dataset(cases.copy()), source = twice, on = 'term')
    data = politics.CodePolitics().apply(
        amos.Dataset(cases.copy()), source = twice, on = 'term', duplicates = 'first').data
    assert list(data['politics_chief'].iloc[:2]) == ['Burger', 'Roberts']


def test_code_politics_missing_columns() -> None:
    with pytest.raises(KeyError, match = 'the data'):
        politics.CodePolitics().apply(amos.Dataset(pd.DataFrame({'x': [1]})))
    with pytest.raises(KeyError, match = 'the other table'):
        politics.CodePolitics().apply(
            amos.Dataset(pd.DataFrame({'year': [2000]})), source = pd.DataFrame({'term': [2000]}))
    # Years that are text are not matched to years that are numbers.
    with pytest.raises(TypeError, match = 'parse_numbers'):
        politics.CodePolitics().apply(amos.Dataset(pd.DataFrame({'year': ['2000']})))
