"""Tests the utilities module."""

from __future__ import annotations

import pathlib
import zipfile

import nagata
import pandas as pd
import pytest
from conftest import Site

import courtpy
from courtpy import utilities


def test_html_to_text() -> None:
    html = (
        '<div><p>First &sect; 1983 paragraph,<br>continued.</p>'
        '<p>Second <span class="star-pagination">*12</span>paragraph'
        '<sup>1</sup>.</p><pre>  keep\n  lines</pre>'
        '<page-number>*13</page-number><script>ignored()</script></div>')
    assert utilities.html_to_text(html) == (
        'First § 1983 paragraph,\ncontinued.\nSecond paragraph1.\nkeep\nlines')
    assert utilities.html_to_text('No tags &amp; text') == 'No tags & text'


def test_html_to_text_with_unclosed_tags() -> None:
    assert utilities.html_to_text('<p>One<p>Two<span class="page-label">3') == 'One\nTwo'


@pytest.mark.parametrize(('text', 'expected'), [
    ('March 3, 2009, Argued', '2009-03-03'),
    ('Decided: Sept. 5 2010', '2010-09-05'),
    ('filed 2020-01-31', '2020-01-31'),
    ('February 30, 2010', None),
    ('no date here', None),
    (None, None)])
def test_parse_date(text: str | None, expected: str | None) -> None:
    assert utilities.parse_date(text) == expected


def test_to_bool() -> None:
    assert utilities.to_bool('TRUE') and utilities.to_bool('yes') and utilities.to_bool(1)
    assert not utilities.to_bool('FALSE') and not utilities.to_bool('') and not utilities.to_bool(None)
    with pytest.raises(ValueError, match = 'not TRUE or FALSE'):
        utilities.to_bool('perhaps')


def test_listify_and_normalize() -> None:
    assert utilities.listify('a, b,,c ') == ['a', 'b', 'c']
    assert utilities.listify(None) == []
    assert utilities.listify(['x', ' y']) == ['x', 'y']
    assert utilities.normalize(' a\n\tb  c ') == 'a b c'
    assert utilities.normalize(None) == ''


def test_expand_courts() -> None:
    courts = utilities.expand_courts('federal_appellate, scotus, ca1')
    assert courts[:2] == ['ca1', 'ca2'] and courts[-1] == 'scotus' and len(courts) == 14
    with pytest.raises(ValueError, match = 'name at least one court'):
        utilities.expand_courts('')


def test_folders_follow_environment(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(courtpy.options._ENV_CONFIG, str(tmp_path / 'c'))
    monkeypatch.setenv(courtpy.options._ENV_DATA, str(tmp_path / 'd'))
    assert utilities.config_folder() == tmp_path / 'c'
    assert utilities.data_folder() == tmp_path / 'd'
    monkeypatch.delenv(courtpy.options._ENV_CONFIG)
    assert utilities.config_folder().name == 'courtpy'


def test_download_file(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    site = Site({'https://example.org/data/scores.csv': 'a,b\n1,2\n'})
    path = utilities.download_file(
        'https://example.org/data/scores.csv', tmp_path / 'new' / 'scores.csv', session = site)
    assert path == tmp_path / 'new' / 'scores.csv'
    assert path.read_text(encoding = 'utf-8') == 'a,b\n1,2\n'
    # A file that is there is not downloaded again unless asked.
    utilities.download_file('https://example.org/data/scores.csv', path, session = site)
    assert len(site.urls) == 1
    utilities.download_file('https://example.org/data/scores.csv', path, overwrite = True, session = site)
    assert len(site.urls) == 2
    # A file that cannot be downloaded is not saved.
    with pytest.raises(RuntimeError, match = '404'):
        utilities.download_file('https://example.org/missing.csv', tmp_path / 'missing.csv', session = site)
    assert not (tmp_path / 'missing.csv').exists()
    # Without a session, one that identifies courtpy is made.
    monkeypatch.setattr(utilities.requests, 'Session', lambda: site)
    assert utilities.open_session() is site and utilities.open_session(site) is site
    assert site.headers == {'User-Agent': courtpy.options._USER_AGENT}


def test_page_links() -> None:
    site = Site({'https://example.org/data/': (
        '<html><body><p>No link here.</p><a name="top">An anchor</a>'
        '<a href="media/2024/court.csv">court.csv</a>'
        '<a class="button" href="/files?id=1&amp;kind=csv"><span>Download CSV</span>\n'
        '  <span>File size: 2 MB</span></a>'
        '<a href="https://other.org/page/">Elsewhere</a></body></html>')})
    assert utilities.page_links('https://example.org/data/', session = site) == [
        ('https://example.org/data/media/2024/court.csv', 'court.csv'),
        ('https://example.org/files?id=1&kind=csv', 'Download CSV File size: 2 MB'),
        ('https://other.org/page/', 'Elsewhere')]
    with pytest.raises(RuntimeError, match = '404'):
        utilities.page_links('https://example.org/nothing/', session = site)


def test_read_csv(tmp_path: pathlib.Path) -> None:
    table = pd.DataFrame({'name': ['Peña', 'Lynch'], 'nid': [1, 2]})
    for encoding in ('utf-8', 'utf-8-sig', 'cp1252'):
        path = tmp_path / f'{encoding}.csv'
        table.to_csv(path, index = False, encoding = encoding)
        assert utilities.read_csv(path).equals(table)
    assert list(utilities.read_csv(path, dtype = str)['nid']) == ['1', '2']
    # A zip archive of one file is read as that file.
    with zipfile.ZipFile(tmp_path / 'table.csv.zip', 'w') as archive:
        archive.writestr('table.csv', table.to_csv(index = False).encode('cp1252'))
    assert utilities.read_csv(tmp_path / 'table.csv.zip').equals(table)


def test_locate(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    clerk = nagata.FileManager(root_folder = tmp_path, input_folder = 'data')
    (tmp_path / 'here.csv').write_text('a\n1\n', encoding = 'utf-8')
    # A file in the current folder is used, and otherwise the input folder.
    assert utilities.locate('here.csv', clerk) == pathlib.Path('here.csv')
    assert utilities.locate('there.csv', clerk) == pathlib.Path(clerk.input_folder) / 'there.csv'
    assert utilities.locate(tmp_path / 'there.csv', clerk) == tmp_path / 'there.csv'
    assert utilities.locate('there.csv') == pathlib.Path('there.csv')
