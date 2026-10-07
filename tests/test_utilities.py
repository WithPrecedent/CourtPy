"""Tests the utilities module."""

from __future__ import annotations

import pathlib

import pytest

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
