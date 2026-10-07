"""Tests the command line interface."""

from __future__ import annotations

import io
import pathlib

import pytest

import courtpy
from courtpy import cli


def test_rules(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(['rules', 'federal']) == 0
    output = capsys.readouterr().out
    assert 'rules are valid' in output and 'outcome' not in output
    assert '  party1_appellant' in output


def test_bad_rules(tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = tmp_path / 'bad.csv'
    path.write_text('target,variable,kind,pattern,ignorecase,dotall\nopinion,x,oops,x,TRUE,FALSE\n',
                    encoding = 'utf-8')
    assert cli.main(['rules', str(path)]) == 1
    assert 'row 2' in capsys.readouterr().err


def test_parse(court_listener_folder: pathlib.Path, tmp_path: pathlib.Path) -> None:
    output = tmp_path / 'cases.csv'
    assert cli.main(['-q', 'parse', str(court_listener_folder), '--output', str(output)]) == 0
    table = courtpy.load_cases(output)
    assert 'outcome_reversal' in table.columns


def test_split_and_parse_lexis(tmp_path: pathlib.Path) -> None:
    fixtures = pathlib.Path(__file__).parent / 'fixtures'
    folder = tmp_path / 'lexis'
    assert cli.main(['-q', 'split', str(fixtures), '--folder', str(folder)]) == 0
    output = tmp_path / 'lexis.csv'
    assert cli.main([
        '-q', 'parse', str(folder), '--source', 'lexis_nexis', '--output', str(output),
        '--no-code']) == 0
    assert len(courtpy.load_cases(output)) == 2


def test_key_commands(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(['key', 'show']) == 1
    monkeypatch.setattr('sys.stdin', io.StringIO('abcd1234efgh5678\n'))
    assert cli.main(['key', 'set', '--stdin', '--store', 'file']) == 0
    assert cli.main(['key', 'show']) == 0
    output = capsys.readouterr().out
    assert 'abcd' in output and '5678' in output and 'abcd1234efgh5678' not in output
    assert cli.main(['key', 'delete']) == 0
    assert courtpy.secrets.find_api_key() is None


def test_download_needs_a_key(capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(['-q', 'download', 'api', '--courts', 'ca1']) == 1
    assert 'no CourtListener API key' in capsys.readouterr().err
