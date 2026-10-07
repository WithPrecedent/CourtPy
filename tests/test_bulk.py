"""Tests the bulk module."""

from __future__ import annotations

import bz2
import csv
import io
import json
import pathlib
from typing import Any

import pytest
from conftest import FakeResponse

import courtpy
from courtpy import bulk

DATE = '2026-09-30'
OPINION_HTML = (
    '<p>The defendant said "I did not" before trial.\nWe reverse.</p>'
    '<p>See 18 U.S.C. \\u00a7 922.</p>')


def write_bulk(folder: pathlib.Path, kind: str, header: list[str], rows: list[list[Any]]) -> None:
    """Writes a bulk file the way PostgreSQL does, with backslash escapes."""
    text = io.StringIO()
    writer = csv.writer(text, escapechar = '\\', doublequote = False, lineterminator = '\n')
    writer.writerow(header)
    writer.writerows(rows)
    folder.mkdir(parents = True, exist_ok = True)
    (folder / f'{kind}-{DATE}.csv.bz2').write_bytes(bz2.compress(text.getvalue().encode('utf-8')))


class LocalBucket:
    """A session that serves the bulk files in a folder and lists them."""

    def __init__(self, folder: pathlib.Path) -> None:
        self.folder = folder
        self.headers: dict[str, str] = {}

    def head(self, url: str, **kwargs: Any) -> Any:
        path = self.folder / url.rsplit('/', 1)[-1]
        response = FakeResponse({}, 200, {'Content-Length': str(path.stat().st_size)})
        response.raise_for_status = lambda: None
        return response

    def get(self, url: str, params: dict[str, Any] | None = None, **kwargs: Any) -> Any:
        keys = ''.join(
            f'<Contents><Key>bulk-data/{p.name}</Key></Contents>'
            for p in sorted(self.folder.glob('*.bz2'))
            if p.name.startswith((params or {}).get('prefix', '').removeprefix('bulk-data/')))
        response = FakeResponse({}, 200)
        response.content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
            f'{keys}<IsTruncated>false</IsTruncated></ListBucketResult>').encode()
        response.raise_for_status = lambda: None
        return response


@pytest.fixture
def bucket(tmp_path: pathlib.Path) -> pathlib.Path:
    folder = tmp_path / 'bulk'
    write_bulk(folder, 'courts', ['id', 'full_name', 'short_name', 'citation_string', 'jurisdiction', 'notes'], [
        ['ca1', 'Court of Appeals for the First Circuit', 'First Circuit', '1st Cir.', 'F', ''],
        ['ca2', 'Court of Appeals for the Second Circuit', 'Second Circuit', '2d Cir.', 'F', '']])
    write_bulk(folder, 'dockets', ['id', 'court_id', 'docket_number', 'case_name_full', 'date_argued', 'cause', 'blocked'], [
        [10, 'ca1', '19-1234', 'United States v. Doe', '2020-01-15', '', 'f'],
        [20, 'ca2', '19-9999', 'Other v. Case', '', '', 'f'],
        [30, 'ca1', '18-0001', 'Old v. Case', '', '', 'f']])
    write_bulk(folder, 'opinion-clusters', [
        'id', 'judges', 'date_filed', 'case_name', 'case_name_full', 'attorneys', 'disposition',
        'precedential_status', 'docket_id', 'citation_count', 'blocked', 'date_created'], [
        [1, 'Lynch, Howard, Thompson', '2020-03-04', 'United States v. Doe',
         'UNITED STATES, Appellee, v. John DOE, Appellant', 'Assistant United States Attorney',
         '', 'Published', 10, 3, 'f', '2020-03-05 10:00:00+00'],
        [2, 'Pooler', '2020-05-05', 'Other v. Case', '', '', '', 'Published', 20, 0, 'f', ''],
        [3, 'Selya', '2018-02-02', 'Old v. Case', '', '', '', 'Published', 30, 0, 'f', '']])
    write_bulk(folder, 'citations', ['id', 'volume', 'reporter', 'page', 'type', 'cluster_id'], [
        [1, 950, 'F.3d', 12, 1, 1], [2, 1, 'F.4th', 2, 1, 2]])
    write_bulk(folder, 'opinions', [
        'id', 'author_str', 'per_curiam', 'type', 'plain_text', 'html_with_citations', 'cluster_id'], [
        [11, 'Lynch', 'f', '010combined', '', OPINION_HTML, 1],
        [12, 'Howard', 'f', '040dissent', 'I dissent.', '', 1],
        [21, 'Pooler', 't', '010combined', 'Other text.', '', 2]])
    return folder


def test_rows_read_postgres_csv(bucket: pathlib.Path) -> None:
    data = bulk.BulkData(folder = bucket, date = DATE, session = LocalBucket(bucket))
    opinions = list(data.rows('opinions'))
    assert opinions[0]['html_with_citations'] == OPINION_HTML
    assert opinions[2]['per_curiam'] == 't'


def test_available_and_latest(bucket: pathlib.Path) -> None:
    data = bulk.BulkData(folder = bucket, session = LocalBucket(bucket))
    assert data.available('opinions') == [DATE]
    assert data.available('opinion-clusters') == [DATE]
    assert data.resolve_date() == DATE


def test_extract(bucket: pathlib.Path, tmp_path: pathlib.Path) -> None:
    data = bulk.BulkData(folder = bucket, date = DATE, session = LocalBucket(bucket))
    output = tmp_path / 'cases'
    saved = data.extract('ca1', '2019-01-01', '2020-12-31', output)
    assert saved == [output / 'ca1' / '1.json']
    record = json.loads(saved[0].read_text(encoding = 'utf-8'))
    assert record['bulk_date'] == DATE
    assert record['court']['citation_string'] == '1st Cir.'
    assert record['docket']['docket_number'] == '19-1234'
    assert record['citations'] == ['950 F.3d 12']
    assert record['cluster']['citation_count'] == 3
    assert record['cluster']['blocked'] is False
    assert 'date_created' not in record['cluster']
    assert [o['id'] for o in record['opinions']] == [11, 12]
    assert record['opinions'][1] == {
        'id': 12, 'cluster_id': 1, 'type': '040dissent', 'author_str': 'Howard',
        'per_curiam': False, 'plain_text': 'I dissent.'}
    # Cases that were saved with their opinions are skipped the next time.
    assert data.extract('ca1', '2019-01-01', '2020-12-31', output) == []
    table = courtpy.parse(output)
    assert table.loc['1', 'docket_number'] == '19-1234'
    assert table.loc['1', 'date_argued'] == '2020-01-15'
    assert table.loc['1', 'dissenting'] == ['HOWARD']
    assert table.loc['1', 'opinion_reverse']


def test_fetch_resumes_partial_download(bucket: pathlib.Path, tmp_path: pathlib.Path) -> None:
    source = bucket / f'courts-{DATE}.csv.bz2'
    content = source.read_bytes()

    class Remote(LocalBucket):
        def get(self, url: str, headers: dict[str, str] | None = None, **kwargs: Any) -> Any:
            start = int((headers or {}).get('Range', 'bytes=0-')[6:-1])
            response = FakeResponse({}, 206 if start else 200)
            response.raise_for_status = lambda: None
            response.iter_content = lambda chunk_size: [content[start:]]
            return _Context(response)

    local = tmp_path / 'downloads'
    local.mkdir()
    (local / f'courts-{DATE}.csv.bz2.part').write_bytes(content[:10])
    data = bulk.BulkData(folder = local, date = DATE, session = Remote(bucket))
    path = data.fetch('courts')
    assert path.read_bytes() == content
    assert not (local / f'courts-{DATE}.csv.bz2.part').exists()


class _Context:
    """Wraps a response so that it can be used in a `with` statement."""

    def __init__(self, response: Any) -> None:
        self.response = response

    def __enter__(self) -> Any:
        return self.response

    def __exit__(self, *args: Any) -> None:
        return None


def test_unknown_kind(bucket: pathlib.Path) -> None:
    data = bulk.BulkData(folder = bucket, date = DATE, session = LocalBucket(bucket))
    with pytest.raises(ValueError, match = 'kind must be one of'):
        list(data.rows('people'))
