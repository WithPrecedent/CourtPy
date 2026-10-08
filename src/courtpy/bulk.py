"""Extracts cases from CourtListener's bulk data.

CourtListener publishes its whole database as compressed CSV files every
three months (on the last day of March, June, September, and December), in a
public bucket that needs no API key and has no limits on requests:
https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/

Four files hold what courtpy needs:

| File | Size (2026) | Holds |
| --- | --- | --- |
| courts | < 1 MB | Each court's name and jurisdiction. |
| dockets | about 5 GB | Each case's court, docket number, and dates. |
| opinion-clusters | about 2.5 GB | Each decision's parties, judges, date, and status. |
| citations | about 130 MB | Each decision's reporter citations. |
| opinions | about 55 GB | The text of each opinion. |

`BulkData.extract` reads them in that order, keeps the cases of the courts and
dates asked for, and saves each case in the same form as the API client does
(see `courtpy.courtlistener`), so the same parser reads both. Reading the
opinions file takes hours, so it is worth extracting every court and year you
may need at once.

By default, the files are downloaded first (to a folder outside of any
project, see `courtpy.utilities.data_folder`) and kept, so that they can be
used again and an interrupted download resumes. With `stream = True`, they are
read straight from the bucket instead, which needs no disk space but starts
over if the connection fails.

Contents:
    BulkData: lists, downloads, and extracts cases from the bulk data.
    logger: the module's logger, which reports the progress of downloads and
        of reading the files.
    _BOOLEANS: columns of the bulk files that hold booleans, written as "t"
        and "f".
    _BUFFER_BYTES: how many bytes of opinion text are held in memory before
        the opinions are written to their saved cases.
    _CHUNK: how many bytes of a bulk file are downloaded at a time.
    _CLUSTER_SKIPPED: columns of the opinion clusters file that are not kept
        with a saved case (dates the record changed and the paths of files
        stored by CourtListener).
    _DATE: pattern for the date in the name of a bulk file, such as
        "opinions-2026-09-30.csv.bz2", with the date as its group.
    _DIALECT: settings for the `csv` module to read the bulk files, which
        PostgreSQL writes with a backslash before quotes in values (instead of
        doubling them).
    _FIELD_LIMIT: the longest value (in characters) that the `csv` module
        reads, which is raised because opinions are longer than its default.
        It is at most 2 ** 31 - 1, because a C `long` is 32 bits on Windows.
    _INTEGERS: columns of the bulk files that hold whole numbers.
    _KINDS: the bulk files that `BulkData` uses, in the order `extract` reads
        them.
    _REPORT_EVERY: how many rows of a bulk file are read between progress
        messages.
    _convert: returns a row of a bulk file with numbers, booleans, and missing
        values.
    _flush: writes buffered opinions to their saved cases.
    _integer: returns a value as an `int`, if it is a whole number.

"""

from __future__ import annotations

import bz2
import contextlib
import csv
import dataclasses
import io
import logging
import pathlib
import re
import sys
from collections.abc import Iterable, Iterator, Mapping
from typing import Any
from xml.etree import ElementTree as ET

import requests

from . import courtlistener, options, utilities

logger = logging.getLogger(__name__)
_DIALECT: dict[str, Any] = {'escapechar': '\\', 'doublequote': False}
_FIELD_LIMIT: int = min(sys.maxsize, 2 ** 31 - 1)
_KINDS: tuple[str, ...] = (
    'courts', 'dockets', 'opinion-clusters', 'citations', 'opinions')
_CHUNK: int = 8 * 1024 * 1024
_REPORT_EVERY: int = 1_000_000
_BUFFER_BYTES: int = 200 * 1024 * 1024
_DATE: re.Pattern[str] = re.compile(r'(\d{4}-\d{2}-\d{2})\.csv\.bz2$')
_INTEGERS: frozenset[str] = frozenset({
    'id', 'cluster_id', 'docket_id', 'author_id', 'citation_count',
    'ordering_key', 'page_count', 'scdb_decision_direction',
    'scdb_votes_majority', 'scdb_votes_minority'})
_BOOLEANS: frozenset[str] = frozenset({
    'per_curiam', 'blocked', 'date_filed_is_approximate', 'extracted_by_ocr'})
_CLUSTER_SKIPPED: frozenset[str] = frozenset({
    'date_created', 'date_modified', 'filepath_json_harvard',
    'filepath_pdf_harvard', 'filepath_pdf_scan', 'filepath_xml_scan'})


@dataclasses.dataclass
class BulkData:
    """Lists, downloads, and extracts cases from CourtListener's bulk data.

    Args:
        folder: folder to keep the downloaded bulk files in. Defaults to
            `None`, in which case "bulk" in `utilities.data_folder()` is used.
        date: the date of the files to use, as YYYY-MM-DD. Defaults to `None`,
            in which case the newest files are used.
        stream: whether to read the files straight from the bucket instead
            of downloading them. Defaults to `False`.
        base_url: address of the bucket. Defaults to `options._BULK_URL`.
        timeout: seconds to wait for a response. Defaults to
            `options._TIMEOUT`.
        session: an object with `get` and `head` methods like a
            `requests.Session`. Defaults to `None`, in which case a
            `requests.Session` is made.

    """

    folder: pathlib.Path | str | None = None
    date: str | None = None
    stream: bool = False
    base_url: str = options._BULK_URL
    timeout: float = options._TIMEOUT
    session: Any = dataclasses.field(default = None, repr = False)

    """ Initialization Methods """

    def __post_init__(self) -> None:
        """Sets the folder and session.

        If no `folder` was passed, "bulk" in `utilities.data_folder()` is used,
        and `folder` is made a `pathlib.Path`. If no `session` was passed, a
        `requests.Session` is made that identifies courtpy.

        """
        if self.folder is None:
            self.folder = utilities.data_folder() / 'bulk'
        self.folder = pathlib.Path(self.folder)
        if self.session is None:
            self.session = requests.Session()
            self.session.headers.update({'User-Agent': options._USER_AGENT})

    """ Public Methods """

    def available(self, kind: str = 'opinions') -> list[str]:
        """Returns the dates of the files of one kind in the bucket.

        Args:
            kind: "courts", "dockets", "opinion-clusters", "citations", or
                "opinions". Defaults to "opinions".

        Returns:
            The dates, from oldest to newest.

        """
        dates = []
        for key in self._list(f'{options._BULK_PREFIX}{kind}-'):
            match = _DATE.search(key)
            # Skips files whose kind only starts with `kind` (such as
            # "opinions-cited" for "opinions").
            if match and key == f'{options._BULK_PREFIX}{kind}-{match.group(1)}.csv.bz2':
                dates.append(match.group(1))
        return sorted(set(dates))

    def extract(
        self,
        courts: str | Iterable[str],
        start_date: str | None = None,
        end_date: str | None = None,
        folder: pathlib.Path | str = 'court_listener',
        *,
        max_cases: int | None = None,
        overwrite: bool = False) -> list[pathlib.Path]:
        """Saves the cases of `courts` filed between two dates.

        Args:
            courts: CourtListener court ids (such as "ca1") or names of groups
                of them (such as "federal_appellate").
            start_date: earliest date filed, as YYYY-MM-DD. Defaults to `None`
                (no earliest date).
            end_date: latest date filed, as YYYY-MM-DD. Defaults to `None` (no
                latest date).
            folder: folder to save the cases in. Defaults to "court_listener".
            max_cases: most cases to save. Defaults to `None` (every case).
            overwrite: whether to replace cases that were already saved with
                their opinions. Defaults to `False`.

        Returns:
            The paths of the cases saved (or updated) by this call.

        """
        folder = pathlib.Path(folder)
        wanted = utilities.expand_courts(courts)
        date = self.resolve_date()
        logger.info('extracting cases from the bulk data of %s', date)
        court_records = self._courts(wanted)
        dockets = self._dockets(set(wanted))
        clusters = self._clusters(dockets, start_date, end_date, max_cases)
        citations = self._citations(set(clusters))
        paths: dict[int, pathlib.Path] = {}
        for cluster_id, cluster in clusters.items():
            docket = dockets[int(cluster['docket_id'])]
            court = str(docket.get('court_id'))
            path = courtlistener.case_path(folder, court, cluster_id)
            if not overwrite and courtlistener.is_complete(path):
                continue
            courtlistener.save_case(path, {
                'source': 'court_listener',
                'bulk_date': date,
                'court': court_records.get(court, {'id': court}),
                'cluster': cluster,
                'docket': docket,
                'citations': citations.get(cluster_id, []),
                'opinions': []})
            paths[cluster_id] = path
        logger.info('saved %d cases; now adding their opinions', len(paths))
        if paths:
            self._opinions(paths)
        return list(paths.values())

    def fetch(self, kind: str) -> pathlib.Path:
        """Downloads one bulk file, resuming a download that was interrupted.

        Args:
            kind: "courts", "dockets", "opinion-clusters", "citations", or
                "opinions".

        Raises:
            requests.HTTPError: if the file cannot be downloaded.

        Returns:
            The path of the downloaded file.

        """
        name = self._name(kind)
        assert isinstance(self.folder, pathlib.Path)  # noqa: S101
        path = self.folder / name
        url = f'{self.base_url}{options._BULK_PREFIX}{name}'
        head = self.session.head(url, timeout = self.timeout)
        head.raise_for_status()
        size = int(head.headers.get('Content-Length', 0))
        if path.is_file() and (not size or path.stat().st_size == size):
            return path
        path.parent.mkdir(parents = True, exist_ok = True)
        partial = path.with_name(f'{name}.part')
        start = partial.stat().st_size if partial.is_file() else 0
        if size and start >= size:
            partial.replace(path)
            return path
        headers = {'Range': f'bytes={start}-'} if start else {}
        logger.info(
            'downloading %s (%.1f GB) to %s', name, size / 1e9, self.folder)
        with self.session.get(
            url, headers = headers, stream = True,
            timeout = self.timeout) as response:
            response.raise_for_status()
            # A server that ignores the range sends the whole file.
            mode = 'ab' if start and response.status_code == 206 else 'wb'
            written = start if mode == 'ab' else 0
            reported = written
            with partial.open(mode) as file:
                for chunk in response.iter_content(chunk_size = _CHUNK):
                    file.write(chunk)
                    written += len(chunk)
                    if written - reported >= 1e9:
                        reported = written
                        logger.info(
                            '%s: %.1f of %.1f GB', name, written / 1e9,
                            size / 1e9)
        if size and partial.stat().st_size != size:
            message = (
                f'the download of {name} stopped early; run it again to '
                f'resume'
            )
            raise OSError(message)
        partial.replace(path)
        return path

    def resolve_date(self) -> str:
        """Returns `date`, finding the newest complete set of files if needed.

        Returns:
            The date of the files to use, as YYYY-MM-DD.

        Raises:
            LookupError: if no date has every kind of file that courtpy needs.

        """
        if self.date is None:
            common: set[str] | None = None
            for kind in _KINDS:
                dates = set(self.available(kind))
                common = dates if common is None else common & dates
            if not common:
                message = 'no date in the bulk data has every file needed'
                raise LookupError(message)
            self.date = max(common)
        return self.date

    def rows(self, kind: str) -> Iterator[dict[str, str]]:
        """Yields the rows of one bulk file.

        Args:
            kind: "courts", "dockets", "opinion-clusters", "citations", or
                "opinions".

        Yields:
            dict[str, str]: each row, mapping column names to text. Missing
                values are empty `str`s.

        """
        csv.field_size_limit(_FIELD_LIMIT)
        with self._open(kind) as text:
            reader = csv.reader(text, **_DIALECT)
            header = next(reader)
            for count, row in enumerate(reader, start = 1):
                if count % _REPORT_EVERY == 0:
                    logger.info('%s: %d rows read', kind, count)
                yield dict(zip(header, row, strict = False))

    """ Private Methods """

    def _citations(self, cluster_ids: set[int]) -> dict[int, list[str]]:
        """Returns the citations of the clusters in `cluster_ids`.

        The citations file is read only if there are clusters to look for.

        Args:
            cluster_ids: ids of the opinion clusters (cases) being extracted.

        Returns:
            The citations of each cluster, as text such as "950 F.3d 12", by
                its id. Clusters without citations are left out.

        """
        found: dict[int, list[str]] = {}
        if not cluster_ids:
            return found
        for row in self.rows('citations'):
            cluster_id = _integer(row.get('cluster_id'))
            if cluster_id in cluster_ids:
                found.setdefault(cluster_id, []).extend(
                    courtlistener.citation_strings([row]))
        return found

    def _clusters(
        self,
        dockets: Mapping[int, Any],
        start_date: str | None,
        end_date: str | None,
        max_cases: int | None) -> dict[int, dict[str, Any]]:
        """Returns the clusters of `dockets` filed between two dates.

        The clusters file is read only if there are dockets to look for, and
        reading stops once `max_cases` clusters are found. Dates are compared
        as text, which works because they are written as YYYY-MM-DD.

        Args:
            dockets: the dockets of the courts being extracted, by their ids.
            start_date: earliest date filed, as YYYY-MM-DD, or `None` for no
                earliest date.
            end_date: latest date filed, as YYYY-MM-DD, or `None` for no latest
                date.
            max_cases: most clusters to find, or `None` for every cluster.

        Returns:
            Each cluster (converted with `_convert`, without the columns in
                `_CLUSTER_SKIPPED`), by its id.

        """
        found: dict[int, dict[str, Any]] = {}
        if not dockets:
            return found
        for row in self.rows('opinion-clusters'):
            docket_id = _integer(row.get('docket_id'))
            filed = row.get('date_filed') or ''
            if docket_id not in dockets:
                continue
            if (start_date and filed < start_date) or (
                end_date and filed > end_date):
                continue
            cluster = _convert(row)
            for column in _CLUSTER_SKIPPED:
                cluster.pop(column, None)
            found[int(cluster['id'])] = cluster
            if max_cases is not None and len(found) >= max_cases:
                break
        logger.info('found %d cases', len(found))
        return found

    def _courts(self, wanted: list[str]) -> dict[str, dict[str, Any]]:
        """Returns information about the courts in `wanted`.

        A warning is logged for any court in `wanted` that is not in the
        courts file (usually a misspelled court id).

        Args:
            wanted: CourtListener court ids.

        Returns:
            The "id", "full_name", "short_name", "citation_string", and
                "jurisdiction" of each court that was found, by its id.

        """
        kept =('id', 'full_name', 'short_name', 'citation_string', 'jurisdiction')
        found = {
            row['id']: {k: row.get(k) for k in kept}
            for row in self.rows('courts') if row.get('id') in wanted}
        missing = [c for c in wanted if c not in found]
        if missing:
            logger.warning('these courts are not in the bulk data: %s', missing)
        return found

    def _dockets(self, wanted: set[str]) -> dict[int, dict[str, Any]]:
        """Returns the dockets of the courts in `wanted`.

        Only the columns in `options._DOCKET_FIELDS` are kept, which keeps the
        dockets of large courts small enough to hold in memory.

        Args:
            wanted: CourtListener court ids.

        Returns:
            Each docket of those courts (converted with `_convert`), by its id.

        """
        found: dict[int, dict[str, Any]] = {}
        for row in self.rows('dockets'):
            if row.get('court_id') in wanted:
                docket = _convert({k: row.get(k, '') for k in options._DOCKET_FIELDS})
                found[int(docket['id'])] = docket
        logger.info('found %d dockets', len(found))
        return found

    def _list(self, prefix: str) -> list[str]:
        """Returns the keys in the bucket that start with `prefix`.

        The bucket is listed with Amazon S3's "ListObjectsV2" request, which
        returns at most 1,000 keys at a time as XML, with a token for the next
        part of the list when there are more.

        Args:
            prefix: the start of the keys (file paths) to list, such as
                "bulk-data/opinions-".

        Raises:
            requests.HTTPError: if the bucket cannot be listed.

        Returns:
            The keys, in the order the bucket lists them.

        """
        keys: list[str] = []
        token = None
        while True:
            params = {'list-type': '2', 'prefix': prefix}
            if token:
                params['continuation-token'] = token
            response = self.session.get(
                self.base_url, params = params, timeout = self.timeout)
            response.raise_for_status()
            # The listing comes from the bucket's own server.
            root = ET.fromstring(response.content)  # noqa: S314
            namespace = root.tag.split('}')[0] + '}' if root.tag.startswith('{') else ''
            keys.extend(
                element.text or ''
                for element in root.iter(f'{namespace}Key'))
            truncated = root.findtext(f'{namespace}IsTruncated') == 'true'
            token = root.findtext(f'{namespace}NextContinuationToken')
            if not truncated or not token:
                return keys

    def _name(self, kind: str) -> str:
        """Returns the name of the bulk file of one kind.

        Args:
            kind: one of `_KINDS`.

        Raises:
            ValueError: if `kind` is not one of `_KINDS`.

        Returns:
            The file's name, such as "opinions-2026-09-30.csv.bz2", for the date
                from `resolve_date`.

        """
        if kind not in _KINDS:
            message = f'kind must be one of {list(_KINDS)}, not {kind!r}'
            raise ValueError(message)
        return f'{kind}-{self.resolve_date()}.csv.bz2'

    @contextlib.contextmanager
    def _open(self, kind: str) -> Iterator[io.TextIOBase]:
        """Opens one bulk file as text, from the folder or the bucket.

        If `stream` is true, the file is read from the bucket as it arrives
        and decompressed on the way, without being saved. Otherwise, it is
        downloaded first (or found in `folder`) with `fetch`. Either way, the
        connection or file is closed when the `with` statement ends.

        Args:
            kind: one of `_KINDS`.

        Yields:
            io.TextIOBase: the decompressed file, as UTF-8 text.

        """
        if self.stream:
            url = f'{self.base_url}{options._BULK_PREFIX}{self._name(kind)}'
            response = self.session.get(url, stream = True, timeout = self.timeout)
            response.raise_for_status()
            try:
                with bz2.open(response.raw, 'rt', encoding = 'utf-8', newline = '') as text:
                    yield text
            finally:
                response.close()
        else:
            path = self.fetch(kind)
            with bz2.open(path, 'rt', encoding = 'utf-8', newline = '') as text:
                yield text

    def _opinions(self, paths: Mapping[int, pathlib.Path]) -> None:
        """Adds the opinions of the cases in `paths` to their files.

        The opinions file is not sorted by case, so a case's opinions can be
        anywhere in it. Opinions are held in memory (with one field of text
        each, see `courtlistener.slim_opinion`) until their text reaches
        `_BUFFER_BYTES`, and then written to their cases, which keeps memory
        use bounded without rewriting a case's file for every opinion.

        Args:
            paths: the path of each saved case, by its cluster id.

        """
        buffer: dict[int, list[dict[str, Any]]] = {}
        size = 0
        for row in self.rows('opinions'):
            cluster_id = _integer(row.get('cluster_id'))
            if cluster_id is None or cluster_id not in paths:
                continue
            opinion = courtlistener.slim_opinion(_convert(row))
            buffer.setdefault(cluster_id, []).append(opinion)
            size += sum(len(v) for v in opinion.values() if isinstance(v, str))
            if size >= _BUFFER_BYTES:
                _flush(buffer, paths)
                size = 0
        _flush(buffer, paths)


""" Private Functions """


def _convert(row: Mapping[str, str]) -> dict[str, Any]:
    """Returns a row of a bulk file with numbers, booleans, and missing values.

    Args:
        row: the row, with every value as text.

    Returns:
        The row, with ids and counts as `int`s, "t" and "f" as `bool`s, and
            empty text as `None`.

    """
    converted: dict[str, Any] = {}
    for key, value in row.items():
        if value == '':
            converted[key] = None
        elif key in _INTEGERS:
            converted[key] = _integer(value)
        elif key in _BOOLEANS:
            converted[key] = value == 't'
        else:
            converted[key] = value
    return converted


def _flush(
    buffer: dict[int, list[dict[str, Any]]],
    paths: Mapping[int, pathlib.Path]) -> None:
    """Writes buffered opinions to their cases and empties the buffer.

    Args:
        buffer: opinions that have not been written yet, by the cluster id of
            their case. It is emptied.
        paths: the path of each saved case, by its cluster id.

    """
    for cluster_id, opinions in buffer.items():
        courtlistener.add_opinions(paths[cluster_id], opinions)
    buffer.clear()


def _integer(value: Any) -> int | None:
    """Returns `value` as an `int`, if it is a whole number.

    Args:
        value: a value from a bulk file, usually text such as "4567890".

    Returns:
        The number, or `None` if `value` is missing or is not a whole number.

    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
