"""Downloads court opinions from the CourtListener API and reads them.

CourtListener (https://www.courtlistener.com) is a free legal research site
run by the Free Law Project. Its opinions can be downloaded in two ways:

* The REST API, with `CourtListener`, which needs an API key (see
  `courtpy.secrets`). It is best for small or very recent sets of cases,
  because CourtListener limits how many requests an account can make (a free
  account can make 5 a minute, 50 an hour, and 125 a day, as of 2026).
  `CourtListener` waits when asked to, saves its place, and resumes where it
  stopped when it is run again.
* The bulk data files, with `courtpy.bulk.BulkData`, which need no key and
  have no limits, but are very large and are updated every three months.

Both save each case as a JSON file named for its CourtListener cluster id, in
a folder named for its court (such as "court_listener/ca1/4567890.json"),
which `read_case` turns into a `Case` for parsing.

Contents:
    CourtListener: a client for the CourtListener REST API.
    CourtListenerError: raised when the API returns an error.
    RateLimitError: raised when the API asks for a longer pause than allowed.
    add_opinions: adds opinions to a saved case.
    case_path: returns the path of a saved case.
    citation_strings: returns citations as text.
    is_complete: returns whether a case was saved with its opinions.
    opinion_role: classifies an opinion as majority, concurrence, or dissent.
    opinion_text: returns the plain text of an opinion.
    read_case: reads a saved case for parsing.
    save_case: saves a case.
    slim_opinion: keeps one field of an opinion's text, to save space.
    logger: the module's logger, which reports the progress of downloads and
        pauses asked for by CourtListener.
    _OPINION_KEPT: the fields of an opinion that are kept in a saved case,
        besides one field of text (see `slim_opinion`).
    _RETRIES: most times a request is tried again after a temporary error
        (see `_TEMPORARY`) or a failed connection.
    _ROLES: pairs of a fragment of an opinion's type (as CourtListener names
        types, such as "010combined" and "040dissent") and the role of
        opinions whose types contain it, in the order they are checked. Types
        that contain none of them (such as "050addendum") have the role
        "other" (see `opinion_role`).
    _SITE: the address of CourtListener's website, which begins the address
        of each case.
    _TEMPORARY: HTTP status codes of temporary server errors, after which a
        request is tried again.
    _cluster_id: returns the cluster id of an opinion.
    _detail: returns the explanation in an error response.
    _first: returns the first value that is not blank, as text.
    _id_from_url: returns the id at the end of an API address.
    _load_json: returns the contents of a JSON file.
    _retry_after: returns how many seconds a 429 response asks to wait.
    _save_json: saves a JSON file, creating its folder if needed.
    _texts: returns the values that are not blank, as text.
    _true: returns whether a value from the API or bulk data is true.
    _url: returns the address of a case on CourtListener.

"""

from __future__ import annotations

import dataclasses
import email.utils
import json
import logging
import pathlib
import re
import time
import urllib.parse
from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

import requests

from . import cases, options, secrets, utilities

logger = logging.getLogger(__name__)
_SITE: str = 'https://www.courtlistener.com'
_TEMPORARY: frozenset[int] = frozenset({500, 502, 503, 504})
_RETRIES: int = 4
_OPINION_KEPT: tuple[str, ...] = (
    'id', 'cluster_id', 'type', 'author_id', 'author_str', 'per_curiam',
    'joined_by_str', 'joined_by', 'ordering_key', 'download_url')
# "Concurrence in part" is checked before "dissent" and "concur", which it
# also contains. "unamimous" is CourtListener's own spelling of the type.
_ROLES: tuple[tuple[str, str], ...] = (
    ('concurrenceinpart', 'mixed'),
    ('in part', 'mixed'),
    ('dissent', 'dissent'),
    ('concur', 'concurrence'),
    ('combined', 'majority'),
    ('unanimous', 'majority'),
    ('unamimous', 'majority'),
    ('lead', 'majority'),
    ('plurality', 'majority'),
    ('onthemerits', 'majority'),
    ('on the merits', 'majority'),
    ('trialcourt', 'majority'),
    ('trial court', 'majority'))


class CourtListenerError(RuntimeError):
    """Raised when the CourtListener API returns an error."""


class RateLimitError(CourtListenerError):
    """Raised when the API asks for a longer pause than courtpy will wait.

    Its message says how long to wait and how to resume.

    Args:
        seconds: how long CourtListener asked courtpy to wait.

    """

    def __init__(self, seconds: float) -> None:  # noqa: D107
        self.seconds = seconds
        hours = seconds / 3600
        message = (
            f'CourtListener asked courtpy to wait {hours:.1f} hours before '
            f"making more requests (the account's limit was reached). Run "
            f'the same download again later: it resumes where it stopped. '
            f'The bulk data (courtpy.bulk) has no limits.'
        )
        super().__init__(message)


@dataclasses.dataclass
class CourtListener:
    """A client for the CourtListener REST API (version 4).

    Every request includes the API key, which is found with
    `courtpy.secrets.get_api_key` if it is not passed. Requests are spaced at
    least `min_interval` seconds apart. If CourtListener asks for a pause (an
    HTTP 429 response), the client waits as long as it is asked, up to
    `max_wait` seconds. A longer pause (such as the daily limit) raises a
    `RateLimitError`, and `download` can be run again later to resume.

    Args:
        api_key: a CourtListener API key. Defaults to `None`, in which case it
            is found with `courtpy.secrets.get_api_key`.
        base_url: address of the API. Defaults to `options._API_URL`.
        min_interval: fewest seconds between requests. Defaults to
            `options._MIN_INTERVAL`.
        max_wait: most seconds to wait when asked to pause. Defaults to
            `options._MAX_WAIT`.
        timeout: seconds to wait for a response. Defaults to
            `options._TIMEOUT`.
        session: an object with a `get` method like a `requests.Session`.
            Defaults to `None`, in which case a `requests.Session` is made.

    Attributes:
        requests_made: number of requests made by this client.
        _courts: information about each court that has been looked up (see
            `court`), by its id, so that each court is asked for only once.
        _last_request: when the last request was made, from
            `time.monotonic`, so that the next one can wait until
            `min_interval` seconds have passed. It is 0 before the first
            request.

    """

    api_key: str | None = dataclasses.field(default = None, repr = False)
    base_url: str = options._API_URL
    min_interval: float = options._MIN_INTERVAL
    max_wait: float = options._MAX_WAIT
    timeout: float = options._TIMEOUT
    session: Any = dataclasses.field(default = None, repr = False)
    requests_made: int = dataclasses.field(default = 0, init = False)
    _last_request: float = dataclasses.field(
        default = 0.0, init = False, repr = False)
    _courts: dict[str, dict[str, Any]] = dataclasses.field(
        default_factory = dict, init = False, repr = False)

    """ Initialization Methods """

    def __post_init__(self) -> None:
        """Finds the API key and prepares the session.

        If no `api_key` was passed, it is found with
        `courtpy.secrets.get_api_key`. If no `session` was passed, a
        `requests.Session` is made. The session's headers (if it has any) are
        set so that every request includes the key, asks for JSON, and
        identifies courtpy.

        """
        if self.api_key is None:
            self.api_key = secrets.get_api_key()
        if self.session is None:
            self.session = requests.Session()
        headers = getattr(self.session, 'headers', None)
        if headers is not None:
            headers.update({
                'Authorization': f'Token {self.api_key}',
                'Accept': 'application/json',
                'User-Agent': options._USER_AGENT})

    """ Public Methods """

    def court(self, court: str) -> dict[str, Any]:
        """Returns information about a court.

        Args:
            court: a CourtListener court id, such as "ca1".

        Returns:
            The court's "id", "full_name", "short_name", "citation_string",
                and "jurisdiction" (such as "F" for federal appellate courts).

        """
        if court not in self._courts:
            fields = 'id,full_name,short_name,citation_string,jurisdiction'
            self._courts[court] = self.get(
                f'courts/{court}/', {'fields': fields})
        return self._courts[court]

    def download(
        self,
        courts: str | Iterable[str],
        start_date: str | None = None,
        end_date: str | None = None,
        folder: pathlib.Path | str = 'court_listener',
        *,
        max_cases: int | None = None,
        dockets: bool = False,
        overwrite: bool = False) -> list[pathlib.Path]:
        """Downloads the cases of `courts` filed between two dates.

        Each page of cases (opinion clusters) takes one request, and their
        opinions take one more (or more, if they have many opinions). Cases
        that were already saved (with their opinions) are skipped, and the
        place in each court's list of cases is saved, so running a download
        again resumes it. Running a finished download again takes one request
        for each court and saves any cases added to CourtListener since.

        Args:
            courts: CourtListener court ids (such as "ca1") or names of groups
                of them (such as "federal_appellate").
            start_date: earliest date filed, as YYYY-MM-DD. Defaults to `None`
                (no earliest date).
            end_date: latest date filed, as YYYY-MM-DD. Defaults to `None` (no
                latest date).
            folder: folder to save the cases in. Defaults to "court_listener".
            max_cases: most cases to save in each court in this run. Defaults
                to `None` (every case).
            dockets: whether to also download each case's docket (with its
                docket number and date argued). This takes one more request
                for each case. Defaults to `False`.
            overwrite: whether to download cases that were already saved.
                Defaults to `False`.

        Raises:
            RateLimitError: if CourtListener asks for a pause longer than
                `max_wait`. What was downloaded is kept.

        Returns:
            The paths of the cases saved in this run.

        """
        folder = pathlib.Path(folder)
        saved: list[pathlib.Path] = []
        for court in utilities.expand_courts(courts):
            saved.extend(self._download_court(
                court, start_date, end_date, folder,
                max_cases = max_cases, dockets = dockets,
                overwrite = overwrite))
        logger.info(
            'saved %d cases in %s with %d requests', len(saved), folder,
            self.requests_made)
        return saved

    def download_case(
        self,
        cluster_id: int | str,
        folder: pathlib.Path | str = 'court_listener') -> pathlib.Path:
        """Downloads one case by its CourtListener cluster id.

        This takes four requests: the case, its opinions, its docket (which
        names its court), and its court (unless another case from the same
        court was downloaded by this client).

        Args:
            cluster_id: the id of the opinion cluster (the number in the
                case's CourtListener address, after "/opinion/").
            folder: folder to save the case in. Defaults to "court_listener".

        Returns:
            The path of the saved case.

        """
        cluster = self.get(f'clusters/{cluster_id}/')
        opinions = self._opinions_of([int(cluster['id'])])
        return self._save(
            cluster, opinions.get(int(cluster['id']), []), pathlib.Path(folder),
            court = None, docket = True)

    def get(
        self,
        path: str,
        params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Returns the JSON response of one request to the API.

        Args:
            path: a path in the API (such as "clusters/") or a full address
                (such as the "next" page of a response).
            params: query parameters. Defaults to `None`.

        Raises:
            CourtListenerError: if the API returns an error, rejects the key,
                or cannot be reached after several tries.
            RateLimitError: if the API asks for a pause longer than
                `max_wait`.

        Returns:
            The decoded JSON.

        """
        url = path if path.startswith('http') else urllib.parse.urljoin(
            self.base_url, path)
        failures = 0
        while True:
            self._space_requests()
            try:
                response = self.session.get(
                    url, params = params, timeout = self.timeout)
            except requests.RequestException as error:
                failures += 1
                if failures > _RETRIES:
                    message = f'CourtListener could not be reached: {error}'
                    raise CourtListenerError(message) from error
                time.sleep(5 * 2 ** failures)
                continue
            finally:
                self.requests_made += 1
                self._last_request = time.monotonic()
            status = response.status_code
            if status == 429:
                wait = _retry_after(response)
                if wait > self.max_wait:
                    raise RateLimitError(wait)
                logger.info(
                    'CourtListener asked for a pause: waiting %.0f seconds',
                    wait)
                time.sleep(wait)
                continue
            if status in _TEMPORARY and failures < _RETRIES:
                failures += 1
                time.sleep(5 * 2 ** failures)
                continue
            if status in (401, 403):
                message = (
                    f'CourtListener rejected the request ({status}). Check '
                    f'the API key with "courtpy key show". Details: '
                    f'{_detail(response)}'
                )
                raise CourtListenerError(message)
            if status >= 400:
                message = f'CourtListener returned {status}: {_detail(response)}'
                raise CourtListenerError(message)
            return dict(response.json())

    def pages(
        self,
        path: str,
        params: Mapping[str, Any] | None = None) -> Iterator[
            tuple[list[dict[str, Any]], str | None]]:
        """Yields the results of each page of a list, and the next address.

        Args:
            path: a path in the API (such as "clusters/") or the address of a
                page.
            params: query parameters for the first page. Defaults to `None`.

        Yields:
            tuple[list[dict[str, Any]], str | None]: the results on each page
                and the address of the next page (or `None` after the last
                page).

        """
        url: str | None = path
        query = params
        while url:
            data = self.get(url, query)
            next_url = data.get('next')
            yield list(data.get('results', [])), next_url
            url, query = next_url, None

    """ Private Methods """

    def _download_court(
        self,
        court: str,
        start_date: str | None,
        end_date: str | None,
        folder: pathlib.Path,
        *,
        max_cases: int | None,
        dockets: bool,
        overwrite: bool) -> list[pathlib.Path]:
        """Downloads the cases of one court (see `download`).

        The court's cases (opinion clusters) are listed one page at a time, in
        the order they were added to CourtListener. For each page, the
        opinions of the cases that are not already saved are requested
        together, and each case is saved. After each page, the address of the
        next page (and of the page just read) is saved in a state file in the
        ".courtpy" folder of `folder`, named for the court and dates, so that
        the download can be resumed. Information about the court is saved
        there too, so that it is asked for only once.

        Args:
            court: a CourtListener court id, such as "ca1".
            start_date: earliest date filed, as YYYY-MM-DD, or `None` for no
                earliest date.
            end_date: latest date filed, as YYYY-MM-DD, or `None` for no latest
                date.
            folder: folder to save the cases in.
            max_cases: most cases to save in this run, or `None` for every
                case. A page that would go past it is cut short and read again
                the next time, so that the cases left out are not skipped.
            dockets: whether to also download each case's docket.
            overwrite: whether to start from the first page and download cases
                that were already saved.

        Returns:
            The paths of the cases saved in this run.

        """
        state_path = folder / '.courtpy' / (
            f'api_{court}_{start_date or "start"}_{end_date or "end"}.json')
        state = _load_json(state_path) or {}
        params: dict[str, Any] = {'docket__court': court, 'order_by': 'id'}
        if start_date:
            params['date_filed__gte'] = start_date
        if end_date:
            params['date_filed__lte'] = end_date
        # The court is saved so that resuming a download does not ask again.
        court_path = folder / '.courtpy' / f'court_{court}.json'
        information = _load_json(court_path)
        if information is None:
            information = self.court(court)
            _save_json(court_path, information)
        self._courts[court] = information
        saved: list[pathlib.Path] = []
        # Every address is complete (with its filters), so any page can be
        # saved and requested again to resume. Cases are listed in the order
        # they were added to CourtListener, so after the last page has been
        # read, reading it again finds any cases added since.
        first = urllib.parse.urljoin(self.base_url, 'clusters/')
        url = (
            None if overwrite else state.get('next') or state.get('last')
        ) or f'{first}?{urllib.parse.urlencode(params)}'
        while url:
            data = self.get(url)
            clusters = list(data.get('results', []))
            next_url = data.get('next')
            needed = [
                c for c in clusters if overwrite or not is_complete(
                    case_path(folder, court, c['id']))]
            remaining = None if max_cases is None else max_cases - len(saved)
            trimmed = remaining is not None and len(needed) > remaining
            if trimmed:
                needed = needed[:remaining]
            if needed:
                opinions = self._opinions_of(
                    [int(c['id']) for c in needed], court = court)
                for cluster in needed:
                    saved.append(self._save(
                        cluster, opinions.get(int(cluster['id']), []), folder,
                        court = information, docket = dockets))
            # A trimmed page is read again next time, so that the cases left
            # out are not skipped.
            state['next'] = url if trimmed else next_url
            state['last'] = url
            _save_json(state_path, state)
            logger.info(
                '%s: %d cases saved (%d requests so far)', court, len(saved),
                self.requests_made)
            if trimmed or (max_cases is not None and len(saved) >= max_cases):
                break
            url = next_url
        return saved

    def _opinions_of(
        self,
        cluster_ids: Sequence[int],
        court: str | None = None) -> dict[int, list[dict[str, Any]]]:
        """Returns the opinions of the clusters in `cluster_ids`.

        The opinions of a page of clusters are requested together, by the
        range of their ids (and their court, to keep the range small).

        Args:
            cluster_ids: ids of opinion clusters.
            court: the court of the clusters. Defaults to `None`.

        Raises:
            CourtListenerError: if the API returns opinions of other clusters
                (which means that it did not apply the filter).

        Returns:
            The opinions of each cluster, by its id, with only one field of
                text each (see `options._TEXT_FIELDS`).

        """
        low, high = min(cluster_ids), max(cluster_ids)
        params: dict[str, Any] = {
            'cluster__id__range': f'{low},{high}',
            'order_by': 'id',
            'fields': ','.join((*options._OPINION_FIELDS, 'cluster'))}
        if court:
            params['cluster__docket__court'] = court
        wanted = set(cluster_ids)
        found: dict[int, list[dict[str, Any]]] = {i: [] for i in cluster_ids}
        for results, _ in self.pages('opinions/', params):
            for opinion in results:
                cluster_id = _cluster_id(opinion)
                if cluster_id is None or not low <= cluster_id <= high:
                    message = (
                        'CourtListener returned an opinion of a case that was '
                        'not asked for, so the "cluster__id__range" filter may '
                        'have changed. Please report this at '
                        'https://github.com/WithPrecedent/courtpy/issues'
                    )
                    raise CourtListenerError(message)
                if cluster_id in wanted:
                    found[cluster_id].append(slim_opinion(opinion))
        return found

    def _space_requests(self) -> None:
        """Waits until `min_interval` seconds have passed since the last request.

        Nothing is done before the first request (when `_last_request` is 0)
        or if enough time has passed already.

        """
        if self._last_request:
            wait = self.min_interval - (time.monotonic() - self._last_request)
            if wait > 0:
                time.sleep(wait)

    def _save(
        self,
        cluster: Mapping[str, Any],
        opinions: Sequence[Mapping[str, Any]],
        folder: pathlib.Path,
        *,
        court: Mapping[str, Any] | None,
        docket: bool) -> pathlib.Path:
        """Saves a cluster and its opinions (and docket, if asked) as a case.

        The case is saved in the folder of its court, which is the court's id
        from `court` or, if that is `None`, from the docket. A case whose court
        cannot be found is saved in a folder named "unknown". The cluster's
        list of opinion addresses ("sub_opinions") is not kept, because the
        opinions themselves are.

        Args:
            cluster: the opinion cluster, from the API.
            opinions: its opinions, each with one field of text (see
                `slim_opinion`).
            folder: the folder of saved cases.
            court: information about the case's court (see `court`), or `None`
                if it is not known. If it is `None` and the docket is
                downloaded, the court is looked up from the docket's
                "court_id".
            docket: whether to download the case's docket (one more request).

        Returns:
            The path of the saved case.

        """
        docket_record = None
        docket_id = cluster.get('docket_id') or _id_from_url(
            cluster.get('docket'))
        if docket and docket_id:
            docket_record = self.get(
                f'dockets/{docket_id}/',
                {'fields': ','.join(options._DOCKET_FIELDS)})
        court_id = (
            (court or {}).get('id') or (docket_record or {}).get('court_id'))
        if court is None and docket_record and docket_record.get('court_id'):
            court = self.court(str(docket_record['court_id']))
        record = {
            'source': 'court_listener',
            'court': dict(court or {'id': court_id}),
            'cluster': {k: v for k, v in cluster.items() if k != 'sub_opinions'},
            'docket': docket_record,
            'citations': citation_strings(cluster.get('citations', [])),
            'opinions': list(opinions)}
        path = case_path(folder, str(court_id or 'unknown'), cluster['id'])
        save_case(path, record)
        return path


def add_opinions(
    path: pathlib.Path,
    opinions: Iterable[Mapping[str, Any]]) -> None:
    """Adds opinions to a saved case, replacing any with the same id.

    Args:
        path: path of the saved case.
        opinions: opinions to add.

    """
    record = json.loads(path.read_text(encoding = 'utf-8'))
    existing = {o.get('id'): o for o in record.get('opinions', [])}
    for opinion in opinions:
        existing[opinion.get('id')] = dict(opinion)
    record['opinions'] = list(existing.values())
    save_case(path, record)


def case_path(
    folder: pathlib.Path | str,
    court: str,
    cluster_id: int | str) -> pathlib.Path:
    """Returns the path of a saved case.

    Args:
        folder: the folder of saved cases.
        court: the case's CourtListener court id.
        cluster_id: the case's CourtListener cluster id.

    Returns:
        "{folder}/{court}/{cluster_id}.json".

    """
    return pathlib.Path(folder) / court / f'{cluster_id}.json'


def citation_strings(citations: Iterable[Any]) -> list[str]:
    """Returns citations as text, such as "123 F.3d 456".

    Args:
        citations: citations from CourtListener: `dict`s with "volume",
            "reporter", and "page", or text.

    Returns:
        The citations as text.

    """
    found = []
    for citation in citations or []:
        if isinstance(citation, Mapping):
            parts = (citation.get(k) for k in ('volume', 'reporter', 'page'))
            text = ' '.join(str(p) for p in parts if p not in (None, ''))
        else:
            text = str(citation)
        if text.strip():
            found.append(text.strip())
    return found


def is_complete(path: pathlib.Path) -> bool:
    """Returns whether a case was saved with at least one opinion.

    Args:
        path: path of a saved case.

    Returns:
        Whether the file exists, can be read, and has an opinion.

    """
    if not path.is_file():
        return False
    try:
        record = json.loads(path.read_text(encoding = 'utf-8'))
    except (OSError, ValueError):
        return False
    return bool(record.get('opinions'))


def opinion_role(kind: str | None) -> str:
    """Classifies an opinion by its CourtListener type.

    Args:
        kind: the opinion's "type", such as "010combined", "020lead",
            "030concurrence", "035concurrenceinpart", or "040dissent".

    Returns:
        "majority" (combined, lead, unanimous, or plurality opinions, and
            opinions without a type), "concurrence", "dissent", "mixed" (an
            opinion concurring in part and dissenting in part), or "other"
            (such as an addendum).

    """
    text = (kind or '').lower()
    if not text:
        return 'majority'
    for fragment, role in _ROLES:
        if fragment in text:
            return role
    return 'other'


def opinion_text(opinion: Mapping[str, Any]) -> str:
    """Returns the plain text of an opinion.

    Args:
        opinion: an opinion from CourtListener.

    Returns:
        The text of the first field in `options._TEXT_FIELDS` that is not
            blank, converted from HTML or XML, or an empty `str`.

    """
    for field in options._TEXT_FIELDS:
        value = opinion.get(field)
        if isinstance(value, str) and value.strip():
            return utilities.html_to_text(value)
    return ''


def read_case(path: pathlib.Path | str) -> cases.Case:
    """Reads a saved CourtListener case for parsing.

    The sections of text have the same names as the sections that rules find
    in Lexis-Nexis cases (such as "party", "history", "counsel", and
    "judges"), so that the same rules work for both.

    Args:
        path: path of a case saved by `CourtListener` or `BulkData`.

    Returns:
        The case.

    """
    path = pathlib.Path(path)
    record = json.loads(path.read_text(encoding = 'utf-8'))
    cluster = record.get('cluster') or {}
    docket = record.get('docket') or {}
    court = record.get('court') or {}
    opinions = sorted(
        record.get('opinions') or [],
        key = lambda o: (
            o.get('ordering_key') is None, o.get('ordering_key') or 0,
            str(o.get('type') or ''), o.get('id') or 0))
    roles = [opinion_role(o.get('type')) for o in opinions]

    def authors(*kinds: str) -> str:
        """Returns the authors of the case's opinions with some roles.

        Args:
            *kinds: roles of opinions (see `opinion_role`), such as
                "majority" or "dissent".

        Returns:
            The "author_str" of each opinion with one of those roles, one per
                line, leaving out blank ones.

        """
        return '\n'.join(
            str(o.get('author_str') or '').strip()
            for o, role in zip(opinions, roles, strict = True)
            if role in kinds and str(o.get('author_str') or '').strip())

    majority = [
        o for o, role in zip(opinions, roles, strict = True)
        if role == 'majority']
    texts = [opinion_text(o) for o in opinions]
    sections = {
        'party': _first(
            cluster.get('case_name_full'), docket.get('case_name_full'),
            cluster.get('case_name')),
        'court': _first(court.get('full_name'), court.get('id')),
        'docket_number': _first(docket.get('docket_number')),
        'citation': '; '.join(record.get('citations') or []),
        'history': '\n'.join(_texts(
            cluster.get('procedural_history'), cluster.get('history'),
            docket.get('appeal_from_str'))),
        'counsel': _first(cluster.get('attorneys')),
        'disposition': _first(cluster.get('disposition')),
        'judges': _first(cluster.get('judges'), docket.get('panel_str')),
        'opinion_by': authors('majority'),
        'concurring_lines': authors('concurrence', 'mixed'),
        'dissenting_lines': authors('dissent', 'mixed'),
        'posture': _first(cluster.get('posture')),
        'syllabus': _first(cluster.get('syllabus')),
        'opinion': '\n\n'.join(t for t in texts if t)}
    sections = {
        k: utilities.html_to_text(v) if '<' in v else v
        for k, v in sections.items() if v}
    date_filed = _first(cluster.get('date_filed'))
    status = _first(cluster.get('precedential_status'))
    metadata: dict[str, Any] = {
        'source': 'court_listener',
        'cluster_id': cluster.get('id'),
        'docket_id': cluster.get('docket_id') or docket.get('id') or (
            _id_from_url(cluster.get('docket'))),
        'court': court.get('id') or docket.get('court_id'),
        'case_name': _first(cluster.get('case_name')),
        'docket_number': sections.get('docket_number'),
        'citation': sections.get('citation'),
        'disposition': sections.get('disposition'),
        'judges': sections.get('judges'),
        'date_filed': date_filed or None,
        'date_argued': _first(docket.get('date_argued')) or None,
        'year': int(date_filed[:4]) if date_filed[:4].isdigit() else None,
        'precedential_status': status or None,
        'published': status.lower() == 'published' if status else None,
        'per_curiam': any(_true(o.get('per_curiam')) for o in majority),
        'opinions': len(opinions),
        'author_id': next(
            (o.get('author_id') for o in majority if o.get('author_id')), None),
        'panel_ids': [
            i for i in (_id_from_url(p) for p in cluster.get('panel') or [])
            if i is not None],
        'citation_count': cluster.get('citation_count'),
        'nature_of_suit': _first(
            cluster.get('nature_of_suit'), docket.get('nature_of_suit'))
            or None,
        'url': _url(cluster)}
    # Only cases of the Supreme Court have an id in the Supreme Court
    # Database, so other tables of cases get no column for it.
    if _first(cluster.get('scdb_id')):
        metadata['scdb_id'] = _first(cluster.get('scdb_id'))
    return cases.Case(
        id = str(cluster.get('id') or path.stem),
        source = 'court_listener',
        sections = sections,
        metadata = metadata,
        path = path)


def save_case(path: pathlib.Path, record: Mapping[str, Any]) -> None:
    """Saves a case as a JSON file, replacing any file at `path` safely.

    Args:
        path: path to save the case to. Its folder is created if needed.
        record: the case: "source", "court", "cluster", "docket",
            "citations", and "opinions".

    """
    path.parent.mkdir(parents = True, exist_ok = True)
    temporary = path.with_suffix('.json.part')
    temporary.write_text(
        json.dumps(record, ensure_ascii = False), encoding = 'utf-8')
    temporary.replace(path)


def slim_opinion(opinion: Mapping[str, Any]) -> dict[str, Any]:
    """Returns an opinion with only one field of text, to save space.

    Args:
        opinion: an opinion from the API or the bulk data.

    Returns:
        The fields in `_OPINION_KEPT` that it has, and the first field in
            `options._TEXT_FIELDS` that is not blank.

    """
    slim = {k: opinion[k] for k in _OPINION_KEPT if k in opinion}
    if 'cluster_id' not in slim:
        slim['cluster_id'] = _cluster_id(opinion)
    for field in options._TEXT_FIELDS:
        value = opinion.get(field)
        if isinstance(value, str) and value.strip():
            slim[field] = value
            break
    return slim


""" Private Functions """


def _cluster_id(opinion: Mapping[str, Any]) -> int | None:
    """Returns the cluster id of an opinion.

    Args:
        opinion: an opinion from the API or the bulk data.

    Returns:
        Its "cluster_id" or, if that is missing, the id at the end of its
            "cluster" address (as the API gives it), or `None` if it has
            neither.

    """
    value = opinion.get('cluster_id')
    if value not in (None, ''):
        return int(value)
    return _id_from_url(opinion.get('cluster'))


def _detail(response: Any) -> str:
    """Returns the explanation in an error response, shortened.

    Args:
        response: a response from the API, like a `requests.Response`.

    Returns:
        The "detail" of its JSON (where CourtListener explains an error) or,
            if there is none, the text of the response, cut to 300
            characters.

    """
    try:
        detail = response.json().get('detail')
    except Exception:  # noqa: BLE001
        detail = None
    return str(detail or getattr(response, 'text', ''))[:300]


def _first(*values: Any) -> str:
    """Returns the first value that is not blank, as text.

    Args:
        *values: values to check, in order of preference (such as a case's
            full name and then its short name).

    Returns:
        The first value that is not `None` or blank, with spaces trimmed from
            each end, or an empty `str` if they are all blank.

    """
    for value in values:
        if value not in (None, '') and str(value).strip():
            return str(value).strip()
    return ''


def _id_from_url(url: Any) -> int | None:
    """Returns the id at the end of an API address.

    The API refers to related objects by their addresses, such as
    "https://www.courtlistener.com/api/rest/v4/people/101/".

    Args:
        url: an address that ends in a number (with or without a final
            slash), an id itself, or `None`.

    Returns:
        The id, or `None` if `url` is blank or does not end in a number.

    """
    if url in (None, ''):
        return None
    if isinstance(url, int):
        return url
    match = re.search(r'(\d+)/?$', str(url))
    return int(match.group(1)) if match else None


def _load_json(path: pathlib.Path) -> dict[str, Any] | None:
    """Returns the contents of a JSON file.

    Args:
        path: path of the file, such as a download's state file.

    Returns:
        The contents, or `None` if there is no file.

    """
    if not path.is_file():
        return None
    return dict(json.loads(path.read_text(encoding = 'utf-8')))


def _retry_after(response: Any) -> float:
    """Returns how many seconds a 429 response asks the client to wait.

    Args:
        response: the response, like a `requests.Response`.

    Returns:
        The seconds in the "Retry-After" header (a number of seconds or a
            date) or in the message ("Expected available in N seconds"), or
            60 if neither says.

    """
    header = str(getattr(response, 'headers', {}).get('Retry-After', '')).strip()
    if header:
        try:
            return max(float(header), 0.0)
        except ValueError:
            moment = email.utils.parsedate_to_datetime(header)
            if moment is not None:
                return max(moment.timestamp() - time.time(), 0.0)
    match = re.search(r'available in (\d+)', _detail(response))
    return float(match.group(1)) if match else 60.0


def _save_json(path: pathlib.Path, contents: Mapping[str, Any]) -> None:
    """Saves `contents` as a JSON file, creating its folder if needed.

    Args:
        path: path to save the file to.
        contents: what to save. It must be something that `json` can write.

    """
    path.parent.mkdir(parents = True, exist_ok = True)
    path.write_text(json.dumps(contents, indent = 2), encoding = 'utf-8')


def _texts(*values: Any) -> list[str]:
    """Returns the values that are not blank, as text.

    Args:
        *values: values to keep, if they are not blank (such as a case's
            procedural history and the court it was appealed from).

    Returns:
        Each value that is not `None` or blank, with spaces trimmed from each
            end, in order.

    """
    return [str(v).strip() for v in values if v not in (None, '') and str(v).strip()]


def _true(value: Any) -> bool:
    """Returns whether a value from the API or bulk data is true.

    The API gives booleans, but the bulk data (written by PostgreSQL) gives
    "t" and "f".

    Args:
        value: a boolean, or text such as "t", "true", or "1" (in any case).

    Returns:
        Whether `value` is true.

    """
    if isinstance(value, str):
        return value.strip().lower() in {'t', 'true', '1'}
    return bool(value)


def _url(cluster: Mapping[str, Any]) -> str | None:
    """Returns the address of a case on CourtListener.

    Args:
        cluster: the case's opinion cluster.

    Returns:
        Its "absolute_url" on CourtListener's website or, if it has none (as
            in the bulk data), an address made from its id and "slug", or
            `None` if it has no id either.

    """
    if cluster.get('absolute_url'):
        return f'{_SITE}{cluster["absolute_url"]}'
    if cluster.get('id'):
        slug = cluster.get('slug') or 'case'
        return f'{_SITE}/opinion/{cluster["id"]}/{slug}/'
    return None
