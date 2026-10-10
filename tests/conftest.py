"""Shared fixtures for the tests."""

from __future__ import annotations

import copy
import json
import pathlib
from collections.abc import Iterator
from typing import Any

import chrisjen
import pytest

import courtpy

FIXTURES = pathlib.Path(__file__).parent / 'fixtures'


@pytest.fixture(autouse = True)
def restore_library() -> Iterator[None]:
    """Removes the classes that a test adds to the library."""
    saved = copy.deepcopy(chrisjen.library.contents)
    yield
    chrisjen.library.contents.clear()
    chrisjen.library.contents.update(saved)


@pytest.fixture(autouse = True)
def private_secrets(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch) -> None:
    """Keeps tests away from the user's real API key and folders."""
    monkeypatch.delenv(courtpy.options._ENV_API_KEY, raising = False)
    monkeypatch.setenv(courtpy.options._ENV_CONFIG, str(tmp_path / 'config'))
    monkeypatch.setenv(courtpy.options._ENV_DATA, str(tmp_path / 'data'))
    monkeypatch.setattr(courtpy.secrets, '_keyring', lambda: None)


def make_cluster(cluster_id: int, **changes: Any) -> dict[str, Any]:
    """Returns a CourtListener cluster like those the API returns."""
    cluster = {
        'id': cluster_id,
        'absolute_url': f'/opinion/{cluster_id}/united-states-v-doe/',
        'docket_id': cluster_id * 10,
        'docket': f'https://www.courtlistener.com/api/rest/v4/dockets/{cluster_id * 10}/',
        'case_name': 'United States v. Doe',
        'case_name_full': (
            'UNITED STATES of America, Plaintiff-Appellee, v. John DOE, '
            'Defendant-Appellant'),
        'date_filed': '2020-03-04',
        'judges': 'Before Lynch, Howard, and Thompson, Circuit Judges.',
        'panel': [
            'https://www.courtlistener.com/api/rest/v4/people/101/',
            'https://www.courtlistener.com/api/rest/v4/people/102/',
            'https://www.courtlistener.com/api/rest/v4/people/103/'],
        'citations': [{'volume': 950, 'reporter': 'F.3d', 'page': '12', 'type': 1}],
        'precedential_status': 'Published',
        'disposition': 'Reversed and remanded.',
        'attorneys': 'Jane Roe, Assistant United States Attorney, for appellee.',
        'procedural_history': '',
        'history': '',
        'nature_of_suit': '',
        'posture': '',
        'syllabus': '',
        'citation_count': 4}
    cluster.update(changes)
    return cluster


def make_opinion(opinion_id: int, cluster_id: int, **changes: Any) -> dict[str, Any]:
    """Returns a CourtListener opinion like those the API returns."""
    opinion = {
        'id': opinion_id,
        'cluster_id': cluster_id,
        'cluster': f'https://www.courtlistener.com/api/rest/v4/clusters/{cluster_id}/',
        'type': '010combined',
        'author_id': 101,
        'author_str': 'Lynch',
        'per_curiam': False,
        'joined_by_str': '',
        'ordering_key': None,
        'html_with_citations': (
            '<p>LYNCH, Circuit Judge. John Doe appeals his conviction for '
            'possession of a firearm. He argues that the search violated the '
            'Fourth Amendment. We review de novo.</p>'
            '<p><span class="star-pagination">*14</span>The district court '
            'erred. See 554 U.S. 570 and 18 U.S.C. § 922(g). We reverse '
            'and remand.</p>'),
        'plain_text': ''}
    opinion.update(changes)
    return opinion


def write_case(
    folder: pathlib.Path,
    cluster: dict[str, Any],
    opinions: list[dict[str, Any]],
    court: str = 'ca1') -> pathlib.Path:
    """Saves a CourtListener case the way the downloaders do."""
    path = courtpy.courtlistener.case_path(folder, court, cluster['id'])
    courtpy.courtlistener.save_case(path, {
        'source': 'court_listener',
        'court': {
            'id': court,
            'full_name': 'Court of Appeals for the First Circuit',
            'citation_string': '1st Cir.',
            'jurisdiction': 'F'},
        'cluster': cluster,
        'docket': None,
        'citations': courtpy.courtlistener.citation_strings(cluster['citations']),
        'opinions': [courtpy.courtlistener.slim_opinion(o) for o in opinions]})
    return path


@pytest.fixture
def court_listener_folder(tmp_path: pathlib.Path) -> pathlib.Path:
    """Returns a folder with three saved CourtListener cases."""
    folder = tmp_path / 'court_listener'
    write_case(folder, make_cluster(1), [make_opinion(11, 1)])
    write_case(
        folder,
        make_cluster(
            2, case_name = 'Smith v. Acme Corp.',
            case_name_full = 'Mary SMITH, Plaintiff-Appellant, v. ACME CORP., Defendant-Appellee',
            disposition = 'Affirmed.', attorneys = 'Pat Lee for appellant.',
            precedential_status = 'Unpublished'),
        [
            make_opinion(21, 2, html_with_citations = (
                '<p>Smith appeals the dismissal of her Title VII claim. We '
                'affirm.</p>')),
            make_opinion(
                22, 2, type = '040dissent', author_str = 'Thompson',
                html_with_citations = '<p>THOMPSON, Circuit Judge, dissenting. I would reverse.</p>')])
    write_case(
        folder,
        make_cluster(3, disposition = '', case_name = 'Brown v. Board',
                     case_name_full = 'Brown v. Board'),
        [make_opinion(31, 3, plain_text = 'We vacate the judgment.', html_with_citations = '')])
    return folder

@pytest.fixture
def recent_folder(tmp_path: pathlib.Path) -> pathlib.Path:
    """Returns a folder with a case like CourtListener's recent ones.

    CourtListener has no full case name, judges, or disposition for it: the
    caption, the panel, and the decision are only in the opinion's text.

    """
    folder = tmp_path / 'recent'
    write_case(
        folder,
        make_cluster(
            4, case_name_full = '', judges = '', panel = [], disposition = '',
            attorneys = ''),
        [make_opinion(41, 4, author_str = '', per_curiam = True, html_with_citations = (
            '<p>[DO NOT PUBLISH] In the United States Court of Appeals For the '
            'First Circuit ____ No. 23-1234 ____</p>'
            '<p>UNITED STATES OF AMERICA, Plaintiﬀ-Appellee, versus JOHN DOE, '
            'Defendant-Appellant.</p>'
            '<p>Appeal from the United States District Court ____</p>'
            '<p>Before LYNCH, Chief Judge, and HOWARD and R. THOMPSON, Circuit '
            'Judges.</p>'
            '<p>PER CURIAM: John Doe appeals his sentence for possession of a '
            'firearm. We will reverse only if the district court abused its '
            'discretion. It did not.</p><p>AFFIRMED.</p>'))])
    return folder



@pytest.fixture
def lexis_folder(tmp_path: pathlib.Path) -> pathlib.Path:
    """Returns a folder with the sample Lexis-Nexis cases, divided."""
    folder = tmp_path / 'lexis_nexis'
    courtpy.lexis.split(FIXTURES / 'lexis_batch.txt', folder)
    return folder


class FakeResponse:
    """A response like those from `requests`."""

    def __init__(
        self,
        payload: Any = None,
        status_code: int = 200,
        headers: dict[str, str] | None = None) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}
        self.text = json.dumps(payload)

    def json(self) -> Any:
        """Returns the payload."""
        return self.payload


class FakeSession:
    """A session that answers requests from a list of handlers."""

    def __init__(self, handler: Any) -> None:
        self.handler = handler
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def get(self, url: str, params: dict[str, Any] | None = None, **kwargs: Any) -> FakeResponse:
        """Records the request and returns the handler's response."""
        self.calls.append((url, params))
        return self.handler(url, params)


class Reply:
    """The response of a web site to a request for a page or a file."""

    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.status = status

    @property
    def text(self) -> str:
        """Returns the content as text."""
        return self.content.decode('utf-8')

    def raise_for_status(self) -> None:
        """Raises an error for a response that failed."""
        if self.status >= 400:
            raise RuntimeError(f'status {self.status}')


class Site:
    """A session that serves pages and files by their addresses."""

    def __init__(self, pages: dict[str, bytes | str], status: int = 200) -> None:
        self.pages = pages
        self.status = status
        self.headers: dict[str, str] = {}
        self.urls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> Reply:
        """Records the request and returns the page or file at an address."""
        self.urls.append(url)
        if url not in self.pages:
            return Reply(b'', 404)
        page = self.pages[url]
        return Reply(page.encode('utf-8') if isinstance(page, str) else page, self.status)


@pytest.fixture(autouse = True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stops a test from downloading anything from a real site."""

    def refuse(self: Any, method: str, url: str, **kwargs: Any) -> None:
        raise AssertionError(f'a test asked the network for {url}')

    monkeypatch.setattr(courtpy.utilities.requests.Session, 'request', refuse)
