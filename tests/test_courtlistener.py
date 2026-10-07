"""Tests the courtlistener module."""

from __future__ import annotations

import json
import pathlib
import urllib.parse
from typing import Any

import pytest
from conftest import FakeResponse, FakeSession, make_cluster, make_opinion

from courtpy import courtlistener

API = 'https://www.courtlistener.com/api/rest/v4/'


@pytest.fixture(autouse = True)
def no_sleeping(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Records pauses instead of sleeping."""
    pauses: list[float] = []
    monkeypatch.setattr(courtlistener.time, 'sleep', pauses.append)
    return pauses


def client(handler: Any) -> courtlistener.CourtListener:
    return courtlistener.CourtListener(
        api_key = 'secret', session = FakeSession(handler), min_interval = 0)


class FakeAPI:
    """Answers cluster, opinion, court, and docket requests like the API."""

    def __init__(self, clusters: list[dict[str, Any]], opinions: list[dict[str, Any]],
                 page_size: int = 2) -> None:
        self.clusters = clusters
        self.opinions = opinions
        self.page_size = page_size

    def __call__(self, url: str, params: dict[str, Any] | None) -> FakeResponse:
        parsed = urllib.parse.urlparse(url)
        query = dict(urllib.parse.parse_qsl(parsed.query))
        query.update(params or {})
        path = parsed.path.split('/rest/v4/')[-1]
        if path.startswith('courts/'):
            return FakeResponse({'id': 'ca1', 'full_name': 'Court of Appeals for the First Circuit'})
        if path.startswith('dockets/'):
            return FakeResponse({'id': 10, 'court_id': 'ca1', 'docket_number': '19-1234'})
        if path == 'clusters/':
            return self._page('clusters/', self.clusters, query)
        if path == 'opinions/':
            low, high = (int(v) for v in query['cluster__id__range'].split(','))
            found = [o for o in self.opinions if low <= o['cluster_id'] <= high]
            return FakeResponse({'next': None, 'results': found})
        if path.startswith('clusters/'):
            cluster_id = int(path.split('/')[1])
            return FakeResponse(next(c for c in self.clusters if c['id'] == cluster_id))
        return FakeResponse({'detail': 'Not found.'}, 404)

    def _page(self, path: str, items: list[dict[str, Any]], query: dict[str, Any]) -> FakeResponse:
        start = int(query.get('cursor', 0))
        page = items[start:start + self.page_size]
        more = start + self.page_size < len(items)
        next_url = f'{API}{path}?cursor={start + self.page_size}' if more else None
        return FakeResponse({'next': next_url, 'results': page})


def test_download_saves_cases_and_resumes(tmp_path: pathlib.Path) -> None:
    clusters = [make_cluster(i) for i in (1, 2, 3)]
    opinions = [make_opinion(10 + i, i) for i in (1, 2, 3)]
    api = client(FakeAPI(clusters, opinions))
    saved = api.download('ca1', '2020-01-01', '2020-12-31', tmp_path)
    assert sorted(p.name for p in saved) == ['1.json', '2.json', '3.json']
    record = json.loads((tmp_path / 'ca1' / '1.json').read_text(encoding = 'utf-8'))
    assert record['court']['full_name'] == 'Court of Appeals for the First Circuit'
    assert record['citations'] == ['950 F.3d 12']
    assert [o['id'] for o in record['opinions']] == [11]
    assert 'plain_text' not in record['opinions'][0]
    first_call = urllib.parse.urlparse(api.session.calls[1][0])
    filters = dict(urllib.parse.parse_qsl(first_call.query))
    assert first_call.path.endswith('/clusters/')
    assert filters['docket__court'] == 'ca1'
    assert filters['date_filed__gte'] == '2020-01-01'
    assert filters['order_by'] == 'id'
    assert api.session.headers['Authorization'] == 'Token secret'
    # Running it again reads only the last page, and saves only new cases.
    again = client(FakeAPI(clusters, opinions))
    assert again.download('ca1', '2020-01-01', '2020-12-31', tmp_path) == []
    assert again.requests_made == 1
    clusters.append(make_cluster(4))
    opinions.append(make_opinion(14, 4))
    later = client(FakeAPI(clusters, opinions))
    assert later.download('ca1', '2020-01-01', '2020-12-31', tmp_path) == [
        tmp_path / 'ca1' / '4.json']
    # One request for the last page and one for the new case's opinions.
    assert later.requests_made == 2


def test_max_cases_resumes_on_the_same_page(tmp_path: pathlib.Path) -> None:
    clusters = [make_cluster(i) for i in (1, 2, 3, 4)]
    opinions = [make_opinion(10 + i, i) for i in (1, 2, 3, 4)]
    fake = FakeAPI(clusters, opinions, page_size = 3)
    first = client(fake).download('ca1', folder = tmp_path, max_cases = 2)
    assert sorted(p.name for p in first) == ['1.json', '2.json']
    second = client(fake).download('ca1', folder = tmp_path)
    assert sorted(p.name for p in second) == ['3.json', '4.json']


def test_rate_limit_waits_then_stops(tmp_path: pathlib.Path, no_sleeping: list[float]) -> None:
    responses = [
        FakeResponse({'detail': 'Request was throttled.'}, 429, {'Retry-After': '30'}),
        FakeResponse({'results': [], 'next': None})]
    api = client(lambda url, params: responses.pop(0))
    assert api.get('clusters/') == {'results': [], 'next': None}
    assert no_sleeping == [30.0]
    daily = client(lambda url, params: FakeResponse(
        {'detail': 'Request was throttled. Expected available in 85774 seconds.'}, 429))
    with pytest.raises(courtlistener.RateLimitError, match = 'resumes where it stopped') as error:
        daily.get('clusters/')
    assert error.value.seconds == 85774


def test_errors() -> None:
    rejected = client(lambda url, params: FakeResponse({'detail': 'Invalid token.'}, 401))
    with pytest.raises(courtlistener.CourtListenerError, match = 'courtpy key show'):
        rejected.get('clusters/')
    missing = client(lambda url, params: FakeResponse({'detail': 'Not found.'}, 404))
    with pytest.raises(courtlistener.CourtListenerError, match = '404'):
        missing.get('clusters/0/')
    flaky = [FakeResponse({}, 503), FakeResponse({'ok': True})]
    assert client(lambda url, params: flaky.pop(0)).get('x/') == {'ok': True}


def test_unfiltered_opinions_raise(tmp_path: pathlib.Path) -> None:
    def handler(url: str, params: dict[str, Any] | None) -> FakeResponse:
        if 'opinions' in url:
            return FakeResponse({'next': None, 'results': [make_opinion(99, 500)]})
        if 'courts' in url:
            return FakeResponse({'id': 'ca1'})
        return FakeResponse({'next': None, 'results': [make_cluster(1)]})
    with pytest.raises(courtlistener.CourtListenerError, match = 'not asked for'):
        client(handler).download('ca1', folder = tmp_path)


def test_download_case(tmp_path: pathlib.Path) -> None:
    api = client(FakeAPI([make_cluster(7)], [make_opinion(70, 7)]))
    path = api.download_case(7, tmp_path)
    assert path == tmp_path / 'ca1' / '7.json'
    case = courtlistener.read_case(path)
    assert case.metadata['court'] == 'ca1'


def test_read_case(court_listener_folder: pathlib.Path) -> None:
    case = courtlistener.read_case(court_listener_folder / 'ca1' / '2.json')
    assert case.id == '2'
    assert case.sections['party'].startswith('Mary SMITH, Plaintiff-Appellant')
    assert case.sections['dissenting_lines'] == 'Thompson'
    assert case.sections['opinion'].startswith('Smith appeals')
    assert 'I would reverse.' in case.sections['opinion']
    assert case.metadata['published'] is False
    assert case.metadata['year'] == 2020
    assert case.metadata['panel_ids'] == [101, 102, 103]
    assert case.metadata['url'] == 'https://www.courtlistener.com/opinion/2/united-states-v-doe/'
    first = courtlistener.read_case(court_listener_folder / 'ca1' / '1.json')
    assert '*14' not in first.sections['opinion']
    assert '§ 922(g)' in first.sections['opinion']


@pytest.mark.parametrize(('kind', 'role'), [
    ('010combined', 'majority'), ('020lead', 'majority'), ('Lead Opinion', 'majority'),
    ('030concurrence', 'concurrence'), ('035concurrenceinpart', 'mixed'),
    ('040dissent', 'dissent'), ('050addendum', 'other'), (None, 'majority')])
def test_opinion_role(kind: str | None, role: str) -> None:
    assert courtlistener.opinion_role(kind) == role


def test_add_opinions_replaces_by_id(tmp_path: pathlib.Path) -> None:
    path = tmp_path / 'case.json'
    courtlistener.save_case(path, {'opinions': [{'id': 1, 'text': 'old'}]})
    courtlistener.add_opinions(path, [{'id': 1, 'text': 'new'}, {'id': 2}])
    record = json.loads(path.read_text(encoding = 'utf-8'))
    assert record['opinions'] == [{'id': 1, 'text': 'new'}, {'id': 2}]
    assert courtlistener.is_complete(path)
    assert not courtlistener.is_complete(tmp_path / 'nothing.json')
