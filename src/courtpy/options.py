"""Default settings for courtpy.

These are module-level constants so that a project built on `courtpy` can
change them before they are used (for example,
`courtpy.options._MIN_INTERVAL = 2.0` to slow down requests to the
CourtListener API).

Contents:
    _API_URL: base address of the CourtListener REST API (version 4).
    _BULK_PREFIX: folder of the CourtListener bulk data in its bucket.
    _BULK_URL: address of the public bucket with CourtListener's bulk data.
    _COURT_GROUPS: names for groups of CourtListener court ids.
    _DEFAULT_CODERS: techniques that code variables after cases are parsed.
    _DEFAULT_JURISDICTION: rulebooks used when none are named.
    _DOCKET_FIELDS: docket fields requested from the CourtListener API.
    _ENV_API_KEY: environment variable that can hold the API key.
    _ENV_CONFIG: environment variable that can name the configuration folder.
    _ENV_DATA: environment variable that can name the data folder.
    _KEYRING_SERVICE: name under which the API key is kept in a keyring.
    _KEYRING_USERNAME: user name under which the API key is kept.
    _LARGE_SECTIONS: sections of text that are not kept in a case table
        unless asked.
    _MAX_WAIT: most seconds to wait when the API asks for a pause.
    _MIN_INTERVAL: fewest seconds between requests to the API.
    _OPINION_FIELDS: opinion fields requested from the CourtListener API.
    _SOURCES: sources of court opinions that courtpy can read.
    _TEXT_FIELDS: CourtListener fields with the text of an opinion, in the
        order they are preferred.
    _TIMEOUT: seconds to wait for a response from a server.
    _USER_AGENT: how courtpy identifies itself to servers.

"""

from __future__ import annotations

_API_URL: str = 'https://www.courtlistener.com/api/rest/v4/'
_BULK_PREFIX: str = 'bulk-data/'
_BULK_URL: str = (
    'https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/')
# Names that can be used in place of a list of CourtListener court ids.
_COURT_GROUPS: dict[str, tuple[str, ...]] = {
    'federal_appellate': (
        'ca1', 'ca2', 'ca3', 'ca4', 'ca5', 'ca6', 'ca7', 'ca8', 'ca9', 'ca10',
        'ca11', 'cadc', 'cafc'),
    'federal_circuits': (
        'ca1', 'ca2', 'ca3', 'ca4', 'ca5', 'ca6', 'ca7', 'ca8', 'ca9', 'ca10',
        'ca11', 'cadc'),
    'supreme_court': ('scotus',)}
# Coders are run in this order because each uses columns made by the ones
# before it.
_DEFAULT_CODERS: tuple[str, ...] = (
    'code_parties', 'code_case_type', 'code_outcome')
_DEFAULT_JURISDICTION: str = 'federal'
_DOCKET_FIELDS: tuple[str, ...] = (
    'id', 'court_id', 'docket_number', 'case_name_full', 'date_argued',
    'date_filed', 'date_terminated', 'appeal_from_str', 'assigned_to_str',
    'panel_str', 'nature_of_suit', 'cause')
_ENV_API_KEY: str = 'COURTLISTENER_API_KEY'
_ENV_CONFIG: str = 'COURTPY_CONFIG_DIR'
_ENV_DATA: str = 'COURTPY_DATA_DIR'
_KEYRING_SERVICE: str = 'courtpy'
_KEYRING_USERNAME: str = 'courtlistener'
# Sections of text so long that they are only kept in a case table if
# "keep_text" is true. The opinion's words are still counted and searched.
_LARGE_SECTIONS: tuple[str, ...] = (
    'text', 'header', 'opinion', 'opinion_lines')
# A free CourtListener account allows 5 requests a minute, 50 an hour, and
# 125 a day (as of 2026). Waiting up to an hour rides out the first two
# limits. A longer pause (the daily limit) stops the download, which can be
# resumed later.
_MAX_WAIT: float = 3600.0
_MIN_INTERVAL: float = 1.0
_OPINION_FIELDS: tuple[str, ...] = (
    'id', 'cluster_id', 'type', 'author_id', 'author_str', 'per_curiam',
    'joined_by_str', 'ordering_key', 'download_url', 'html_with_citations',
    'plain_text')
_SOURCES: tuple[str, ...] = ('court_listener', 'lexis_nexis')
# CourtListener recommends "html_with_citations" as the most reliable text.
_TEXT_FIELDS: tuple[str, ...] = (
    'html_with_citations', 'html_columbia', 'html_lawbox', 'xml_harvard',
    'html_anon_2020', 'html', 'plain_text')
_TIMEOUT: float = 60.0
_USER_AGENT: str = 'courtpy (https://github.com/WithPrecedent/courtpy)'
