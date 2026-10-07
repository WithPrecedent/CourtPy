"""Shared tools.

Contents:
    config_folder: returns the folder for courtpy's settings and secrets.
    data_folder: returns the folder for large downloads, such as bulk data.
    expand_courts: turns court ids and group names into court ids.
    html_to_text: converts the HTML or XML of an opinion to plain text.
    listify: returns a setting as a list of names.
    normalize: collapses all whitespace in text into single spaces.
    parse_date: returns an ISO date (YYYY-MM-DD) from a date in text.
    to_bool: converts a setting or a CSV cell to a boolean.

"""

from __future__ import annotations

import datetime
import html.parser
import os
import pathlib
import re
import sys
from collections.abc import Iterable
from typing import Any

from . import options

# Tags that start a new line when HTML or XML is converted to text.
_BLOCKS: frozenset[str] = frozenset({
    'address', 'article', 'author', 'blockquote', 'br', 'caption', 'center',
    'dd', 'div', 'dl', 'dt', 'footnote', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
    'header', 'hr', 'li', 'ol', 'opinion', 'p', 'parties', 'pre', 'section',
    'table', 'td', 'th', 'tr', 'ul'})
# Tags whose contents are page numbers or code, not words of the opinion.
_SKIPPED: frozenset[str] = frozenset({'page-number', 'script', 'style'})
# Classes of elements whose contents are page numbers (star pagination).
_SKIPPED_CLASSES: frozenset[str] = frozenset({'star-pagination', 'page-label'})
# Tags that have no closing tag.
_VOID: frozenset[str] = frozenset({
    'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link',
    'meta', 'source', 'track', 'wbr'})
_MONTHS: dict[str, int] = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6, 'JUL': 7,
    'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
# A date such as "January 5, 2010", "Jan. 5, 2010", or "Sept. 5 2010".
_WRITTEN_DATE: re.Pattern[str] = re.compile(
    r'\b(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[A-Z]*\.?\s+'
    r'(\d{1,2}),?\s+(\d{4})\b',
    flags = re.IGNORECASE)
_ISO_DATE: re.Pattern[str] = re.compile(r'\b(\d{4})-(\d{2})-(\d{2})\b')


class _TextExtractor(html.parser.HTMLParser):
    """Collects the text of HTML or XML, with line breaks between blocks."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs = True)
        self.parts: list[str] = []
        # Open tags, each with whether its contents are skipped.
        self.stack: list[tuple[str, bool]] = []
        self.preformatted = 0

    @property
    def skipping(self) -> bool:
        """Returns whether the current element's text is being skipped."""
        return any(skip for _, skip in self.stack)

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]]) -> None:
        """Records an opening tag."""
        classes = set((dict(attrs).get('class') or '').split())
        skip = tag in _SKIPPED or bool(classes & _SKIPPED_CLASSES)
        if tag in _BLOCKS:
            self.parts.append('\n')
        if tag == 'pre':
            self.preformatted += 1
        if tag not in _VOID:
            self.stack.append((tag, skip))

    def handle_endtag(self, tag: str) -> None:
        """Records a closing tag, closing any tags left open inside it."""
        if tag in _BLOCKS:
            self.parts.append('\n')
        if tag == 'pre' and self.preformatted:
            self.preformatted -= 1
        names = [name for name, _ in self.stack]
        if tag in names:
            # Unclosed tags inside this one are closed with it.
            position = len(names) - 1 - names[::-1].index(tag)
            del self.stack[position:]

    def handle_data(self, data: str) -> None:
        """Records text that is not skipped."""
        if not self.skipping:
            self.parts.append(data)


def config_folder() -> pathlib.Path:
    """Returns the folder for courtpy's settings and secrets.

    The folder is outside of any project or package, so that a secret (such as
    the CourtListener API key) is never saved in a project's files or shared
    with them. It is the folder named by the `COURTPY_CONFIG_DIR` environment
    variable, if it is set. Otherwise, it is "courtpy" in the user's
    application data folder on Windows ("%APPDATA%"), or in
    "~/.config" (or "$XDG_CONFIG_HOME") on other systems.

    Returns:
        The path of the folder. It is not created.

    """
    named = os.environ.get(options._ENV_CONFIG)
    if named:
        return pathlib.Path(named).expanduser()
    if sys.platform == 'win32':
        base = os.environ.get('APPDATA') or pathlib.Path.home() / 'AppData' / 'Roaming'
        return pathlib.Path(base) / 'courtpy'
    base = os.environ.get('XDG_CONFIG_HOME') or pathlib.Path.home() / '.config'
    return pathlib.Path(base) / 'courtpy'


def data_folder() -> pathlib.Path:
    """Returns the folder for large downloads, such as bulk data.

    Bulk data files are very large (the opinions alone are more than 50 GB),
    so they are kept outside of projects (which are often in folders that are
    backed up or synced to the cloud). The folder is the one named by the
    `COURTPY_DATA_DIR` environment variable, if it is set. Otherwise, it is
    "courtpy" in the user's local application data folder on Windows
    ("%LOCALAPPDATA%"), or in "~/.local/share" (or "$XDG_DATA_HOME") on other
    systems.

    Returns:
        The path of the folder. It is not created.

    """
    named = os.environ.get(options._ENV_DATA)
    if named:
        return pathlib.Path(named).expanduser()
    if sys.platform == 'win32':
        base = os.environ.get('LOCALAPPDATA') or pathlib.Path.home() / 'AppData' / 'Local'
        return pathlib.Path(base) / 'courtpy'
    base = os.environ.get('XDG_DATA_HOME') or pathlib.Path.home() / '.local' / 'share'
    return pathlib.Path(base) / 'courtpy'


def expand_courts(courts: str | Iterable[str] | None) -> list[str]:
    """Turns court ids and names of groups of courts into court ids.

    Args:
        courts: CourtListener court ids (such as "ca1" or "scotus") or names of
            groups in `options._COURT_GROUPS` (such as "federal_appellate"),
            as a list or a comma-separated `str`.

    Raises:
        ValueError: if no courts are named.

    Returns:
        The court ids, in order, without repeats.

    """
    found: list[str] = []
    for name in listify(courts):
        for court in options._COURT_GROUPS.get(name.lower(), (name.lower(),)):
            if court not in found:
                found.append(court)
    if not found:
        message = 'name at least one court (such as "ca1") or group of courts'
        raise ValueError(message)
    return found


def html_to_text(markup: str) -> str:
    """Converts the HTML or XML of an opinion to plain text.

    Paragraphs and other blocks are put on their own lines, character
    references (such as "&sect;") become characters, and page numbers inserted
    by publishers ("star pagination") are removed.

    Args:
        markup: HTML or XML. Text without tags is returned unchanged, except
            that character references are converted.

    Returns:
        The text, with blank lines removed and spaces trimmed from each line.

    """
    if '<' not in markup:
        return html.unescape(markup)
    extractor = _TextExtractor()
    extractor.feed(markup)
    extractor.close()
    lines = ''.join(extractor.parts).splitlines()
    return '\n'.join(
        ' '.join(line.split()) for line in lines if line.strip())


def listify(item: Any) -> list[str]:
    """Returns a setting as a list of names.

    Args:
        item: `None`, a `str` (which may list names separated by commas), or an
            iterable of names.

    Returns:
        The names, with surrounding spaces removed and blank names dropped.

    """
    if item is None:
        return []
    if isinstance(item, str):
        item = item.split(',')
    elif not isinstance(item, Iterable):
        item = [item]
    return [str(name).strip() for name in item if str(name).strip()]


def normalize(text: str | None) -> str:
    """Collapses all whitespace in `text` into single spaces.

    Args:
        text: text to normalize. `None` becomes an empty `str`.

    Returns:
        The text on one line, with single spaces between words.

    """
    return ' '.join(text.split()) if text else ''


def parse_date(text: str | None) -> str | None:
    """Returns the first date in `text` as an ISO date (YYYY-MM-DD).

    Args:
        text: text with a date written out (such as "March 3, 2010" or
            "Mar. 3, 2010") or in ISO format ("2010-03-03").

    Returns:
        The date, or `None` if there is no valid date in `text`.

    """
    if not text:
        return None
    match = _ISO_DATE.search(text)
    if match:
        year, month, day = (int(part) for part in match.groups())
    else:
        match = _WRITTEN_DATE.search(text)
        if match is None:
            return None
        month = _MONTHS[match.group(1).upper()[:3]]
        day, year = int(match.group(2)), int(match.group(3))
    try:
        return datetime.date(year, month, day).isoformat()
    except ValueError:
        return None


def to_bool(value: Any) -> bool:
    """Converts a setting or a CSV cell to a boolean.

    Args:
        value: a `bool`, a number, or text such as "TRUE", "yes", "1", "t",
            "FALSE", "no", "0", or "f" (in any case). Blank text is `False`.

    Raises:
        ValueError: if `value` is text that is not one of those.

    Returns:
        The boolean value.

    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int | float):
        return bool(value)
    text = str(value).strip().lower()
    if text in {'true', 't', 'yes', 'y', '1'}:
        return True
    if text in {'false', 'f', 'no', 'n', '0', ''}:
        return False
    message = f'{value!r} is not TRUE or FALSE'
    raise ValueError(message)
