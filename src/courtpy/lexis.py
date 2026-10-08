"""Reads court opinions downloaded from Lexis-Nexis as text files.

Lexis-Nexis lets users download many cases at once in one text file, with a
line such as "3 of 250 DOCUMENTS" before each case. `split` divides such
files into one file for each case, which `read_case` reads for parsing.

Lexis-Nexis cases have a header (with the parties, court, docket number,
dates, history, counsel, judges, and so on) followed by the opinions. The
rules in the "lexis_nexis" rulebook (in courtpy's "instructions" folder) find
each part of the header, so they can be changed without changing courtpy.

Contents:
    clean: removes common clutter from Lexis-Nexis text.
    finish: adds dates and publication status to a parsed case.
    read_case: reads a Lexis-Nexis case for parsing.
    split: divides files of many cases into one file for each case.
    logger: the module's logger, which reports how many cases each file held.
    _DATE_KINDS: words in a line of dates that say what each date is (such
        as "Argued" in "March 3, 2009, Argued"), in capitals. Each becomes a
        column such as "date_argued".
    _DIVIDER: pattern for the line that Lexis-Nexis puts before each case in
        a file of many cases, such as "3 of 250 DOCUMENTS".
    _read_text: returns the text of a file in UTF-8 or Windows-1252.

"""

from __future__ import annotations

import logging
import pathlib
import re
from collections.abc import Iterable, MutableMapping
from typing import Any

from . import cases, utilities

logger = logging.getLogger(__name__)
_DIVIDER: re.Pattern[str] = re.compile(r'\d+ of \d+ DOCUMENTS', re.IGNORECASE)
_DATE_KINDS: tuple[str, ...] = (
    'DECIDED', 'FILED', 'ARGUED', 'SUBMITTED', 'AMENDED')


def clean(text: str) -> str:
    """Removes common clutter from Lexis-Nexis text.

    Star paging ("*123"), bracketed footnote numbers ("[12]"), and the
    "Signal:" and "As of:" lines that Lexis adds are removed, spaces are
    collapsed, and spaces at the starts and ends of lines are trimmed.

    Args:
        text: text of one or more cases.

    Returns:
        The cleaned text.

    """
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    text = text.replace('*', '')
    text = re.sub(r'\[\d*\]', '', text)
    text = re.sub(r'[^\S\n]+', ' ', text)
    text = re.sub(r'(?im)^ ?(?:signal|as of):.*\n?\n?', '', text)
    return re.sub(r' *\n *', '\n', text)


def finish(row: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Adds dates and publication status to a parsed Lexis-Nexis case.

    The "dates" section has each line of the header with a date, such as
    "March 3, 2009, Argued". Each kind of date (decided, filed, argued,
    submitted, and amended) is stored as an ISO date in its own column
    ("date_decided" and so on). "date_filed" is the date filed or, if there is
    none, the date decided, as on CourtListener. "year" is the year of that
    date (or of the first date found).

    A case is published if it has a Federal Reporter citation and no notice
    that it is unpublished (as found by the federal rules).

    Args:
        row: a parsed case.

    Returns:
        The same `row`, with the new columns.

    """
    lines = str(row.get('dates') or '').splitlines()
    for kind in _DATE_KINDS:
        column = f'date_{kind.lower()}'
        row.setdefault(column, None)
        for line in lines:
            if kind in line.upper():
                row[column] = utilities.parse_date(line)
                break
    if not row.get('date_filed'):
        row['date_filed'] = row.get('date_decided')
    first = row.get('date_filed') or next(
        (d for d in map(utilities.parse_date, lines) if d), None)
    row['year'] = int(first[:4]) if first else None
    if 'cite_federal_reporter' in row:
        row['published'] = bool(row['cite_federal_reporter']) and not bool(
            row.get('notice_unpublished'))
    return row


def read_case(path: pathlib.Path | str) -> cases.Case:
    """Reads a Lexis-Nexis case for parsing.

    Args:
        path: path of a text file with one case.

    Returns:
        The case. Its only section is "text" (the whole case, cleaned), which
            the "lexis_nexis" rules divide into the header and opinions.

    """
    path = pathlib.Path(path)
    text = clean(_read_text(path)).strip()
    # Line breaks at each end let rules look for a line at the start or end.
    return cases.Case(
        id = path.stem,
        source = 'lexis_nexis',
        sections = {'text': f'\n{text}\n'},
        metadata = {'source': 'lexis_nexis', 'file': path.name},
        path = path)


def split(
    files: Iterable[pathlib.Path | str] | pathlib.Path | str,
    folder: pathlib.Path | str = 'lexis_nexis') -> list[pathlib.Path]:
    """Divides files of many cases into one file for each case.

    Args:
        files: text files downloaded from Lexis-Nexis, or a folder of them.
        folder: folder to save the cases in. Each is named for the file it came
            from and its place in that file (such as "batch1_00003.txt").
            Defaults to "lexis_nexis".

    Returns:
        The paths of the new files.

    """
    if isinstance(files, str | pathlib.Path):
        source = pathlib.Path(files)
        files = sorted(source.glob('*.txt')) if source.is_dir() else [source]
    folder = pathlib.Path(folder)
    folder.mkdir(parents = True, exist_ok = True)
    saved = []
    for file in files:
        file = pathlib.Path(file)  # noqa: PLW2901
        pieces = [clean(p).strip() for p in _DIVIDER.split(_read_text(file))]
        number = 0
        for piece in pieces:
            if not piece:
                continue
            number += 1
            path = folder / f'{file.stem}_{number:05d}.txt'
            path.write_text(piece + '\n', encoding = 'utf-8')
            saved.append(path)
        logger.info('%s: %d cases', file.name, number)
    return saved


""" Private Functions """


def _read_text(path: pathlib.Path) -> str:
    """Returns the text of a file in UTF-8 or, failing that, Windows-1252.

    Lexis-Nexis has saved downloads in both encodings. Text that is not valid
    UTF-8 is read as Windows-1252, with any byte that is not valid there
    either replaced by a placeholder character.

    Args:
        path: path of a text file.

    Returns:
        The text, without a UTF-8 byte order mark if the file had one.

    """
    data = path.read_bytes()
    try:
        return data.decode('utf-8-sig')
    except UnicodeDecodeError:
        return data.decode('cp1252', errors = 'replace')
