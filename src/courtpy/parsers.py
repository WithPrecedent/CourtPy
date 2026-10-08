"""Turns saved court opinions into a table, one row for each case.

Parsing has three stages:

1. A reader for the source (CourtListener or Lexis-Nexis) makes a `Case`
   with sections of text (such as "party", "judges", and "opinion") and
   information that needs no parsing (such as CourtListener's date filed).
2. For Lexis-Nexis, the "lexis_nexis" rules find the sections of the header
   (see `courtpy.lexis`). CourtListener's sections come from its data.
3. The rules of the jurisdiction (by default, the "federal" rules) find
   variables in the sections: the parties' roles, the court, the
   disposition, the judges, the issues discussed in the opinion, and so on.

Every section except those whose names end in "_lines" is collapsed onto one
line between stages 2 and 3, so that phrases broken across lines are found.

Contents:
    SOURCES: the sources that courtpy can read.
    Parser: parses cases with rulebooks.
    Source: how to read and parse one source of opinions.
    find_files: returns the saved cases of a source in a folder.
    parse: parses a folder of saved cases into a table.
    logger: the module's logger, which reports parsing progress and cases
        that could not be parsed.
    _derive: adds columns that summarize others to a parsed case.
    _parse_all: parses each saved case, in one process or several.

"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import logging
import pathlib
from collections.abc import Callable, Iterable, MutableMapping, Sequence
from typing import Any

import pandas as pd

from . import cases, courtlistener, lexis, options, rules, utilities

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen = True)
class Source:
    """How to read and parse one source of opinions.

    Args:
        name: the name of the source, such as "court_listener".
        pattern: the file pattern of its saved cases, such as "*.json".
        read: a function that reads a saved case into a `Case`.
        structure: the rulebook that finds the sections of its cases, if they
            need finding. Defaults to `None`.
        finish: a function that adds columns to a parsed case (a `dict`)
            after the rules are applied. Defaults to `None`.

    """

    name: str
    pattern: str
    read: Callable[[pathlib.Path], cases.Case]
    structure: str | None = None
    finish: Callable[[MutableMapping[str, Any]], Any] | None = None


SOURCES: dict[str, Source] = {
    'court_listener': Source(
        name = 'court_listener',
        pattern = '*.json',
        read = courtlistener.read_case),
    'lexis_nexis': Source(
        name = 'lexis_nexis',
        pattern = '*.txt',
        read = lexis.read_case,
        structure = 'lexis_nexis',
        finish = lexis.finish)}


@dataclasses.dataclass
class Parser:
    """Parses cases with rulebooks.

    Args:
        rulebook: the rules of the jurisdiction, applied to every case.
        structures: the rules that find the sections of each source's cases,
            by the name of the source. Defaults to an empty `dict`, in which
            case they are loaded when first needed.
        keep_text: whether to keep the text of the opinions (and of the
            Lexis-Nexis header) in the table. Defaults to `False`.

    """

    rulebook: rules.Rulebook
    structures: dict[str, rules.Rulebook] = dataclasses.field(
        default_factory = dict)
    keep_text: bool = False

    """ Class Methods """

    @classmethod
    def create(
        cls,
        rulebooks: str | pathlib.Path | Sequence[str | pathlib.Path] = (
            options._DEFAULT_JURISDICTION),
        *,
        keep_text: bool = False) -> Parser:
        """Returns a parser with the rules in `rulebooks`.

        Args:
            rulebooks: names of built-in rulebooks (such as "federal") or
                paths to CSV files or folders of them, applied in order.
                Defaults to `options._DEFAULT_JURISDICTION`.
            keep_text: whether to keep the text of the opinions in the table.
                Defaults to `False`.

        Returns:
            The parser.

        """
        if isinstance(rulebooks, str | pathlib.Path):
            rulebooks = utilities.listify(str(rulebooks)) if isinstance(
                rulebooks, str) else [rulebooks]
        return cls(
            rulebook = rules.Rulebook.load(*rulebooks), keep_text = keep_text)

    """ Public Methods """

    def parse(self, case: cases.Case) -> dict[str, Any]:
        """Returns one row of the table for `case`.

        Args:
            case: the case to parse.

        Returns:
            The case's variables, by the names of their columns. Information
                from the case's `metadata` comes first and is used instead of
                any variable of the same name found by the rules (unless it is
                missing).

        """
        sections = dict(case.sections)
        found: dict[str, Any] = {}
        source = SOURCES.get(case.source)
        if source is not None and source.structure:
            found.update(self._structure(source.structure).apply(sections))
        for name, text in list(sections.items()):
            if not name.endswith('_lines'):
                sections[name] = utilities.normalize(text)
        found.update(self.rulebook.apply(sections))
        row: dict[str, Any] = dict(case.metadata)
        for name, value in found.items():
            if row.get(name) is None:
                row[name] = value
        if source is not None and source.finish is not None:
            source.finish(row)
        _derive(row, sections)
        for name in list(row):
            if name in options._LARGE_SECTIONS or name.endswith('_lines'):
                del row[name]
        if self.keep_text:
            row['header'] = sections.get('header')
            row['opinion'] = sections.get('opinion')
        return row

    def parse_file(
        self,
        path: pathlib.Path,
        source: str) -> tuple[str, dict[str, Any]]:
        """Reads and parses one saved case.

        Args:
            path: path of the saved case.
            source: the name of its source, such as "court_listener".

        Returns:
            The case's id and its row of the table.

        """
        case = SOURCES[source].read(path)
        return case.id, self.parse(case)

    """ Private Methods """

    def _structure(self, name: str) -> rules.Rulebook:
        """Returns the rulebook that finds the sections of a source's cases.

        The rulebook is loaded the first time it is needed and kept in
        `structures`, so its CSV files are read only once.

        Args:
            name: the name of a built-in rulebook or the path to a CSV file
                (a `Source`'s `structure`, such as "lexis_nexis").

        Returns:
            The rulebook.

        """
        if name not in self.structures:
            self.structures[name] = rules.Rulebook.load(name)
        return self.structures[name]


def find_files(
    folder: pathlib.Path | str,
    source: str = 'court_listener') -> list[pathlib.Path]:
    """Returns the saved cases of a source in a folder and its subfolders.

    Args:
        folder: the folder.
        source: the name of the source. Defaults to "court_listener".

    Raises:
        ValueError: if `source` is not in `SOURCES`.

    Returns:
        The paths, sorted. Files in hidden folders (such as ".courtpy", where
            downloads keep their place) are left out.

    """
    if source not in SOURCES:
        message = f'source must be one of {list(SOURCES)}, not {source!r}'
        raise ValueError(message)
    folder = pathlib.Path(folder)
    return sorted(
        p for p in folder.rglob(SOURCES[source].pattern)
        if p.is_file() and not any(
            part.startswith('.') for part in p.relative_to(folder).parts))


def parse(
    files: pathlib.Path | str | Iterable[pathlib.Path | str],
    source: str = 'court_listener',
    rulebooks: str | pathlib.Path | Sequence[str | pathlib.Path] = (
        options._DEFAULT_JURISDICTION),
    *,
    keep_text: bool = False,
    limit: int | None = None,
    workers: int = 1) -> pd.DataFrame:
    """Parses saved cases into a table, one row for each case.

    Args:
        files: a folder of saved cases (searched with its subfolders), or the
            paths of saved cases.
        source: where the cases came from: "court_listener" (the default) or
            "lexis_nexis".
        rulebooks: names of built-in rulebooks (such as "federal") or paths to
            CSV files or folders of them. Defaults to
            `options._DEFAULT_JURISDICTION`.
        keep_text: whether to keep the text of the opinions in the table.
            Defaults to `False`.
        limit: most cases to parse, for trying out rules. Defaults to `None`
            (every case).
        workers: number of processes to parse with. Defaults to 1.

    Returns:
        The table, labeled by each case's id ("case_id"). A case that cannot
            be read is left out, with a warning.

    """
    if source not in SOURCES:
        message = f'source must be one of {list(SOURCES)}, not {source!r}'
        raise ValueError(message)
    if isinstance(files, str | pathlib.Path):
        paths = find_files(files, source)
    else:
        paths = [pathlib.Path(p) for p in files]
    if limit is not None:
        paths = paths[:limit]
    parser = Parser.create(rulebooks, keep_text = keep_text)
    rows: dict[str, dict[str, Any]] = {}
    failures = 0
    results = _parse_all(parser, paths, source, workers)
    for path, outcome in zip(paths, results, strict = True):
        if isinstance(outcome, BaseException):
            failures += 1
            logger.warning('%s could not be parsed: %s', path, outcome)
            continue
        case_id, row = outcome
        unique, number = case_id, 1
        while unique in rows:
            number += 1
            unique = f'{case_id}_{number}'
        rows[unique] = row
    logger.info('parsed %d cases (%d could not be parsed)', len(rows), failures)
    data = pd.DataFrame.from_dict(rows, orient = 'index')
    data.index.name = 'case_id'
    return cases.tidy(data)


""" Private Functions """


def _derive(row: MutableMapping[str, Any], sections: dict[str, str]) -> None:
    """Adds columns that summarize others to a parsed case.

    The columns are "author" (the first of "authors", unless the case's
    metadata already gave one), "panel_size" (the number of judges named, or
    of CourtListener's panel ids if there are more of them), "concurrences"
    and "dissents" (the number of judges who concurred or dissented), and
    "word_count" (the number of words in the opinions).

    Args:
        row: a parsed case. It is changed in place.
        sections: its sections of text, after the rules were applied.

    """
    authors = row.get('authors') or []
    row.setdefault('author', authors[0] if authors else None)
    judges = row.get('panel_judges') or []
    panel_ids = row.get('panel_ids') or []
    row['panel_size'] = max(len(judges), len(panel_ids))
    row['concurrences'] = len(row.get('concurring') or [])
    row['dissents'] = len(row.get('dissenting') or [])
    row['word_count'] = len(sections.get('opinion', '').split())


def _parse_all(
    parser: Parser,
    paths: Sequence[pathlib.Path],
    source: str,
    workers: int) -> list[Any]:
    """Parses each saved case, in one process or several.

    An error while reading or parsing one case does not stop the others: the
    error takes the place of the case's result. With one worker (or one
    case), the cases are parsed in this process, and progress is logged every
    1,000 cases. Otherwise, they are parsed in a pool of `workers` processes.

    Args:
        parser: the parser to use. It is copied to each process.
        paths: the paths of the saved cases.
        source: the name of their source, such as "court_listener".
        workers: number of processes to parse with.

    Returns:
        For each path, in order, the case's id and row (see
            `Parser.parse_file`) or the error that parsing it raised.

    """
    if workers <= 1 or len(paths) < 2:
        results: list[Any] = []
        for number, path in enumerate(paths, start = 1):
            try:
                results.append(parser.parse_file(path, source))
            except Exception as error:  # noqa: BLE001
                results.append(error)
            if number % 1000 == 0:
                logger.info('%d of %d cases parsed', number, len(paths))
        return results
    with concurrent.futures.ProcessPoolExecutor(max_workers = workers) as pool:
        futures = [pool.submit(parser.parse_file, p, source) for p in paths]
        outcomes: list[Any] = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except Exception as error:  # noqa: BLE001
                outcomes.append(error)
        return outcomes
