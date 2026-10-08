"""Rules that find information in the text of court opinions.

The rules are written in CSV files (spreadsheets), so they can be read and
changed without knowing Python. Each row is one rule. The columns are:

| Column | Meaning |
| --- | --- |
| `target` | The section of text to search, such as "opinion", "party", or "disposition". Several sections can be listed, separated by commas, and are searched in order. |
| `variable` | The name of the column that the rule makes (or adds to). |
| `kind` | What the rule does (see below). |
| `pattern` | A regular expression to search for. |
| `value` | For a `label` rule, the value to record. Otherwise blank. |
| `ignorecase` | TRUE to ignore the difference between capital and lower-case letters. |
| `dotall` | TRUE for "." in the pattern to also match line breaks. |
| `note` | Anything you want to say about the rule. It is not used. |

The kinds of rules:

| Kind | Makes |
| --- | --- |
| `flag` | TRUE if the pattern appears, and otherwise FALSE. |
| `count` | The number of times the pattern appears. |
| `matches` | A list of every match. |
| `label` | The rule's `value` if the pattern appears. The first rule for a variable that matches wins, so it can turn text into categories (such as the name of a court into its number). |
| `names` | A list of names, found by splitting the text wherever the pattern matches. The pattern should match the words and punctuation between names (such as "JUDGES", "BEFORE", and commas). |
| `section` | A new section of text: the first match. Later rules can search it. |
| `excerpts` | A new section of text: every match, one per line. |
| `split` | Two new sections, named in `variable` (such as "party1, party2"): the text before and after the first match. |
| `remove` | Nothing. It deletes every match from the target for the rules after it. |

Rules are applied from top to bottom, so a rule can search a section made by
a rule above it. Rules with the same `variable` are combined: flags are TRUE if
any rule matches, counts are added, lists of matches and names are joined, and
the first `label` or `section` that matches is used.

Contents:
    KINDS: the kinds of rules.
    Rule: one rule from a CSV file.
    Rulebook: an ordered collection of rules.
    find_instructions: returns the CSV files for a built-in or custom rulebook.
    split_names: splits text into names wherever a pattern matches.
    _BUILT_IN: the CSV files of each built-in rulebook that is made of
        several files, by its name, in the order they are applied. Any other
        name is a file or folder in courtpy's "instructions" folder.
    _PUNCTUATION: pattern for the periods and apostrophes (straight and curly)
        that `split_names` removes from names.
    _REQUIRED: columns that every rules file must have. "value" and "note"
        are optional.
    _SECTION_KINDS: kinds of rules that make new sections of text, which later
        rules can search.
    _convert: returns a cell of a CSV file as a number if it is one.
    _read: returns the rules in a CSV file.
    _unique: returns names in order, without repeats.

"""

from __future__ import annotations

import csv
import dataclasses
import importlib.resources
import pathlib
import re
from collections.abc import (
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    Sequence,
)
from typing import Any

from . import utilities

KINDS: tuple[str, ...] = (
    'count', 'excerpts', 'flag', 'label', 'matches', 'names', 'remove',
    'section', 'split')
_SECTION_KINDS: frozenset[str] = frozenset({'excerpts', 'section', 'split'})
# The curly apostrophe (U+2019) is added with `chr` so that the source file
# has only plain characters.
_PUNCTUATION: re.Pattern[str] = re.compile("[.'" + chr(0x2019) + ']')
_REQUIRED: tuple[str, ...] = (
    'target', 'variable', 'kind', 'pattern', 'ignorecase', 'dotall')
_BUILT_IN: dict[str, tuple[str, ...]] = {
    'federal': ('federal/header.csv', 'federal/opinion.csv')}


@dataclasses.dataclass(frozen = True)
class Rule:
    """One rule from a CSV file.

    Args:
        targets: names of the sections of text to search, in order.
        variables: names of the columns (or, for `section`, `excerpts`, and
            `split` rules, the sections) that the rule makes. Only a `split`
            rule has more than one.
        kind: one of `KINDS`.
        pattern: the compiled regular expression.
        value: for a `label` rule, the value to record. Defaults to `None`.
        note: a description of the rule. Defaults to an empty `str`.

    """

    targets: tuple[str, ...]
    variables: tuple[str, ...]
    kind: str
    pattern: re.Pattern[str]
    value: Any = None
    note: str = ''

    """ Class Methods """

    @classmethod
    def create(cls, row: Mapping[str, Any]) -> Rule:
        """Returns a rule made from a row of a CSV file.

        Args:
            row: the cells of the row, by the names of the columns.

        Raises:
            ValueError: if the kind is not one of `KINDS`, the pattern is not
                a valid regular expression, a cell that is needed is blank, or
                a `split` rule does not name two variables.

        Returns:
            The rule.

        """
        kind = str(row.get('kind') or '').strip().lower()
        if kind not in KINDS:
            message = f'the kind {kind!r} is not one of {list(KINDS)}'
            raise ValueError(message)
        targets = tuple(utilities.listify(row.get('target')))
        variables = tuple(utilities.listify(row.get('variable')))
        if not targets:
            message = 'the target is blank'
            raise ValueError(message)
        if kind == 'split' and len(variables) != 2:
            message = (
                'a split rule needs two variables, separated by a comma (such '
                'as "party1, party2")'
            )
            raise ValueError(message)
        if kind != 'remove' and not variables:
            message = 'the variable is blank'
            raise ValueError(message)
        source = str(row.get('pattern') or '')
        if not source:
            message = 'the pattern is blank'
            raise ValueError(message)
        flags = 0
        if utilities.to_bool(row.get('ignorecase')):
            flags |= re.IGNORECASE
        if utilities.to_bool(row.get('dotall')):
            flags |= re.DOTALL
        try:
            pattern = re.compile(source, flags)
        except re.error as error:
            message = f'the pattern {source!r} is not valid: {error}'
            raise ValueError(message) from error
        value = row.get('value')
        return cls(
            targets = targets,
            variables = variables,
            kind = kind,
            pattern = pattern,
            value = _convert(value) if kind == 'label' else None,
            note = str(row.get('note') or ''))

    """ Properties """

    @property
    def variable(self) -> str:
        """Returns the name of the (first) variable that the rule makes.

        Returns:
            The first of `variables`, or an empty `str` for a `remove` rule,
                which makes none.

        """
        return self.variables[0] if self.variables else ''

    """ Public Methods """

    def apply(
        self,
        sections: MutableMapping[str, str],
        results: MutableMapping[str, Any]) -> None:
        """Applies the rule to `sections`, recording what it finds in `results`.

        Args:
            sections: the sections of text, by name. `remove`, `section`,
                `excerpts`, and `split` rules change it.
            results: the variables found so far, by name. It is changed in
                place.

        """
        texts = [sections[t] for t in self.targets if sections.get(t)]
        if self.kind == 'remove':
            for target in self.targets:
                if sections.get(target):
                    sections[target] = self.pattern.sub(' ', sections[target])
        elif self.kind == 'split':
            self._split(texts, sections, results)
        elif self.kind == 'flag':
            flagged = any(self.pattern.search(text) for text in texts)
            results[self.variable] = bool(results.get(self.variable)) or flagged
        elif self.kind == 'count':
            counted = sum(
                1 for text in texts for _ in self.pattern.finditer(text))
            results[self.variable] = results.get(self.variable, 0) + counted
        elif self.kind == 'matches':
            matches = results.setdefault(self.variable, [])
            matches.extend(
                m.group(0).strip() for t in texts
                for m in self.pattern.finditer(t))
        elif self.kind == 'label':
            if results.get(self.variable) is None:
                matched = any(self.pattern.search(text) for text in texts)
                results[self.variable] = self.value if matched else None
        elif self.kind == 'names':
            names = results.setdefault(self.variable, [])
            for text in texts:
                names.extend(n for n in split_names(text, self.pattern)
                             if n not in names)
        elif self.kind == 'section':
            if not results.get(self.variable):
                match = next(
                    (m for t in texts if (m := self.pattern.search(t))), None)
                section = match.group(0).strip() if match else None
                results[self.variable] = section
                if section:
                    sections[self.variable] = section
        elif self.kind == 'excerpts':
            excerpts = [
                m.group(0).strip() for t in texts
                for m in self.pattern.finditer(t) if m.group(0).strip()]
            existing = results.get(self.variable)
            joined = '\n'.join(([existing] if existing else []) + excerpts)
            results[self.variable] = joined or None
            if joined:
                sections[self.variable] = joined

    """ Private Methods """

    def _split(
        self,
        texts: Sequence[str],
        sections: MutableMapping[str, str],
        results: MutableMapping[str, Any]) -> None:
        """Divides the first target at the first match of the pattern.

        Args:
            texts: the texts of the targets that are not blank.
            sections: the sections of text, by name.
            results: the variables found so far, by name.

        """
        first, second = self.variables
        if not texts:
            results.setdefault(first, None)
            results.setdefault(second, None)
            return
        text = texts[0]
        match = self.pattern.search(text)
        if match is None:
            before, after = text, ''
        else:
            before, after = text[:match.start()], text[match.end():]
        for name, part in ((first, before.strip()), (second, after.strip())):
            results[name] = part or None
            if part:
                sections[name] = part


@dataclasses.dataclass
class Rulebook:
    """An ordered collection of rules.

    Args:
        rules: the rules, in the order they are applied. Defaults to an empty
            `list`.
        name: a name for the rulebook, such as the file it came from. Defaults
            to an empty `str`.

    """

    rules: list[Rule] = dataclasses.field(default_factory = list)
    name: str = ''

    """ Class Methods """

    @classmethod
    def load(cls, *sources: str | pathlib.Path) -> Rulebook:
        """Loads rules from CSV files.

        Args:
            *sources: names of built-in rulebooks (such as "federal" or
                "lexis_nexis"), or paths to CSV files or to folders of them.
                The rules are applied in the order they are given (and, in a
                folder, in the alphabetical order of the files).

        Raises:
            FileNotFoundError: if a source is not a built-in rulebook or a file
                or folder.
            ValueError: if a file is missing a column or a row is not a valid
                rule. The message names the file and row.

        Returns:
            A rulebook with the rules of every source.

        """
        rules: list[Rule] = []
        names: list[str] = []
        for source in sources:
            for path in find_instructions(source):
                rules.extend(_read(path))
                names.append(path.stem)
        return cls(rules = rules, name = ', '.join(names))

    """ Properties """

    @property
    def sections(self) -> list[str]:
        """Returns the names of the sections of text that the rules make.

        Returns:
            The variables of the `section`, `excerpts`, and `split` rules, in
                order, without repeats.

        """
        return _unique(
            v for r in self.rules if r.kind in _SECTION_KINDS
            for v in r.variables)

    @property
    def targets(self) -> list[str]:
        """Returns the names of the sections of text that the rules search.

        Returns:
            The targets of every rule, in order, without repeats.

        """
        return _unique(t for rule in self.rules for t in rule.targets)

    @property
    def variables(self) -> list[str]:
        """Returns the names of the columns that the rules make.

        Returns:
            The variables of every rule except `remove` rules (which make
                none), in order, without repeats.

        """
        return _unique(
            v for r in self.rules if r.kind != 'remove' for v in r.variables)

    """ Public Methods """

    def apply(self, sections: MutableMapping[str, str]) -> dict[str, Any]:
        """Applies every rule, in order, to `sections`.

        Args:
            sections: the sections of text, by name. Rules that make or change
                sections change it in place.

        Returns:
            The value of each variable, by name. A variable whose rules found
                nothing is `False` (flags), 0 (counts), an empty `list`
                (matches and names), or `None` (labels and sections).

        """
        results: dict[str, Any] = {}
        for rule in self.rules:
            rule.apply(sections, results)
        return results

    """ Dunder Methods """

    def __add__(self, other: Rulebook) -> Rulebook:
        """Returns a rulebook with the rules of both rulebooks.

        Args:
            other: the rulebook whose rules come second.

        Returns:
            A new rulebook with this rulebook's rules followed by those of
                `other`, named for both.

        """
        name = ', '.join(n for n in (self.name, other.name) if n)
        return Rulebook(rules = [*self.rules, *other.rules], name = name)

    def __iter__(self) -> Iterator[Rule]:
        """Returns an iterator of the rules.

        Returns:
            An iterator of `rules`, in the order they are applied.

        """
        return iter(self.rules)

    def __len__(self) -> int:
        """Returns the number of rules.

        Returns:
            The length of `rules`.

        """
        return len(self.rules)


def find_instructions(source: str | pathlib.Path) -> list[pathlib.Path]:
    """Returns the CSV files for a built-in or custom rulebook.

    Args:
        source: the name of a built-in rulebook (such as "federal" or
            "lexis_nexis"), or the path to a CSV file or a folder of them.

    Raises:
        FileNotFoundError: if `source` is not a built-in rulebook, a CSV file,
            or a folder with CSV files.

    Returns:
        The paths of the CSV files, in the order they are applied.

    """
    path = pathlib.Path(source)
    if not path.exists():
        folder = pathlib.Path(str(importlib.resources.files('courtpy'))) / 'instructions'
        name = str(source)
        if name in _BUILT_IN:
            return [folder / part for part in _BUILT_IN[name]]
        for candidate in (folder / name, folder / f'{name}.csv'):
            if candidate.exists():
                path = candidate
                break
        else:
            message = (
                f'{source!r} is not a CSV file, a folder, or a built-in '
                f'rulebook ({sorted([*_BUILT_IN, "lexis_nexis"])})'
            )
            raise FileNotFoundError(message)
    if path.is_dir():
        files = sorted(path.glob('*.csv'))
        if not files:
            message = f'there are no CSV files in {path}'
            raise FileNotFoundError(message)
        return files
    return [path]


def split_names(text: str, separators: re.Pattern[str]) -> list[str]:
    """Splits `text` into names wherever `separators` matches.

    The text is made upper-case and periods and apostrophes are removed first
    (so "O'Scannlain" becomes "OSCANNLAIN"), and line breaks always separate
    names. Pieces without at least two letters are dropped.

    Args:
        text: text with names, such as "Before SMITH, JONES, and BROWN,
            Circuit Judges."
        separators: a pattern that matches the words and punctuation between
            the names. It is matched against the upper-case text.

    Returns:
        The names, in order, without repeats.

    """
    cleaned = _PUNCTUATION.sub('', text.upper())
    names: list[str] = []
    for line in cleaned.splitlines():
        # The text between matches is used (rather than `re.split`) so that
        # groups in the pattern are not mistaken for names.
        pieces, start = [], 0
        for match in separators.finditer(line):
            pieces.append(line[start:match.start()])
            start = match.end()
        pieces.append(line[start:])
        for piece in pieces:
            name = ' '.join(piece.split())
            if sum(c.isalpha() for c in name) >= 2 and name not in names:
                names.append(name)
    return names


""" Private Functions """


def _convert(value: Any) -> Any:
    """Returns a cell of a CSV file as a number if it is one.

    Args:
        value: the text of the cell.

    Returns:
        An `int` or `float` if the text is a number, the text otherwise, or
            `None` if the cell is blank.

    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for kind in (int, float):
        try:
            return kind(text)
        except ValueError:
            continue
    return text


def _read(path: pathlib.Path) -> list[Rule]:
    """Returns the rules in the CSV file at `path`.

    Args:
        path: path to a CSV file of rules, in UTF-8 (with or without a byte
            order mark, as Excel saves them).

    Raises:
        ValueError: if the file is missing a column or a row is not a valid
            rule.

    Returns:
        The rules, in order.

    """
    with path.open(encoding = 'utf-8-sig', newline = '') as file:
        reader = csv.DictReader(file)
        columns = [c.strip().lower() for c in reader.fieldnames or []]
        missing = [c for c in _REQUIRED if c not in columns]
        if missing:
            message = f'{path} is missing the columns {missing}'
            raise ValueError(message)
        rules = []
        # Row 1 is the header, so the first rule is on row 2.
        for number, row in enumerate(reader, start = 2):
            cells = {
                str(k).strip().lower(): v for k, v in row.items()
                if k is not None}
            if not any(str(v or '').strip() for v in cells.values()):
                continue
            try:
                rules.append(Rule.create(cells))
            except ValueError as error:
                message = f'{path}, row {number}: {error}'
                raise ValueError(message) from error
    return rules


def _unique(items: Iterable[str]) -> list[str]:
    """Returns `items` in order, without repeats.

    Args:
        items: names.

    Returns:
        The names, in the order first seen.

    """
    return list(dict.fromkeys(items))
