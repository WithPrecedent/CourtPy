"""Identifies the judges in parsed cases and adds what is known about them.

The federal rules find the names of the judges on each case as the opinion
writes them, such as "LYNCH" and "R THOMPSON" (the "panel_judges", "authors",
"concurring", and "dissenting" columns). To study the judges, each name has to
be matched to a person. A `Roster` is a table of judges, with one row for each
court that a judge has served on, which finds the judge that a name refers to
from the court and year of the case.

`build_roster` makes that table from three files of the Federal Judicial
Center's Biographical Directory of Article III Federal Judges (which
`download` gets): the judges' service on each court, their demographics, and
their careers before the bench. The rules in the "judges" rulebook (in
courtpy's "instructions" folder) code the careers, so they can be changed
without changing courtpy:

```python
import courtpy

roster = courtpy.judges.Roster.from_fjc()
roster.save()
```

A saved roster is kept outside of projects (in "judges" in
`courtpy.utilities.data_folder()`), where the techniques look for it when no
roster is named. It is a CSV file, so judges and columns (such as a score of
each judge's ideology) can be added to it in a spreadsheet. Every column of
numbers or booleans that does not identify or date a judge's service is an
attribute of the judge, which the techniques use.

Three techniques use a roster. They are added to the `amos` library as soon
as courtpy is imported, so they can be named in settings. Cases and judges
are different things, so the techniques are built from the mergers and
shapers of `amos` (0.2.6 and later), which join tables whose rows are not the
same:

* `merge_judges` (an `amos.Merger`) adds what the roster says about the judge
  that each row names, such as the author of each opinion.
* `code_judges` (an `amos.Munger`) adds the composition of each case's panel:
  the share of its judges who are women, the mean party of the presidents who
  appointed them, and so on. It makes a row for each judge on each case (with
  `lists_to_rows`), finds the judges (with `merge_judges`), and adds summaries
  of each case's judges to the case (with `merge_summary`).
* `judge_votes` (an `amos.Shaper`) makes a table with one row for each judge
  on each case, with the judge's attributes, those of the other judges on the
  panel, and how the judge voted.

Contents:
    CodeJudges: adds the judges and the composition of each case's panel.
    JudgeVotes: makes a table with one row for each judge on each case.
    MergeJudges: adds what a roster says about the judge that each row names.
    Roster: a table of judges that finds the judge a name refers to.
    build_roster: makes the table of a roster from the Federal Judicial
        Center's files.
    clean_name: returns a name the way that names are compared.
    download: downloads the Federal Judicial Center's files about judges.
    name_forms: returns the ways that an opinion may write a judge's name.
    vote_table: returns a table with one row for each judge on each case.
    _ABA_RATINGS: the number for each rating of a nominee by the American
        Bar Association, from 1 (not qualified) to 4.
    _ALIASES: other names that opinions use for judges, by the id of the
        judge ("nid") in the Federal Judicial Center's files.
    _APPELLATE: the Federal Judicial Center's types of courts whose numbers
        the federal rules give to cases.
    _APPOINTMENT: columns of the service file that describe an appointment,
        which a judge keeps when reassigned to another court.
    _CASE: name of the column, in a table with a row for each judge on each
        case, with the position of the case in the table of cases.
    _CIRCUITS: the number of the circuit that the district courts of each
        state or territory belong to.
    _DISTRICT: pattern for the state or territory in the name of a district
        court, which is its group.
    _DISTRICT_COURT: the Federal Judicial Center's type of a district court.
    _FOUND: name of the column, in such a table, that says whether the judge
        was found in the roster (or how many of a case's judges were).
    _IDENTIFIERS: columns of numbers in a roster that identify or date a
        judge's service rather than describe the judge.
    _NAME: name of the column, in such a table, with the judge's name as the
        opinion writes it.
    _NAME_PUNCTUATION: pattern for the punctuation that `clean_name` removes.
    _NEEDED: columns that every roster must have.
    _OTHER: name of the column, in a table that pairs the judges of a case,
        with the place of the other judge.
    _PARTIES: the number for the party of an appointing president.
    _PERSONAL: columns that the demographics file must have.
    _PLACE: name of the column, in such a table, with the place of the judge
        among the judges of the case.
    _RELATIVE: attributes of a judge that depend on the case: its year and
        court.
    _ROLES: the columns of a case table that name the judges who wrote or
        joined each kind of opinion, with the column of a table of votes
        that says whether a judge is one of them.
    _ROSTER_FILE: the name of the file that a roster is saved to.
    _SERVICE: columns that the service file must have.
    _TEXT: columns of text in a roster, which are never attributes, even if
        every cell of one is blank.
    _VOTES: pattern for a roll call vote, such as "96/2", with the ayes and
        nays as its groups.
    _career_flags: codes the careers of judges with a rulebook.
    _circuit: returns the number of the circuit of a district court.
    _colleagues: returns the means of columns among the other judges of each
        judge's case.
    _folder: returns the folder where a roster and its sources are kept.
    _named: finds the judges that a column of the cases names.
    _number_courts: returns the numbers that the federal rules give courts.
    _read: returns a file of the Federal Judicial Center as a table of text.
    _require: raises an error if a table lacks columns.
    _summarize: returns summaries of the judges of each case.
    _vote_share: returns the share of senators who voted to confirm a judge.
    _whole: returns a value as a whole number, or `None` if it is missing.
    _year: returns the years in a column of dates.

"""

from __future__ import annotations

import dataclasses
import pathlib
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import amos
import numpy as np
import pandas as pd

from . import cases, coders, options, rules, utilities

_ABA_RATINGS: dict[str, int] = {
    'Exceptionally Well Qualified': 4,
    'Well Qualified': 3,
    'Qualified': 2,
    'Not Qualified': 1,
    'Not Qualified By Reason of Age': 1}
# Judge Johnson of the Fifth Circuit signed opinions as "Sam D. Johnson", and
# Judge King of the Fifth Circuit sat as Carolyn Dineen Randall until 1988.
_ALIASES: dict[int, str] = {
    1382851: 'Sam D. Johnson',
    1386716: 'Carolyn Dineen Randall'}
_APPELLATE: frozenset[str] = frozenset({
    'U.S. Court of Appeals', 'Supreme Court'})
_APPOINTMENT: tuple[str, ...] = (
    'appointing president', 'party of appointing president', 'aba rating',
    'recess appointment date', 'senate vote type', 'ayes/nays')
# Names of the columns that the techniques add to the tables that they make
# on the way to their results, which no table of cases or roster has.
_CASE: str = '__case__'
_FOUND: str = '__found__'
_NAME: str = '__name__'
_OTHER: str = '__other__'
_PLACE: str = '__place__'
_CIRCUITS: dict[str, int] = {
    'Maine': 1, 'Massachusetts': 1, 'New Hampshire': 1, 'Puerto Rico': 1,
    'Rhode Island': 1,
    'Connecticut': 2, 'New York': 2, 'Vermont': 2,
    'Delaware': 3, 'New Jersey': 3, 'Pennsylvania': 3, 'Virgin Islands': 3,
    'Maryland': 4, 'North Carolina': 4, 'South Carolina': 4, 'Virginia': 4,
    'West Virginia': 4,
    'Canal Zone': 5, 'Louisiana': 5, 'Mississippi': 5, 'Texas': 5,
    'Kentucky': 6, 'Michigan': 6, 'Ohio': 6, 'Tennessee': 6,
    'Illinois': 7, 'Indiana': 7, 'Wisconsin': 7,
    'Arkansas': 8, 'Iowa': 8, 'Minnesota': 8, 'Missouri': 8, 'Nebraska': 8,
    'North Dakota': 8, 'South Dakota': 8,
    'Alaska': 9, 'Arizona': 9, 'California': 9, 'Guam': 9, 'Hawaii': 9,
    'Idaho': 9, 'Montana': 9, 'Nevada': 9, 'Northern Mariana Islands': 9,
    'Oregon': 9, 'Washington': 9,
    'Colorado': 10, 'Kansas': 10, 'New Mexico': 10, 'Oklahoma': 10,
    'Utah': 10, 'Wyoming': 10,
    'Alabama': 11, 'Florida': 11, 'Georgia': 11,
    'Columbia': 12}
# "Middle District of Georgia" and "Districts of North Carolina" both end
# with the state, which any note in parentheses follows.
_DISTRICT: re.Pattern[str] = re.compile(
    r'Districts? of ([A-Za-z ]+?)\s*(?:\(|$)')
_DISTRICT_COURT: str = 'U.S. District Court'
_IDENTIFIERS: frozenset[str] = frozenset({
    'nid', 'court_num', 'circuit_num', 'start_year', 'end_year',
    'senior_year', 'birth_year'})
# The curly apostrophe (U+2019) is added with `chr` so that the source file
# has only plain characters.
_NAME_PUNCTUATION: re.Pattern[str] = re.compile(
    r"[.,\[\]'" + chr(0x2019) + ']')
_NEEDED: tuple[str, ...] = (
    'nid', 'judge', 'last_name', 'first_name', 'court_num', 'circuit_num',
    'start_year', 'end_year')
_PARTIES: dict[str, int] = {'Democratic': -1, 'Republican': 1}
_PERSONAL: tuple[str, ...] = (
    'nid', 'last name', 'first name', 'middle name', 'suffix', 'birth year',
    'gender', 'race or ethnicity')
_RELATIVE: tuple[str, ...] = ('age', 'senior', 'designated', 'district_judge')
_ROLES: dict[str, str] = {
    'authors': 'judge_author',
    'concurring': 'judge_concurred',
    'dissenting': 'judge_dissented'}
_ROSTER_FILE: str = 'roster.csv'
_SERVICE: tuple[str, ...] = (
    'nid', 'judge name', 'court type', 'court name', 'commission date',
    'senior status date', 'termination date', *_APPOINTMENT)
_TEXT: tuple[str, ...] = (
    'judge', 'last_name', 'first_name', 'middle_name', 'suffix', 'aliases',
    'court', 'court_type', 'president')
_VOTES: re.Pattern[str] = re.compile(r'(\d+)\s*/\s*(\d+)')


@dataclasses.dataclass
class Roster:
    """A table of judges that finds the judge a name in an opinion refers to.

    Each row of the table is a judge's service on one court, so a judge who
    was promoted from a district court to a court of appeals has two rows,
    with the president who made each appointment. An opinion usually gives
    only a judge's last name, which many judges share, so a name is matched
    with the court and year of its case:

    1. to a judge of the case's court,
    2. or else to a judge of a district court in the case's circuit, who may
       sit on its court of appeals by designation,
    3. or else to a judge of any other court, who may be visiting.

    Only judges who were serving in the year of the case (or had left their
    court no more than `grace` years before it) are considered. If a name
    fits more than one such judge at the first step that it fits any, the
    name is not matched, since there is no telling which judge was meant. Nor
    is a name on a case of a court that has no judges in the table (such as a
    bankruptcy appellate panel, whose judges are not in the Federal Judicial
    Center's files), since a judge of another court who shares the name
    would be a guess.

    Args:
        data: the table. It needs the columns "nid" (a number that identifies
            each judge), "judge" (the judge's name, as it is reported),
            "last_name", "first_name", "court_num" and "circuit_num" (the
            numbers that the federal rules give the judge's court and its
            circuit), and "start_year" and "end_year" (of the judge's service
            on the court). "middle_name", "aliases" (other names that
            opinions use for the judge, as "First Middle Last", separated by
            semicolons), "senior_year", "birth_year", and "court_type" are
            used if they are there.
        grace: years after a judge's service on a court ended in which an
            opinion may still name the judge. Defaults to
            `options._JUDGE_GRACE` (as it is when the roster is made).

    """

    data: pd.DataFrame
    grace: int = dataclasses.field(
        default_factory = lambda: options._JUDGE_GRACE)
    _described: dict[
        tuple[int, int | None, int | None], dict[str, float | None]] = (
            dataclasses.field(default_factory = dict, init = False, repr = False))
    _forms: dict[str, list[int]] = dataclasses.field(
        default_factory = dict, init = False, repr = False)
    _found: dict[tuple[str, int | None, int | None], int | None] = (
        dataclasses.field(default_factory = dict, init = False, repr = False))
    _numbers: dict[str, list[int | None]] = dataclasses.field(
        default_factory = dict, init = False, repr = False)
    _text: dict[str, list[str]] = dataclasses.field(
        default_factory = dict, init = False, repr = False)
    _values: dict[str, list[float | None]] = dataclasses.field(
        default_factory = dict, init = False, repr = False)

    """ Initialization Methods """

    def __post_init__(self) -> None:
        """Checks the table and indexes the names and numbers of its judges.

        Raises:
            KeyError: if the table lacks a column that every roster needs.

        """
        _require(self.data, _NEEDED, 'a roster of judges')
        self.data = cases.tidy(self.data.reset_index(drop = True))
        for column in _TEXT:
            self._text[column] = (
                self.data[column].fillna('').astype(str).tolist()
                if column in self.data.columns else [''] * len(self.data))
        people = zip(
            self._text['first_name'], self._text['middle_name'],
            self._text['last_name'], self._text['aliases'], strict = True)
        for row, (first, middle, last, aliases) in enumerate(people):
            forms = name_forms(first, middle, last)
            for alias in aliases.split(';'):
                parts = alias.split()
                if len(parts) > 1:
                    forms.extend(name_forms(
                        parts[0], ' '.join(parts[1:-1]), parts[-1]))
            for form in dict.fromkeys(forms):
                self._forms.setdefault(form, []).append(row)
        for column in sorted(_IDENTIFIERS):
            self._numbers[column] = (
                [_whole(value) for value in self.data[column]]
                if column in self.data.columns else [None] * len(self.data))
        for column in self.attributes:
            self._values[column] = [
                None if pd.isna(value) else float(value)
                for value in self.data[column]]

    """ Class Methods """

    @classmethod
    def create(
        cls,
        source: Roster | pd.DataFrame | pathlib.Path | str | None = None) -> (
            Roster):
        """Returns a roster made from `source`.

        Args:
            source: a roster, the table of one, or the path of a saved one.
                Defaults to `None`, in which case the roster saved in
                "judges" in `utilities.data_folder()` is loaded.

        Returns:
            The roster.

        """
        if isinstance(source, Roster):
            return source
        if isinstance(source, pd.DataFrame):
            return cls(source)
        return cls.load(source)

    @classmethod
    def from_fjc(
        cls,
        folder: pathlib.Path | str | None = None,
        *,
        update: bool = False,
        scores: pd.DataFrame | pathlib.Path | str | None = None,
        rulebook: rules.Rulebook | pathlib.Path | str = 'judges') -> Roster:
        """Returns a roster made from the Federal Judicial Center's files.

        Any of the files that `folder` lacks is downloaded to it first (see
        `download`).

        Args:
            folder: the folder with the files. Defaults to `None`, in which
                case "judges" in `utilities.data_folder()` is used.
            update: whether to download the files again even if they are
                there, to get the judges and changes since they were
                downloaded. Defaults to `False`.
            scores: scores of the judges to add (see `build_roster`). Defaults
                to `None`.
            rulebook: the rules that code the judges' careers (see
                `build_roster`). Defaults to "judges".

        Returns:
            The roster.

        """
        files = download(folder, overwrite = update)
        return cls(build_roster(
            files['service'], files['demographics'], files['career'],
            scores = scores, rulebook = rulebook))

    @classmethod
    def load(cls, path: pathlib.Path | str | None = None) -> Roster:
        """Loads a roster saved by `save` (or made in a spreadsheet).

        Args:
            path: path of the CSV file. Defaults to `None`, in which case it
                is "roster.csv" in "judges" in `utilities.data_folder()`.

        Raises:
            FileNotFoundError: if there is no file at `path`.

        Returns:
            The roster.

        """
        found = (
            pathlib.Path(path).expanduser() if path
            else _folder() / _ROSTER_FILE)
        if not found.is_file():
            message = (
                f'there is no roster of judges at {found}: make one with '
                f'`courtpy.judges.Roster.from_fjc()` and save it with its '
                f'`save` method'
            )
            raise FileNotFoundError(message)
        return cls(pd.read_csv(
            found, encoding = 'utf-8', dtype = dict.fromkeys(_TEXT, str)))

    """ Properties """

    @property
    def attributes(self) -> list[str]:
        """Returns the names of the columns that describe the judges.

        Returns:
            The columns of numbers or booleans that do not identify or date a
                judge's service (such as "party" and "woman"), in order.

        """
        return [
            column for column in self.data.columns
            if column not in _IDENTIFIERS and column not in _TEXT and (
                pd.api.types.is_bool_dtype(self.data[column].dtype)
                or pd.api.types.is_numeric_dtype(self.data[column].dtype))]

    @property
    def described(self) -> list[str]:
        """Returns the names of everything that `describe` reports.

        Returns:
            `attributes`, followed by the attributes that depend on the case
                ("age", "senior", "designated", and "district_judge").

        """
        return [*self.attributes, *_RELATIVE]

    """ Public Methods """

    def describe(
        self,
        row: int,
        court_num: int | None = None,
        year: int | None = None) -> dict[str, float | None]:
        """Returns what is known about a judge on a case.

        Args:
            row: the judge's row of the table (as `match` returns it).
            court_num: the number of the case's court. Defaults to `None`.
            year: the year of the case. Defaults to `None`.

        Returns:
            Each of `described`, as a number (1 and 0 for true and false) or
                `None` if it is not known. "age" is the judge's age at the
                end of `year`, "senior" is whether the judge had taken senior
                status by `year`, "designated" is whether the judge's court
                is not the case's court (so the judge sat by designation),
                and "district_judge" is whether it is a district court. The
                same `dict` is returned each time for a judge, court, and
                year, so copy it before changing it.

        """
        key = (row, court_num, year)
        if key in self._described:
            return self._described[key]
        known = {name: values[row] for name, values in self._values.items()}
        birth = self._numbers['birth_year'][row]
        senior = self._numbers['senior_year'][row]
        known['age'] = (
            None if year is None or birth is None else float(year - birth))
        known['senior'] = (
            None if year is None or 'senior_year' not in self.data.columns
            else float(senior is not None and senior <= year))
        known['designated'] = (
            None if court_num is None
            else float(self._numbers['court_num'][row] != court_num))
        known['district_judge'] = (
            float(self._text['court_type'][row] == _DISTRICT_COURT)
            if 'court_type' in self.data.columns else None)
        self._described[key] = known
        return known

    def identify(
        self,
        names: Iterable[str],
        court_num: int | None = None,
        year: int | None = None) -> list[int]:
        """Returns the judges that the names on a case refer to.

        Args:
            names: the names, as the opinion writes them.
            court_num: the number of the case's court. Defaults to `None`.
            year: the year of the case. Defaults to `None`.

        Returns:
            The rows of the judges who were found, in the order of `names`,
                with each judge once.

        """
        rows: list[int] = []
        judges: set[int | None] = set()
        for name in names:
            row = self.match(name, court_num, year)
            if row is not None and self._numbers['nid'][row] not in judges:
                rows.append(row)
                judges.add(self._numbers['nid'][row])
        return rows

    def match(
        self,
        name: str,
        court_num: int | None = None,
        year: int | None = None) -> int | None:
        """Returns the judge that a name on a case refers to.

        Args:
            name: the name, as the opinion writes it (such as "LYNCH",
                "R. Thompson", or "Sandra L. Lynch").
            court_num: the number of the case's court, which is needed to
                prefer its own judges and those of its circuit. Defaults to
                `None`, in which case judges of every court are considered.
            year: the year of the case. Defaults to `None`, in which case
                judges of every year are considered.

        Returns:
            The row of the judge's service in the table (the most recent, if
                more than one fits), or `None` if the name is not matched
                (see the class's documentation).

        """
        key = (clean_name(name), court_num, year)
        if key not in self._found:
            self._found[key] = self._match(*key)
        return self._found[key]

    def names(self, rows: Iterable[int]) -> list[str]:
        """Returns the names of judges as the roster reports them.

        Args:
            rows: rows of the table (as `identify` returns them).

        Returns:
            The "judge" of each row, such as "Lynch, Sandra Lea".

        """
        return [self._text['judge'][row] for row in rows]

    def nids(self, rows: Iterable[int]) -> list[int]:
        """Returns the numbers that identify judges.

        Args:
            rows: rows of the table (as `identify` returns them).

        Returns:
            The "nid" of each row.

        """
        return [int(self._numbers['nid'][row] or 0) for row in rows]

    def save(self, path: pathlib.Path | str | None = None) -> pathlib.Path:
        """Saves the table as a CSV file, which a spreadsheet can change.

        Args:
            path: path to save it to. Its folder is created if needed.
                Defaults to `None`, in which case it is "roster.csv" in
                "judges" in `utilities.data_folder()`, where `load` and the
                techniques look for it.

        Returns:
            The path the table was saved to.

        """
        saved = (
            pathlib.Path(path).expanduser() if path
            else _folder() / _ROSTER_FILE)
        saved.parent.mkdir(parents = True, exist_ok = True)
        self.data.to_csv(saved, index = False, encoding = 'utf-8')
        return saved

    """ Private Methods """

    def _match(
        self,
        name: str,
        court_num: int | None,
        year: int | None) -> int | None:
        """Returns the judge that a cleaned name on a case refers to.

        Args:
            name: the name, as `clean_name` returns it.
            court_num: the number of the case's court, or `None`.
            year: the year of the case, or `None`.

        Returns:
            The row of the judge's service, or `None`.

        """
        courts = self._numbers['court_num']
        circuits = self._numbers['circuit_num']
        if court_num is not None and court_num not in courts:
            return None
        rows = [
            row for row in self._forms.get(name, ())
            if year is None or self._serving(row, year)]
        tiers = (
            [r for r in rows if court_num is not None
             and courts[r] == court_num],
            [r for r in rows if court_num is not None
             and circuits[r] == court_num],
            rows)
        for tier in tiers:
            judges = {self._numbers['nid'][row] for row in tier}
            if len(judges) > 1:
                return None
            if judges:
                starts = self._numbers['start_year']
                return max(tier, key = lambda row: starts[row] or 0)
        return None

    def _serving(self, row: int, year: int) -> bool:
        """Returns whether an opinion of a year may name a judge.

        Args:
            row: a row of the table.
            year: the year of a case.

        Returns:
            Whether the service of `row` had begun by `year` and had not
                ended more than `grace` years before it.

        """
        start = self._numbers['start_year'][row]
        end = self._numbers['end_year'][row]
        return (
            start is not None and start <= year
            and (end is None or year <= end + self.grace))


@dataclasses.dataclass
class CodeJudges(amos.Munger):
    """Adds the judges and the composition of each case's panel.

    Each name in "panel_judges" is matched to a judge in a roster (see
    `Roster`) with the case's court and year. The judges who are found are
    listed in "panel_names" (as the roster names them, such as "Lynch, Sandra
    Lea"), and "panel_found" is how many there are, which can be compared
    with "panel_size". For each attribute in the roster, "panel_{attribute}"
    is its mean among those judges: "panel_woman" is the share of the judges
    who are women, and "panel_party" is the mean party of the presidents who
    appointed them (-1 for a Democrat and 1 for a Republican). So are
    "panel_age", "panel_senior", "panel_designated", and
    "panel_district_judge" (see `Roster.describe`). A mean is missing if no
    judge was found (or none has the attribute).

    If the cases have "authors", "concurring", or "dissenting" columns, the
    judges they name are added as "author_name" (the first author found),
    "concurring_names", and "dissenting_names".

    The judges of each case are found as `merge_judges` finds them (see
    `MergeJudges`), in a table with a row for each judge on each case, and
    the case gets summaries of its rows (as the `merge_summary` technique of
    `amos` adds them).

    Needs "panel_judges", "court_num", and "year".

    """

    def munge(
        self,
        data: pd.DataFrame,
        roster: Roster | pd.DataFrame | pathlib.Path | str | None = None,
        **kwargs: Any) -> pd.DataFrame:
        """Adds the judges and the composition of each panel.

        Args:
            data: the parsed cases.
            roster: a roster, the table of one, or the path of a saved one.
                Defaults to `None`, in which case the roster saved in
                "judges" in `utilities.data_folder()` is used.
            **kwargs: not used.

        Returns:
            The cases, with the new columns.

        """
        coders._require(data, ['panel_judges', 'court_num', 'year'], self.name)
        judges = Roster.create(roster)
        described = judges.described
        panels = _summarize(
            _named(data, 'panel_judges', judges, ['judge', *described]),
            len(data),
            {'judge': 'list', **dict.fromkeys(described, 'mean')},
            count = _FOUND)
        names = {
            column: _summarize(
                _named(data, column, judges, ['judge']),
                len(data),
                {'judge': 'list'})['judge'].tolist()
            for column in _ROLES if column in data.columns}
        data['panel_names'] = pd.Series(
            panels['judge'].tolist(), index = data.index, dtype = object)
        data['panel_found'] = panels[_FOUND].to_numpy()
        if 'authors' in names:
            data['author_name'] = pd.Series(
                [found[0] if found else None for found in names['authors']],
                index = data.index, dtype = object)
        for column in ('concurring', 'dissenting'):
            if column in names:
                data[f'{column}_names'] = pd.Series(
                    names[column], index = data.index, dtype = object)
        for name in described:
            data[f'panel_{name}'] = panels[name].to_numpy(dtype = 'float64')
        return data


@dataclasses.dataclass
class JudgeVotes(amos.Shaper):
    """Makes a table with one row for each judge on each case.

    The table is for studying how judges vote, rather than what courts
    decide. See `vote_table` for its columns. It replaces the table of cases,
    so it should come after the techniques that code the cases (such as
    `code_outcome`) and, like every shaper of `amos`, before the data is
    split. Its "case_id" column can be the "groups" of a study, which keeps
    the judges of a case together.

    Like every shaper, it takes "label", "task", and "groups", which name the
    label and groups of the table of votes (such as "vote_reversal" and
    "case_id"). Without them, the dataset keeps the label and groups that it
    has.

    """

    def shape(
        self,
        data: pd.DataFrame,
        roster: Roster | pd.DataFrame | pathlib.Path | str | None = None,
        outcomes: Sequence[str] | str | None = None,
        **kwargs: Any) -> pd.DataFrame:
        """Returns a table with one row for each judge on each case.

        Args:
            data: the parsed cases.
            roster: a roster, the table of one, or the path of a saved one.
                Defaults to `None`, in which case the roster saved in
                "judges" in `utilities.data_folder()` is used.
            outcomes: names of the columns with the outcomes of the cases,
                for which each judge's vote is added. Defaults to `None`, in
                which case they are the columns whose names start with
                "outcome_".
            **kwargs: not used.

        Returns:
            The table of judges' votes.

        """
        return vote_table(
            data, roster, outcomes = utilities.listify(outcomes) or None)


@dataclasses.dataclass
class MergeJudges(amos.Merger):
    """Adds what a roster says about the judge that each row names.

    Each row of the data names one judge, as an opinion writes the name
    (such as "LYNCH"). The name is matched to a judge in a roster (see
    `Roster`) with the court and year of the row's case, and the columns of
    the judge's row of the roster are added, with a prefix before their
    names. So a table with a row for each judge on each case gets
    "judge_party", "judge_woman", and so on. A table of cases gets what is
    known about the author of each opinion with "name" set to "author" and
    "prefix" to "author_". A row whose name is not matched gets missing
    values.

    Like every merger of `amos`, it takes these parameters, never adds,
    removes, or reorders rows, and records how many rows it matched in the
    dataset's history:

    | Parameter | Meaning |
    | --- | --- |
    | `source` | The roster: the path of a saved one (looked for in the current folder and then in the clerk's input folder), a roster, or the table of one. Without it, the roster saved in "judges" in `utilities.data_folder()` is used. |
    | `columns` | The columns to add. By default, they are the roster's attributes (see `Roster.attributes`) and those that depend on the case: "age", "senior", "designated", and "district_judge" (see `Roster.describe`). All of them are added as numbers (1 and 0 for true and false). Any other column of the roster can be named too, such as "judge" (the judge's name, as the roster reports it), "nid", or "president". |
    | `prefix` | Text to put before the names of the added columns. It is "judge_" unless another is set. |
    | `indicator` | The name of a column to make that says whether each row's judge was found. |
    | `name`, `court`, `year` | The columns of the data with the judge's name and the number of the court and the year of the row's case. They are "judge", "court_num", and "year" unless others are set. |

    """

    """ Public Methods """

    def implement(
        self,
        item: amos.Dataset,
        *,
        source: Any = None,
        columns: Sequence[str] | str | None = None,
        prefix: str | None = 'judge_',
        indicator: str | None = None,
        name: str = 'judge',
        court: str = 'court_num',
        year: str = 'year',
        **kwargs: Any) -> amos.Dataset:
        """Adds the columns of the judge that each row of `item` names.

        Args:
            item: the dataset to add columns to.
            source: the roster: a roster, the table of one, or the path of a
                saved one. Defaults to `None`, in which case the roster saved
                in "judges" in `utilities.data_folder()` is used.
            columns: the columns to add: attributes of the judge (see
                `Roster.described`) or other columns of the roster. Defaults
                to `None`, in which case they are every attribute.
            prefix: text to put before the names of the added columns.
                Defaults to "judge_".
            indicator: name of a boolean column to make that says whether
                each row's judge was found. Defaults to `None`, which makes
                no such column.
            name: the column of `item` with the judge's name, as an opinion
                writes it. Defaults to "judge".
            court: the column of `item` with the number of the court of the
                row's case. Defaults to "court_num".
            year: the column of `item` with the year of the row's case.
                Defaults to "year".
            **kwargs: not used.

        Raises:
            KeyError: if `item` lacks one of the columns `name`, `court`, and
                `year`, or one of `columns` is not in the roster.
            ValueError: if an added column would have the name of a column
                of the data.

        Returns:
            The dataset with the added columns. Its rows and index are not
                changed.

        """
        if source is None:
            source = _folder() / _ROSTER_FILE
        roster = (
            Roster.create(utilities.locate(source, self.clerk))
            if isinstance(source, str | pathlib.Path)
            else Roster.create(source))
        data = item.data
        rows = np.asarray(
            self.match(data, roster, name = name, court = court, year = year),
            dtype = 'int64')
        chosen = (
            roster.described if columns is None
            else utilities.listify(columns))
        missing = [
            c for c in chosen
            if c not in roster.described and c not in roster.data.columns]
        if missing:
            message = f'the columns {missing} are not in the roster of judges'
            raise KeyError(message)
        described = pd.DataFrame(columns = roster.described, dtype = 'float64')
        if any(column in roster.described for column in chosen):
            blank: dict[str, float | None] = dict.fromkeys(roster.described)
            cases_of = zip(
                rows.tolist(), data[court].tolist(), data[year].tolist(),
                strict = True)
            described = pd.DataFrame.from_records(
                [roster.describe(row, _whole(number), _whole(when))
                 if row >= 0 else blank for row, number, when in cases_of],
                columns = roster.described).astype('float64')
        # A row of -1 is no row of the roster, so its values are missing.
        listed = roster.data.reindex(rows)
        added = pd.DataFrame({
            f'{prefix or ""}{column}': (
                described[column] if column in roster.described
                else listed[column]).to_numpy()
            for column in chosen}, index = data.index)
        if indicator is not None:
            added[indicator] = rows >= 0
        clashes = [c for c in added.columns if c in data.columns]
        if clashes:
            message = (
                f'{self.name!r} would add columns named {clashes}, which the '
                f'data already has: set "prefix" to tell them apart, or name '
                f'the columns to add in "columns"'
            )
            raise ValueError(message)
        item.replace(pd.concat([data, added], axis = 1))
        item.record(
            self.name,
            source = (
                str(source) if isinstance(source, str | pathlib.Path)
                else type(source).__name__),
            rows = len(data),
            matched = int((rows >= 0).sum()),
            created = list(added.columns))
        return item

    def match(
        self,
        data: pd.DataFrame,
        other: pd.DataFrame | Roster,
        *,
        name: str = 'judge',
        court: str = 'court_num',
        year: str = 'year',
        **kwargs: Any) -> Any:
        """Returns the row of a roster for the judge that each row names.

        Args:
            data: the data, with a row for each judge to find.
            other: a roster, or the table of one.
            name: the column of `data` with the judge's name, as an opinion
                writes it. Defaults to "judge".
            court: the column of `data` with the number of the court of the
                row's case. Defaults to "court_num".
            year: the column of `data` with the year of the row's case.
                Defaults to "year".
            **kwargs: not used.

        Raises:
            KeyError: if `data` lacks one of the columns.

        Returns:
            The position of the judge's row in the roster for each row of
                `data` (see `Roster.match`), or -1 if the name is missing or
                is not matched.

        """
        _require(data, [name, court, year], repr(self.name))
        roster = Roster.create(other)
        named = zip(
            data[name].tolist(), data[court].tolist(), data[year].tolist(),
            strict = True)
        found = (
            roster.match(written, _whole(number), _whole(when))
            if isinstance(written, str) and written.strip() else None
            for written, number, when in named)
        return np.array(
            [-1 if row is None else row for row in found], dtype = 'int64')


def build_roster(
    service: pd.DataFrame | pathlib.Path | str,
    demographics: pd.DataFrame | pathlib.Path | str,
    career: pd.DataFrame | pathlib.Path | str | None = None,
    *,
    scores: pd.DataFrame | pathlib.Path | str | None = None,
    rulebook: rules.Rulebook | pathlib.Path | str = 'judges') -> pd.DataFrame:
    """Makes the table of a roster from the Federal Judicial Center's files.

    The files are those of the Biographical Directory of Article III Federal
    Judges that are organized by category (see `download`). The table has one
    row for each court that each judge has served on, with these columns:

    | Column | Meaning |
    | --- | --- |
    | `nid` | The center's number for the judge. |
    | `judge` | The judge's name, as the center reports it. |
    | `last_name`, `first_name`, `middle_name`, `suffix` | Its parts. |
    | `aliases` | Other names that opinions use for the judge. |
    | `court`, `court_type` | The court's name and its type. |
    | `court_num` | The number that the federal rules give a court of appeals or the Supreme Court. Missing for other courts. |
    | `circuit_num` | The number of the court's circuit: its own, or for a district court, that of its state. |
    | `start_year`, `end_year` | The years the judge's service on the court began (with a commission or a recess appointment) and ended. `end_year` is missing for a judge who is still serving. |
    | `senior_year` | The year the judge took senior status, if any. |
    | `birth_year` | The year the judge was born. |
    | `president` | The president who appointed the judge. |
    | `party` | That president's party: -1 for Democratic and 1 for Republican. Missing for other parties. |
    | `aba_rating` | The American Bar Association's rating, from 1 (not qualified) to 4 (exceptionally well qualified). Missing if there was none. |
    | `recess` | Whether the judge first took the seat by a recess appointment. |
    | `senate_vote` | The share of senators who voted to confirm the judge (1 for a voice vote). Missing if the vote is not recorded. |
    | `woman`, `minority` | Whether the judge is a woman, and whether the judge's race or ethnicity is anything other than white. |

    A judge who was reassigned to another court without a new appointment
    (as when the Eleventh Circuit was divided from the Fifth) keeps the
    president, party, rating, recess appointment, and vote of the judge's
    last appointment.

    If `career` is passed, each variable that `rulebook` makes from a
    judge's career is a column too. The "judges" rulebook makes
    "prosecutor", "public_defender", "law_clerk", "supreme_court_clerk",
    "solicitor_general", and "law_professor".

    The files have a row for each court that a judge has served on, for each
    judge, and for each position that a judge has held, so they are combined
    by the judges' numbers with the `merge_keys` technique of `amos`: each
    row of a judge's service gets the columns of the judge.

    Args:
        service: the file of federal judicial service
            ("federal-judicial-service.csv"), as a path or a table.
        demographics: the file of demographics ("demographics.csv").
        career: the file of professional careers
            ("professional-career.csv"). Defaults to `None`, in which case no
            careers are coded.
        scores: a table (or the path of a CSV file) with an "nid" column and
            columns of scores for the judges (such as their Judicial Common
            Space scores), which are added to the roster. If it has more
            than one row for a judge, the first is used, and a column whose
            name the roster already has is left out. Defaults to `None`.
        rulebook: the rules that code careers: a rulebook, or the name of a
            built-in rulebook or the path of a CSV file. Its rules search the
            section "career", which has each position that a judge held on
            its own line. Defaults to "judges".

    Raises:
        KeyError: if a file lacks a column that is needed.
        ValueError: if the file of demographics has more than one row for a
            judge.

    Returns:
        The table, sorted by judge and then by the order of the judge's
            service.

    """
    merger = amos.mergers.MergeKeys()
    served = _read(service)
    _require(served, _SERVICE, 'the file of federal judicial service')
    people = _read(demographics)
    _require(people, _PERSONAL, 'the file of demographics')
    order = (
        pd.to_numeric(served['sequence'], errors = 'coerce')
        if 'sequence' in served.columns else 0)
    served = (
        served.assign(nid = served['nid'].astype(int), order = order)
        .sort_values(['nid', 'order'], kind = 'stable')
        .reset_index(drop = True))
    # A reassigned judge has "None (reassignment)" in place of a president,
    # so the columns of the appointment are taken from the judge's row above.
    reassigned = served['appointing president'].str.startswith('None')
    columns = list(_APPOINTMENT)
    appointment = served[columns].astype(object).mask(reassigned)
    inherited = appointment.groupby(served['nid']).ffill()
    served[columns] = inherited.where(reassigned, served[columns]).fillna('')
    # Each row of a judge's service gets the judge's demographics. A judge
    # who is not in the file of demographics has blank ones.
    details = [column for column in _PERSONAL if column != 'nid']
    personal = merger.apply(
        served[['nid']],
        source = people.assign(nid = people['nid'].astype(int)),
        on = 'nid',
        columns = details).data[details].fillna('')
    numbers = _number_courts(
        served.loc[served['court type'].isin(_APPELLATE), 'court name'])
    court_num = served['court name'].map(numbers).astype('Int64')
    circuit_num = court_num.where(
        served['court type'] != _DISTRICT_COURT,
        served['court name'].map(_circuit).astype('Int64'))
    started = _year(served['commission date'])
    roster = pd.DataFrame({
        'nid': served['nid'],
        'judge': served['judge name'],
        'last_name': personal['last name'],
        'first_name': personal['first name'],
        'middle_name': personal['middle name'],
        'suffix': personal['suffix'],
        'aliases': served['nid'].map(_ALIASES).fillna(''),
        'court': served['court name'],
        'court_type': served['court type'],
        'court_num': court_num,
        'circuit_num': circuit_num,
        'start_year': started.fillna(_year(served['recess appointment date'])),
        'end_year': _year(served['termination date']),
        'senior_year': _year(served['senior status date']),
        'birth_year': _year(personal['birth year']),
        'president': served['appointing president'].astype(str),
        'party': served['party of appointing president'].map(
            _PARTIES).astype('Int64'),
        'aba_rating': served['aba rating'].map(_ABA_RATINGS).astype('Int64'),
        'recess': served['recess appointment date'] != '',
        'senate_vote': [
            _vote_share(kind, votes) for kind, votes in zip(
                served['senate vote type'], served['ayes/nays'],
                strict = True)],
        'woman': (personal['gender'] == 'Female').astype('boolean').mask(
            personal['gender'] == ''),
        'minority': (
            personal['race or ethnicity'] != 'White').astype('boolean').mask(
                personal['race or ethnicity'] == '')})
    if career is not None:
        flags = _career_flags(_read(career), rulebook, roster['nid'])
        roster = merger.apply(
            roster, source = flags.rename_axis('nid').reset_index(),
            on = 'nid').data
    if scores is not None:
        scored = (
            scores.copy() if isinstance(scores, pd.DataFrame)
            else utilities.read_csv(scores))
        _require(scored, ['nid'], 'the table of scores')
        roster = merger.apply(
            roster,
            source = scored.assign(
                nid = pd.to_numeric(scored['nid'], errors = 'coerce')),
            on = 'nid',
            duplicates = 'first').data
    return roster


def clean_name(name: str) -> str:
    """Returns a name the way that names are compared.

    Args:
        name: a name or a part of one, such as "R[obert] Lanier" or
            "O'Scannlain".

    Returns:
        The name in capital letters, without periods, commas, brackets, or
            apostrophes, and with single spaces (as the `names` rules of a
            rulebook write the names they find).

    """
    return ' '.join(_NAME_PUNCTUATION.sub('', name.upper()).split())


def download(
    folder: pathlib.Path | str | None = None,
    *,
    overwrite: bool = False,
    session: Any = None) -> dict[str, pathlib.Path]:
    """Downloads the Federal Judicial Center's files about judges.

    The files are three of those of the Biographical Directory of Article III
    Federal Judges that are organized by category (see
    https://www.fjc.gov/history/judges/biographical-directory-article-iii-federal-judges-export):
    federal judicial service, demographics, and professional careers. They
    need no account and are a few megabytes together.

    Args:
        folder: the folder to save them in. Defaults to `None`, in which case
            "judges" in `utilities.data_folder()` is used.
        overwrite: whether to download a file that is already there. The
            center adds new judges and changes as they happen. Defaults to
            `False`.
        session: an object with a `get` method like a `requests.Session`.
            Defaults to `None`, in which case a `requests.Session` is made.

    Raises:
        requests.HTTPError: if a file cannot be downloaded.

    Returns:
        The path of each file, by the name of the parameter of `build_roster`
            that takes it ("service", "demographics", and "career").

    """
    folder = pathlib.Path(folder).expanduser() if folder else _folder()
    session = utilities.open_session(session)
    return {
        kind: utilities.download_file(
            options._FJC_URL + name, folder / name, overwrite = overwrite,
            session = session)
        for kind, name in options._FJC_FILES.items()}


def name_forms(first: str, middle: str, last: str) -> list[str]:
    """Returns the ways that an opinion may write a judge's name.

    Opinions name judges by their last names, with initials or first names
    only when two judges of a court share one. For Sandra Lea Lynch, the
    forms are "SANDRA LEA LYNCH", "SANDRA L LYNCH", "SANDRA LYNCH", "S LEA
    LYNCH", "S L LYNCH", "SL LYNCH" (as "S.L." is written once its periods
    are removed), "S LYNCH", "LEA LYNCH" (for a judge who goes by a middle
    name), and "LYNCH".

    Args:
        first: the judge's first name.
        middle: the judge's middle name or names, which may be blank.
        last: the judge's last name.

    Returns:
        The forms, as `clean_name` writes them, without repeats. There are
            none if `last` is blank.

    """
    first, middle, last = clean_name(first), clean_name(middle), clean_name(last)
    if not last:
        return []
    forms = [
        f'{first} {middle} {last}',
        f'{first} {middle[:1]} {last}',
        f'{first} {last}',
        f'{first[:1]} {middle} {last}',
        f'{first[:1]} {middle[:1]} {last}',
        f'{first[:1]}{middle[:1]} {last}',
        f'{first[:1]} {last}',
        # An initial alone is not a name that a judge goes by.
        f'{middle} {last}' if len(middle) > 1 else last,
        last]
    return list(dict.fromkeys(' '.join(form.split()) for form in forms))


def vote_table(
    data: pd.DataFrame,
    roster: Roster | pd.DataFrame | pathlib.Path | str | None = None,
    *,
    outcomes: Sequence[str] | None = None) -> pd.DataFrame:
    """Returns a table with one row for each judge on each case.

    Each name in "panel_judges" is matched to a judge in a roster (see
    `Roster`), and each judge who is found gets a row, labeled by the case's
    id and the judge's place among them (such as "4567890-2"). Cases with no
    judge who is found are left out. The table is made as `amos` reshapes
    and merges data: the `lists_to_rows` technique makes a row for each name,
    and `merge_judges` (see `MergeJudges`) adds the judge that each name
    refers to. The columns are:

    | Column | Meaning |
    | --- | --- |
    | `case_id` | The case's id, which the rows of its judges share. |
    | `judge`, `judge_nid` | The judge's name and number in the roster. |
    | (the columns of `data`) | The case's columns, repeated for each judge. |
    | `judge_{attribute}` | Each attribute of the judge (see `Roster.describe`), such as `judge_party`, `judge_woman`, and `judge_age`. |
    | `colleagues_{attribute}` | The mean of the attribute among the other judges of the panel who were found. Missing if there are none. |
    | `judge_author`, `judge_concurred`, `judge_dissented` | Whether the judge wrote the opinion of the court, and wrote or joined a concurrence or a dissent. A judge who concurred in part and dissented in part did both. |
    | `vote_{outcome}` | For each outcome of the case, how the judge voted: the outcome itself, or its opposite if the judge dissented. So with "outcome_reversal", `vote_reversal` is whether the judge voted to reverse. Missing if the outcome is. |

    Args:
        data: the parsed cases, which need "panel_judges", "court_num", and
            "year". "authors", "concurring", and "dissenting" are used if
            they are there.
        roster: a roster, the table of one, or the path of a saved one.
            Defaults to `None`, in which case the roster saved in "judges" in
            `utilities.data_folder()` is used.
        outcomes: names of the columns with the outcomes of the cases.
            Defaults to `None`, in which case they are the columns whose
            names start with "outcome_".

    Raises:
        KeyError: if `data` lacks a column that is needed or named.

    Returns:
        The table, labeled by "vote_id".

    """
    _require(data, ['panel_judges', 'court_num', 'year'], 'a table of votes')
    if outcomes is None:
        outcomes = [c for c in data.columns if str(c).startswith('outcome_')]
    _require(data, outcomes, 'a table of votes')
    judges = Roster.create(roster)
    described = judges.described
    panel = _named(data, 'panel_judges', judges, ['judge', *described])
    origins = panel[_CASE].to_numpy()
    labels = data.index[origins]
    places = (panel.groupby(_CASE).cumcount() + 1).tolist()
    index = pd.Index(
        [f'{label}-{place}' for label, place in zip(
            labels, places, strict = True)],
        name = 'vote_id', dtype = object)
    votes = pd.DataFrame({
        'case_id': labels.to_numpy(),
        'judge': panel['judge'].to_numpy(),
        'judge_nid': panel['nid'].to_numpy()}, index = index)
    numbers = pd.concat([
        panel[described].add_prefix('judge_'),
        _colleagues(panel, described).add_prefix('colleagues_')], axis = 1)
    made = numbers[[
        f'{kind}_{name}' for name in described
        for kind in ('judge', 'colleagues')]].astype('float64')
    made.index = index
    # Whether each judge is one of the judges that each kind of opinion
    # names, which are found in the roster as the judges of the panel are.
    keys = pd.MultiIndex.from_arrays([origins, votes['judge_nid']])
    for column, role in _ROLES.items():
        made[role] = False
        if column in data.columns:
            acted = _named(data, column, judges)
            made[role] = keys.isin(
                pd.MultiIndex.from_arrays([acted[_CASE], acted['nid']]))
    repeated = data.iloc[origins].drop(columns = [
        c for c in data.columns if c in votes.columns or c in made.columns])
    repeated.index = index
    table = pd.concat([votes, repeated, made], axis = 1)
    dissented = table['judge_dissented'].astype('boolean')
    for column in outcomes:
        name = str(column).removeprefix('outcome_')
        table[f'vote_{name}'] = table[column].astype('boolean') ^ dissented
    return table


""" Private Functions """


def _career_flags(
    career: pd.DataFrame,
    rulebook: rules.Rulebook | pathlib.Path | str,
    nids: Iterable[int]) -> pd.DataFrame:
    """Codes the careers of judges with a rulebook.

    Args:
        career: the file of professional careers, as `_read` returns it.
        rulebook: a rulebook, or the name or path of one.
        nids: the ids of the judges to code. A judge with no career in the
            file gets what the rules make of nothing (a flag is false).

    Raises:
        KeyError: if `career` lacks a column that is needed.

    Returns:
        A table with the variables that the rules make as its columns,
            labeled by the judges' ids ("nid").

    """
    _require(
        career, ['nid', 'professional career'],
        'the file of professional careers')
    book = (
        rulebook if isinstance(rulebook, rules.Rulebook)
        else rules.Rulebook.load(rulebook))
    positions = career.assign(nid = career['nid'].astype(int)).groupby('nid')[
        'professional career'].agg('\n'.join)
    coded = {
        nid: book.apply({'career': positions.get(nid, '')})
        for nid in dict.fromkeys(nids)}
    return pd.DataFrame.from_dict(coded, orient = 'index')[book.variables]


def _circuit(court: str) -> int | None:
    """Returns the number of the circuit of a district court.

    Args:
        court: the name of the court, such as "U.S. District Court for the
            Middle District of Georgia".

    Returns:
        The number of the circuit that the court's state or territory is in
            now (12 for the District of Columbia), or `None` if the name has
            none that `_CIRCUITS` knows.

    """
    match = _DISTRICT.search(court)
    return _CIRCUITS.get(match.group(1)) if match else None


def _colleagues(panel: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    """Returns the means of columns among the other judges of each case.

    Each judge is paired with every other judge of the same case, and the
    means are taken of the pairs of each judge.

    Args:
        panel: a table with a row for each judge on each case (as `_named`
            returns it), labeled 0, 1, 2, and so on.
        columns: names of the columns of numbers to take the means of.

    Returns:
        A table with a row for each row of `panel`, in order, and a column
            for each of `columns` with the mean of the values that are known
            among the other judges of the row's case. It is missing if there
            are no other judges, or none has a value.

    """
    place = panel.groupby(_CASE).cumcount()
    judges = pd.DataFrame({_CASE: panel[_CASE], _PLACE: place})
    pairs = judges.merge(
        panel[[_CASE, *columns]].assign(**{_OTHER: place}), on = _CASE)
    pairs = pairs[pairs[_PLACE] != pairs[_OTHER]]
    means = pairs.groupby([_CASE, _PLACE])[list(columns)].mean()
    others: pd.DataFrame = means.reindex(
        pd.MultiIndex.from_frame(judges)).reset_index(drop = True)
    return others


def _folder() -> pathlib.Path:
    """Returns the folder where a roster and its sources are kept.

    Returns:
        "judges" in `utilities.data_folder()`. It is not created.

    """
    return utilities.data_folder() / 'judges'


def _named(
    data: pd.DataFrame,
    column: str,
    roster: Roster,
    columns: Sequence[str] = ()) -> pd.DataFrame:
    """Finds the judges that a column of the cases names.

    The `lists_to_rows` technique of `amos` makes a row for each name that a
    case lists, and `MergeJudges` finds the judge that each name refers to.

    Args:
        data: the parsed cases, with "court_num" and "year".
        column: name of the column that lists judges' names: lists of names,
            or the text that `cases.save_cases` writes for them (in a table
            that was loaded without its lists being restored).
        roster: the roster to find the judges in.
        columns: the columns to add for each judge, other than "nid" (see
            `MergeJudges`). Defaults to an empty `tuple`.

    Returns:
        A table with a row for each judge who was found, in the order of the
            cases and then of the names, with each judge once for a case. Its
            columns are the position of the judge's case in `data`, the
            judge's "nid", and `columns`, and its rows are labeled 0, 1, 2,
            and so on.

    """
    names = pd.DataFrame({
        _CASE: np.arange(len(data)),
        _NAME: pd.Series(data[column].tolist(), dtype = object),
        'court_num': pd.Series(data['court_num'].tolist(), dtype = object),
        'year': pd.Series(data['year'].tolist(), dtype = object)})
    listed = amos.shapers.ListsToRows().shape(
        names, column = _NAME, separator = cases._SEPARATOR.strip())
    merged = MergeJudges().apply(
        listed,
        source = roster,
        name = _NAME,
        columns = ['nid', *columns],
        prefix = None,
        indicator = _FOUND).data
    found: pd.DataFrame = merged[merged[_FOUND]].drop_duplicates(
        [_CASE, 'nid'])[[_CASE, 'nid', *columns]]
    return found.astype({'nid': 'int64'}).reset_index(drop = True)


def _number_courts(courts: Iterable[str]) -> dict[str, int]:
    """Returns the numbers that the federal rules give courts.

    The numbers come from the rules that make "court_num" for cases (in
    "federal/header.csv"), so a judge's court and a case's court always have
    the same number.

    Args:
        courts: names of courts of appeals and the Supreme Court.

    Returns:
        The number of each court that the rules number, by its name.

    """
    numbering = rules.Rulebook(rules = [
        rule for rule in rules.Rulebook.load(options._DEFAULT_JURISDICTION)
        if rule.variables == ('court_num',)])
    numbers = {}
    for court in dict.fromkeys(courts):
        number = numbering.apply({'court': court}).get('court_num')
        if number is not None:
            numbers[court] = int(number)
    return numbers


def _read(source: pd.DataFrame | pathlib.Path | str) -> pd.DataFrame:
    """Returns a file of the Federal Judicial Center as a table of text.

    Args:
        source: the path of a CSV file (in UTF-8 or Windows-1252), or a table
            that was loaded from one.

    Returns:
        The table, with the names of its columns in lower case and every cell
            as text without spaces at its ends. A missing cell is blank.

    """
    table = (
        source.copy() if isinstance(source, pd.DataFrame)
        else utilities.read_csv(source, dtype = str, keep_default_na = False))
    return pd.DataFrame({
        str(column).strip().lower(): [
            '' if pd.isna(cell) else str(cell).strip()
            for cell in table[column]]
        for column in table.columns})


def _require(data: pd.DataFrame, columns: Iterable[str], name: str) -> None:
    """Raises an error if a table lacks columns.

    Args:
        data: the table.
        columns: names of the columns that are needed.
        name: what the table is, for the message.

    Raises:
        KeyError: if a column is missing.

    """
    missing = [c for c in columns if c not in data.columns]
    if missing:
        message = f'{name} needs the columns {missing}, which it does not have'
        raise KeyError(message)


def _summarize(
    named: pd.DataFrame,
    size: int,
    how: Mapping[str, str],
    count: str | None = None) -> pd.DataFrame:
    """Returns summaries of the judges of each case.

    The `merge_summary` technique of `amos` adds summaries of the rows of
    another table that have the same keys: here, of the judges of each case.

    Args:
        named: a table with a row for each judge on each case (as `_named`
            returns it).
        size: the number of cases.
        how: the summary to take of each column of `named` to summarize,
            such as "mean" or "list", by the name of the column.
        count: name of a column to make with the number of judges of each
            case. Defaults to `None`, which makes no such column.

    Returns:
        A table with a row for each case, in order, and the summaries of its
            judges as columns. A case with no judges has an empty `list` for
            a "list", no judges for `count`, and other summaries that are
            missing.

    """
    table = amos.Dataset(pd.DataFrame({_CASE: np.arange(size)}))
    if named.empty:
        # There is nothing to summarize, so every summary is missing.
        blank = pd.DataFrame(
            np.nan, index = table.data.index,
            columns = [*how, *([count] if count else [])])
        table.replace(pd.concat([table.data, blank], axis = 1))
    else:
        amos.mergers.MergeSummary().apply(
            table, source = named, on = _CASE, how = dict(how), count = count)
    summaries = table.data.drop(columns = _CASE)
    for column, summary in how.items():
        if summary == 'list':
            summaries[column] = pd.Series(
                [found if isinstance(found, list) else []
                 for found in summaries[column].tolist()],
                index = summaries.index, dtype = object)
    if count is not None:
        summaries[count] = summaries[count].fillna(0).astype('int64')
    return summaries


def _vote_share(kind: str, votes: str) -> float | None:
    """Returns the share of senators who voted to confirm a judge.

    Args:
        kind: the type of vote: "Voice", "Roll Call", or anything else.
        votes: the ayes and nays of a roll call, such as "96/2".

    Returns:
        1 for a voice vote, the share of ayes for a roll call, or `None` if
            there was neither (or no votes are recorded).

    """
    if kind == 'Voice':
        return 1.0
    match = _VOTES.search(votes)
    if match is None:
        return None
    ayes, nays = int(match.group(1)), int(match.group(2))
    return ayes / (ayes + nays) if ayes + nays else None


def _whole(value: Any) -> int | None:
    """Returns a value as a whole number, or `None` if it is missing.

    Args:
        value: a number (or text of one) or a missing value.

    Returns:
        The number, or `None`.

    """
    return None if value is None or pd.isna(value) else int(value)


def _year(dates: pd.Series) -> pd.Series:
    """Returns the years in a column of dates.

    Args:
        dates: dates as text, such as "2014-11-20" or "11/20/2014", or years.

    Returns:
        The first four digits in a row of each cell as a whole number, which
            is missing if there are none.

    """
    found = dates.astype(str).str.extract(r'(\d{4})', expand = False)
    return pd.to_numeric(found, errors = 'coerce').astype('Int64')
