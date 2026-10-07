"""Techniques that code variables from parsed cases.

Each coder is an `amos.Cleaner`, so it is added to the `amos` (and
`chrisjen`) library as soon as courtpy is imported, and it can be named in the
settings of any `amos` worker. courtpy also runs the coders named in the
"coders" setting of the "cases" section (by default, `code_parties`,
`code_case_type`, and `code_outcome`, in that order) right after parsing, so
that a label such as "outcome_reversal" exists before an analysis begins.

The coders use the columns made by the federal rules (such as
"party1_appellant" and "disposition_reverse"). With other rules, make columns
with the same names or write your own coder.

Contents:
    CodeCaseType: decides whether each case is criminal or civil.
    CodeOutcome: decides whether each decision reversed the court below, and
        which side won.
    CodeParties: completes the roles of the parties.
    DropText: removes columns that models cannot use, such as text and lists.

"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from typing import Any

import amos
import pandas as pd

# The roles in each pair are opposites: if one party is an appellant, the
# other is an appellee, and so on.
_OPPOSITES: tuple[tuple[str, str], ...] = (
    ('appellant', 'appellee'),
    ('petitioner', 'respondent'),
    ('plaintiff', 'defendant'))
_SIDES: tuple[tuple[str, str], ...] = (('party1', 'party2'), ('party2', 'party1'))


@dataclasses.dataclass
class CodeParties(amos.Cleaner):
    """Completes the roles of the parties and finds who brought the appeal.

    A caption often gives one party's role but not the other's ("United
    States v. Smith, Defendant-Appellant"). Because the roles in each pair are
    opposites, a party is coded as an appellee if the other party is an
    appellant (and so on for petitioners and respondents, and for plaintiffs
    and defendants).

    It also adds "party1_appealing" and "party2_appealing" (the party is an
    appellant or petitioner and not also an appellee or respondent) and
    "party1_defending" and "party2_defending" (the reverse). Both are false
    for a party whose role is unknown.

    Needs the columns "party1_{role}" and "party2_{role}" for each role:
    appellant, appellee, petitioner, respondent, plaintiff, and defendant.

    """

    def clean(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Completes the roles of the parties.

        Args:
            data: the parsed cases.
            **kwargs: not used.

        Returns:
            The cases, with completed roles.

        """
        roles = [r for pair in _OPPOSITES for r in pair]
        needed = [f'{s}_{r}' for s in ('party1', 'party2') for r in roles]
        _require(data, needed, self.name)
        original = {c: _flags(data, c) for c in needed}
        for first, second in _OPPOSITES:
            for side, other in _SIDES:
                data[f'{side}_{first}'] = (
                    original[f'{side}_{first}'] | original[f'{other}_{second}'])
                data[f'{side}_{second}'] = (
                    original[f'{side}_{second}'] | original[f'{other}_{first}'])
        for side, _ in _SIDES:
            appealing = data[f'{side}_appellant'] | data[f'{side}_petitioner']
            defending = data[f'{side}_appellee'] | data[f'{side}_respondent']
            data[f'{side}_appealing'] = appealing & ~defending
            data[f'{side}_defending'] = defending & ~appealing
        return data


@dataclasses.dataclass
class CodeCaseType(amos.Cleaner):
    """Decides whether each case is criminal or civil.

    A case is criminal ("type_criminal") if the United States is a party and
    there is a sign of a criminal case: a criminal docket number or history,
    a United States Attorney as counsel, or a criminal issue in the opinion
    (any "criminal_..." column). The United States is then the prosecution,
    and the other party is the criminal defendant. In a civil case, the
    plaintiffs and defendants are coded as civil plaintiffs and defendants.

    Adds "type_criminal", and for each party, "party1_prosecution",
    "party1_criminal_defendant", "party1_civil_plaintiff", and
    "party1_civil_defendant" (and the same for "party2").

    Needs "party1_united_states", "party2_united_states", "party1_plaintiff",
    "party2_plaintiff", "party1_defendant", and "party2_defendant". Uses
    "docket_criminal", "counsel_us_attorney", and the "criminal_..." columns
    if they exist.

    """

    def clean(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Codes each case as criminal or civil.

        Args:
            data: the parsed cases.
            **kwargs: not used.

        Returns:
            The cases, with the new columns.

        """
        needed = [
            f'{side}_{role}' for side in ('party1', 'party2')
            for role in ('united_states', 'plaintiff', 'defendant')]
        _require(data, needed, self.name)
        signals = [
            c for c in data.columns
            if c in ('docket_criminal', 'counsel_us_attorney')
            or c.startswith('criminal_')]
        criminal_signal = pd.Series(False, index = data.index)
        for column in signals:
            criminal_signal |= _flags(data, column)
        government = (
            _flags(data, 'party1_united_states')
            | _flags(data, 'party2_united_states'))
        criminal = government & criminal_signal
        data['type_criminal'] = criminal
        for side, other in _SIDES:
            prosecution = criminal & _flags(data, f'{side}_united_states')
            data[f'{side}_prosecution'] = prosecution
            data[f'{side}_criminal_defendant'] = (
                criminal & _flags(data, f'{other}_united_states') & ~prosecution)
            data[f'{side}_civil_plaintiff'] = ~criminal & (
                _flags(data, f'{side}_plaintiff')
                | _flags(data, f'{other}_defendant'))
            data[f'{side}_civil_defendant'] = ~criminal & (
                _flags(data, f'{side}_defendant')
                | _flags(data, f'{other}_plaintiff'))
        return data


@dataclasses.dataclass
class CodeOutcome(amos.Cleaner):
    """Decides whether each decision reversed the court below, and who won.

    A decision is a reversal ("outcome_reversal") if it reversed, vacated, or
    remanded. The disposition in the header (or in CourtListener's data) is
    used when there is one, because the opinion may mention reversals that are
    not its own. Otherwise, the opinion's own words ("we reverse") are used.

    Then, for each party whose role is known (see `CodeParties`), the party
    won ("outcome_party1_won") if it brought the appeal and the decision was
    reversed, or it defended the appeal and the decision was not reversed.
    With `CodeCaseType`, the winner is also coded by side:
    "outcome_criminal_defendant_won", "outcome_prosecution_won",
    "outcome_civil_plaintiff_won", and "outcome_civil_defendant_won", and
    "appeal_by_defendant" says whether the (criminal or civil) defendant
    brought the appeal.

    Values that cannot be known (such as who won when neither party's role is
    known, or whether the criminal defendant won a civil case) are missing
    (`pd.NA`). Filter the cases before using such a column as a label (for
    example, with the `filter_rows` technique and the query "type_criminal").

    Needs "disposition_reverse", "disposition_vacate", "disposition_remand",
    "opinion_reverse", "opinion_vacate", and "opinion_remand". Uses the
    columns made by `CodeParties` and `CodeCaseType` if they exist.

    """

    def clean(self, data: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        """Codes the outcome of each decision.

        Args:
            data: the parsed cases.
            **kwargs: not used.

        Returns:
            The cases, with the new columns.

        """
        rulings = ('reverse', 'vacate', 'remand')
        _require(
            data,
            [f'{p}_{r}' for p in ('disposition', 'opinion') for r in rulings],
            self.name)
        header = pd.Series(False, index = data.index)
        opinion = pd.Series(False, index = data.index)
        for ruling in rulings:
            header |= _flags(data, f'disposition_{ruling}')
            opinion |= _flags(data, f'opinion_{ruling}')
        if 'disposition' in data.columns:
            stated = data['disposition'].fillna('').astype(str).str.strip() != ''
        else:
            stated = pd.Series(False, index = data.index)
        reversal = header.where(stated, opinion)
        data['outcome_reversal'] = reversal
        if not {'party1_appealing', 'party2_appealing'} <= set(data.columns):
            return data
        won = {}
        for side, _ in _SIDES:
            outcome = pd.Series(pd.NA, index = data.index, dtype = 'boolean')
            appealing = _flags(data, f'{side}_appealing')
            defending = _flags(data, f'{side}_defending')
            outcome[appealing] = reversal[appealing]
            outcome[defending] = ~reversal[defending]
            won[side] = outcome
            data[f'outcome_{side}_won'] = outcome
        categories = {
            'criminal_defendant': 'criminal_defendant',
            'prosecution': 'prosecution',
            'civil_plaintiff': 'civil_plaintiff',
            'civil_defendant': 'civil_defendant'}
        if all(f'party1_{c}' in data.columns for c in categories.values()):
            for name, column in categories.items():
                outcome = pd.Series(pd.NA, index = data.index, dtype = 'boolean')
                for side, _ in _SIDES:
                    present = _flags(data, f'{side}_{column}')
                    outcome[present] = won[side][present]
                data[f'outcome_{name}_won'] = outcome
            by_defendant = pd.Series(pd.NA, index = data.index, dtype = 'boolean')
            for side, _ in _SIDES:
                defendant = (
                    _flags(data, f'{side}_criminal_defendant')
                    | _flags(data, f'{side}_civil_defendant'))
                known = defendant & (
                    _flags(data, f'{side}_appealing')
                    | _flags(data, f'{side}_defending'))
                by_defendant[known] = _flags(data, f'{side}_appealing')[known]
            data['appeal_by_defendant'] = by_defendant
        return data


@dataclasses.dataclass
class DropText(amos.Cleaner):
    """Removes columns that models cannot use, such as text and lists.

    Removes every column of text, lists, or dates stored as text (such as
    "case_name", "panel_judges", and "date_filed"), so that the cases can be
    analyzed. Categories, numbers, and booleans are kept. The label and groups
    are always kept.

    """

    def clean(
        self,
        data: pd.DataFrame,
        keep: Sequence[str] | str | None = None,
        label: str | None = None,
        groups: Sequence[str] = (),
        **kwargs: Any) -> pd.DataFrame:
        """Removes columns of text and lists.

        Args:
            data: the cases.
            keep: names of columns to keep even if they hold text. Defaults to
                `None`.
            label: name of the label, which is always kept. Defaults to
                `None`.
            groups: names of the group columns, which are always kept.
                Defaults to an empty tuple.
            **kwargs: not used.

        Returns:
            The cases, without text and lists.

        """
        names = [keep] if isinstance(keep, str) else list(keep or [])
        missing = [c for c in names if c not in data.columns]
        if missing:
            message = f'the columns {missing} are not in the data'
            raise KeyError(message)
        kept = {*names, *(c for c in [label, *groups] if c)}
        dropped = [
            c for c in data.columns
            if c not in kept and _is_text(data[c])]
        return data.drop(columns = dropped)

    def implement(self, item: amos.Dataset, **kwargs: Any) -> amos.Dataset:
        """Removes the columns of text and lists of `item`.

        Args:
            item: the dataset to clean.
            **kwargs: parameters for `clean`.

        Returns:
            The cleaned dataset.

        """
        return super().implement(
            item, **{'label': item.label, 'groups': item.groups, **kwargs})


""" Private Functions """


def _flags(data: pd.DataFrame, column: str) -> pd.Series:
    """Returns a column as booleans, with missing values (or columns) false.

    Args:
        data: the cases.
        column: name of the column.

    Returns:
        The column as plain booleans.

    """
    if column not in data.columns:
        return pd.Series(False, index = data.index)
    values = data[column]
    if values.dtype == object:
        values = values.map(
            lambda v: str(v).strip().lower() in {'true', 't', '1', 'yes'}
            if isinstance(v, str) else bool(v) if pd.notna(v) else False)
    return values.astype('boolean').fillna(False).astype(bool)


def _is_text(column: pd.Series) -> bool:
    """Returns whether a column holds text, lists, or other objects.

    A column of `object` type that holds only booleans (and missing values)
    is not text.

    """
    if isinstance(column.dtype, pd.CategoricalDtype):
        return False
    if pd.api.types.is_bool_dtype(column.dtype) or pd.api.types.is_numeric_dtype(column.dtype):
        return False
    if pd.api.types.is_datetime64_any_dtype(column.dtype):
        return False
    values = column.dropna()
    return not (len(values) > 0 and values.map(lambda v: isinstance(v, bool)).all())


def _require(data: pd.DataFrame, columns: Sequence[str], name: str) -> None:
    """Raises an error if any of `columns` is not in `data`.

    Args:
        data: the cases.
        columns: names of the columns that are needed.
        name: name of the technique that needs them.

    Raises:
        KeyError: if a column is missing.

    """
    missing = [c for c in columns if c not in data.columns]
    if missing:
        message = (
            f'{name!r} needs the columns {missing}, which the federal rules '
            f'make. Parse the cases with them (or with rules that make the '
            f'same columns) first.'
        )
        raise KeyError(message)
