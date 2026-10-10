"""Court cases and tables of them.

Contents:
    Case: the text and information of one case, ready to be parsed.
    load_cases: loads a table of parsed cases.
    save_cases: saves a table of parsed cases.
    tidy: gives columns with missing values the right type.
    _LIST_COLUMNS_FILE: the name of the file, saved beside a CSV file of
        cases, that lists the columns that held lists, so that `load_cases`
        can restore them. "{stem}" is replaced with the name of the CSV file
        without its extension.
    _SEPARATOR: the text between the items of a list when a table is saved as
        a CSV file.
    _WHOLE_NUMBERS: columns of whole numbers that may be missing for some
        cases, which `tidy` gives the "Int64" type.
    _is_list: returns whether a value is a `list` or `tuple`.
    _unjoin: returns text saved by `save_cases` as a list again.

"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from typing import Any

import pandas as pd

_SEPARATOR: str = '; '
_LIST_COLUMNS_FILE: str = '{stem}.lists.json'
_WHOLE_NUMBERS: frozenset[str] = frozenset({
    'year', 'court_num', 'cluster_id', 'docket_id', 'author_id',
    'citation_count'})


@dataclasses.dataclass
class Case:
    """The text and information of one case, ready to be parsed.

    Readers (such as `courtlistener.read_case` and `lexis.read_case`) make
    cases from files, and a `Parser` turns each case into a row of a table.

    Args:
        id: a unique name for the case, which becomes the row's label in the
            table (such as the CourtListener cluster id or the file name).
        source: where the case came from: "court_listener" or "lexis_nexis".
        sections: sections of text that rules search, by name (such as
            "party", "disposition", and "opinion"). Defaults to an empty
            `dict`.
        metadata: information that needs no parsing (such as the date filed
            from CourtListener), by the name of its column. These values are
            used instead of anything rules find with the same names. Defaults
            to an empty `dict`.
        path: the file the case came from. Defaults to `None`.

    """

    id: str
    source: str
    sections: dict[str, str] = dataclasses.field(default_factory = dict)
    metadata: dict[str, Any] = dataclasses.field(default_factory = dict)
    path: pathlib.Path | None = None


def load_cases(path: pathlib.Path | str) -> pd.DataFrame:
    """Loads a table of parsed cases saved by `save_cases`.

    Args:
        path: path to a CSV or parquet file.

    Returns:
        The table, labeled by "case_id". Columns that held lists are lists
            again, and the ids of cases are text, as they were (in the labels
            and in a "case_id" column, which a table of judges' votes has).

    """
    path = pathlib.Path(path)
    if path.suffix.lower() == '.parquet':
        data = pd.read_parquet(path)
        for column in data.columns:
            if data[column].map(lambda v: hasattr(v, 'tolist')).any():
                data[column] = data[column].map(
                    lambda v: list(v) if hasattr(v, 'tolist') else v)
        return data
    data = pd.read_csv(
        path, index_col = 0, encoding = 'utf-8', dtype = {'case_id': str})
    data.index = data.index.astype(str)
    record = path.with_name(_LIST_COLUMNS_FILE.format(stem = path.stem))
    if record.is_file():
        for column in json.loads(record.read_text(encoding = 'utf-8')):
            if column in data.columns:
                data[column] = data[column].map(_unjoin)
    return tidy(data)


def save_cases(data: pd.DataFrame, path: pathlib.Path | str) -> pathlib.Path:
    """Saves a table of parsed cases.

    A CSV file can be opened in a spreadsheet. Lists (such as the judges on a
    panel) are written as text, with "; " between the items, and the names of
    those columns are saved beside the file (as "{name}.lists.json") so that
    `load_cases` can restore them. A parquet file (which needs the optional
    pyarrow package) keeps lists and types as they are.

    Args:
        data: the table.
        path: path to save it to, ending in ".csv" or ".parquet". Its folder
            is created if needed.

    Returns:
        The path the table was saved to.

    """
    path = pathlib.Path(path)
    path.parent.mkdir(parents = True, exist_ok = True)
    if path.suffix.lower() == '.parquet':
        data.to_parquet(path)
        return path
    lists = [c for c in data.columns if data[c].map(_is_list).any()]
    copy = data.copy()
    for column in lists:
        copy[column] = copy[column].map(
            lambda v: _SEPARATOR.join(map(str, v)) if _is_list(v) else v)
    copy.to_csv(path, encoding = 'utf-8')
    record = path.with_name(_LIST_COLUMNS_FILE.format(stem = path.stem))
    record.write_text(json.dumps(lists), encoding = 'utf-8')
    return path


def tidy(data: pd.DataFrame) -> pd.DataFrame:
    """Gives columns of booleans or whole numbers with missing values a type.

    `pandas` stores a column of booleans with missing values (such as
    "published", which is unknown for some cases) as plain objects, and a
    column of whole numbers with missing values (such as "year") as decimals.
    Such columns become the "boolean" and "Int64" types, which allow missing
    values.

    Args:
        data: a table of cases.

    Returns:
        The table, with those columns changed.

    """
    for column in data.columns:
        values = data[column].dropna()
        if len(values) == 0:
            continue
        if data[column].dtype == object and values.map(
            lambda v: isinstance(v, bool)).all():
            data[column] = data[column].astype('boolean')
        elif (
            column in _WHOLE_NUMBERS
            and pd.api.types.is_float_dtype(data[column].dtype)
            and (values == values.round()).all()):
            data[column] = data[column].astype('Int64')
    return data


""" Private Functions """


def _is_list(value: Any) -> bool:
    """Returns whether `value` is a `list` or `tuple`.

    Args:
        value: a value in a table of cases.

    Returns:
        Whether `value` is a `list` or `tuple` (such as the names made by a
            `names` rule).

    """
    return isinstance(value, list | tuple)


def _unjoin(value: Any) -> list[str]:
    """Returns text saved by `save_cases` as a list again.

    Args:
        value: a cell of a column that held lists, read from a CSV file: the
            items joined by `_SEPARATOR`, or a missing value for an empty
            list.

    Returns:
        The items, or an empty `list` if `value` is not text or is blank.

    """
    if not isinstance(value, str) or not value:
        return []
    return value.split(_SEPARATOR)
