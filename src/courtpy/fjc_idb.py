"""Adds the Federal Judicial Center's Integrated Database to cases.

The Federal Judicial Center's Integrated Database (IDB) has a record of each
appeal that the federal courts of appeals have docketed, terminated, or had
pending since the 2008 fiscal year, taken from the courts' own dockets: the
type of appeal, the parties, the court and judge appealed from, the dates of
each step, the judges of the panel and the opinions each wrote, how the
appeal ended, and whether the decision was published, among much else. It
covers the twelve regional circuits (not the Federal Circuit). Its codebooks
(https://www.fjc.gov/research/idb) explain each variable.

courtpy uses the fjc-idb package (https://github.com/WithPrecedent/fjc_idb)
to download, read, and decode the database's files. `download` gets the
yearly files that are missing and keeps them outside of projects (in
"fjc_idb" in `courtpy.utilities.data_folder()`, in the layout that fjc-idb
uses). `appeals` returns the database's table of appeals for a range of
years, with fjc-idb's descriptive names for its columns (such as
"appeal_type", "docket_date", and "judgment_date") and labels in place of
many of its codes. With `district = True`, each appeal from a district court
also has the district court's record of the case that was appealed (in
columns that begin with "district_"), which fjc-idb's `merge_district`
finds.

`code_idb` (an `amos.Merger`, which is added to the `amos` library as soon
as courtpy is imported) adds the database's columns to parsed cases of the
courts of appeals, with "idb_" before their names. A case is matched by its
circuit and one of its docket numbers:

```python
import courtpy

cases = courtpy.parse("data/court_listener")
dataset = courtpy.code(cases, "code_idb", parameters = {
    "code_idb": {"columns": "appeal_type, judgment_date, published"}})
```

Contents:
    CodeIdb: adds the Integrated Database's record of each appeal to its
        case.
    appeals: returns the database's table of appeals for a range of years.
    download: downloads the database's yearly files that are missing.
    logger: the module's logger, which reports the files that are
        downloaded.

"""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import pathlib
import re
from collections.abc import Sequence
from typing import Any

import amos
import fjc_idb
import fjc_idb.main
import fjc_idb.options
import fjc_idb.rules
import numpy as np
import pandas as pd

from . import options, utilities

logger = logging.getLogger(__name__)
_COURTS: tuple[str, ...] = ("court_num", "court")
# The federal rules number the D.C. Circuit 12 (and the Federal Circuit 13,
# which the database does not cover).
_COURT_NUMBERS: dict[int, str] = {
    **{number: str(number) for number in range(1, 12)}, 12: "0"}
_DATES: tuple[str, ...] = ("date_filed", "year")
_DOCKET: re.Pattern[str] = re.compile(r"(?<!\d)(\d{2})-(\d{1,6})(?!\d)")
_DOCKETS: tuple[str, ...] = ("docket_numbers", "docket_number")
_KINDS: tuple[str, ...] = ("appellate", "civil", "criminal")
_TEXT_COLUMNS: tuple[str, ...] = (
    "circuit", "docket_num_idb", "docket_number", "district_of_origin",
    "district_courthouse", "district_docket", "criminal_defendant_number",
    "joined_docket")


@dataclasses.dataclass
class CodeIdb(amos.Merger):
    """Adds the Integrated Database's record of each appeal to its case.

    Each case of a court of appeals is matched to the database's record of
    the appeal with the same circuit and docket number. A case with several
    docket numbers (as consolidated appeals have) is matched by the first of
    them that the database has. An appeal that was reopened or remanded has
    a record for each time it ended, under the same docket number, so the
    record whose judgment is nearest the date of the decision is used (or
    the latest one, if there are no dates), and never a record of an appeal
    docketed after the decision. A case that is not matched, such as a case
    of another court or of a year that the database does not cover, gets
    missing values.

    The database's records come from fjc-idb (see `appeals`). By default,
    they are those of the years of the cases (from the year of the earliest
    decision to the year after the latest, since the database's years end on
    September 30), and the yearly files that are missing are downloaded.

    Like every merger of `amos`, it takes these parameters, never adds,
    removes, or reorders rows, and records how many cases it matched in the
    dataset's history:

    | Parameter | Meaning |
    | --- | --- |
    | `source` | The database's table of appeals: a table, or the path of a file (looked for in the current folder and then in the clerk's input folder), such as the CSV file that the `fjc-idb` command makes. Without it, the table is made from the database's yearly files (see `appeals`). |
    | `columns` | The columns of the database to add, by fjc-idb's names for them (such as "appeal_type"). By default, they are all of them. |
    | `prefix` | Text to put before the names of the added columns. It is "idb_" unless another is set. |
    | `indicator` | The name of a column to make that says whether each case was found in the database. |
    | `start_year`, `stop_year` | The first and last years of the database to use, without a `source`. They cover the cases unless they are set. |
    | `district` | Whether to add the district court's record of the case that was appealed, without a `source` (in columns that begin with "idb_district_"). It is false unless set, since the district courts' files are large. |
    | `folder` | The folder of the database's yearly files, without a `source` (looked for in the current folder and then in the clerk's input folder). It is "fjc_idb" in courtpy's data folder unless another is set. |
    | `download` | Whether to download the yearly files that are missing, without a `source`. It is true unless set. |
    | `courts`, `dockets`, `dates` | The columns of the cases with their courts, docket numbers, and dates of decision. They are "court_num" (or else "court"), "docket_numbers" (or else "docket_number"), and "date_filed" (or else "year") unless others are set. |

    Needs a court (a CourtListener court id, such as "ca1", or the number
    that the federal rules give a court of appeals) and docket numbers (such
    as "17-1234").

    """

    """ Public Methods """

    def apply(
        self,
        item: amos.Dataset,
        *,
        source: Any = None,
        columns: Sequence[str] | str | None = None,
        prefix: str | None = "idb_",
        start_year: int | None = None,
        stop_year: int | None = None,
        district: bool = False,
        folder: pathlib.Path | str | None = None,
        download: bool = True,
        **kwargs: Any) -> amos.Dataset:
        """Adds the database's record of the appeal of each case of `item`.

        Args:
            item: the dataset of parsed cases.
            source: the database's table of appeals, or the path of a file of
                it. Defaults to `None`, in which case it is made from the
                database's yearly files (see `appeals`).
            columns: the columns of the database to add, as a list or as
                text with commas between their names. Defaults to `None`,
                for all of them.
            prefix: text to put before the names of the added columns.
                Defaults to "idb_".
            start_year: the first year of the database to use, without a
                `source`. Defaults to `None`, in which case it is the year of
                the earliest decision in `item`.
            stop_year: the last year of the database to use, without a
                `source`. Defaults to `None`, in which case it is the year
                after the latest decision in `item` (but not after this
                year).
            district: whether to add the district court's record of the case
                that was appealed, without a `source`. Defaults to `False`.
            folder: the folder of the database's yearly files, without a
                `source`, which is found as the clerk finds files (see
                `utilities.locate`). Defaults to `None`, in which case
                "fjc_idb" in `utilities.data_folder()` is used.
            download: whether to download the yearly files that are missing,
                without a `source`. Defaults to `True`.
            **kwargs: other parameters of the mergers of `amos` (such as
                "indicator" and "reader"), and those of `match`.

        Raises:
            KeyError: if the cases lack a court or docket numbers, or one of
                "columns" is not in the database.
            ValueError: if the years of the cases are all before the
                database's first year, or an added column would have the
                name of a column of the cases.

        Returns:
            The dataset, with the new columns.

        """
        reader = dict(kwargs.pop("reader", None) or {})
        if source is None:
            start, stop = _years(
                item.data, start_year, stop_year,
                kwargs.get("dates", _DATES))
            district = utilities.to_bool(district)
            table = appeals(
                start,
                stop,
                utilities.locate(folder, self.clerk) if folder else None,
                district = district,
                download = utilities.to_bool(download))
            described = f"FJC IDB appeals, {start}-{stop}" + (
                ", with district records" if district else "")
        else:
            table = self.read(source, **reader)
            described = (
                str(source) if isinstance(source, str | pathlib.Path)
                else type(source).__name__)
        # A merger leaves out the columns whose names the cases have, but
        # the prefix keeps the database's columns apart from the cases'.
        chosen = (
            list(table.columns) if columns is None
            else utilities.listify(columns))
        dataset = super().apply(
            item, source = table, columns = chosen, prefix = prefix, **kwargs)
        dataset.history[-1]["source"] = described
        return dataset

    def match(
        self,
        data: pd.DataFrame,
        other: pd.DataFrame,
        *,
        courts: Sequence[str] | str = _COURTS,
        dockets: Sequence[str] | str = _DOCKETS,
        dates: Sequence[str] | str = _DATES,
        **kwargs: Any) -> Any:
        """Returns the database's record of the appeal of each case.

        Args:
            data: the parsed cases.
            other: the database's table of appeals.
            courts: the columns of `data` with the court of each case, tried
                in order: CourtListener court ids or the numbers that the
                federal rules give the courts of appeals. Defaults to
                `_COURTS`.
            dockets: the columns of `data` with the docket numbers of each
                case, as lists or as text, tried in order. Defaults to
                `_DOCKETS`.
            dates: the columns of `data` with the date (or year) of each
                decision, tried in order. Defaults to `_DATES`.
            **kwargs: not used.

        Raises:
            KeyError: if `data` has none of `courts` or `dockets`.

        Returns:
            The position of the matching row of `other` for each row of
                `data`, or -1 if there is none.

        """
        courts = utilities.listify(courts)
        dockets = utilities.listify(dockets)
        for wanted, kind in ((courts, "courts"), (dockets, "docket numbers")):
            if not any(column in data.columns for column in wanted):
                message = (
                    f"{self.name!r} needs the {kind} of the cases (one of "
                    f"the columns {wanted}), and the cases have none"
                )
                raise KeyError(message)
        records, docketed, judged = _records(other)
        circuits = _circuits(data, courts)
        decided = _decided(data, utilities.listify(dates))
        cells = [data[c].tolist() for c in dockets if c in data.columns]
        positions = np.full(len(data), -1, dtype = "int64")
        for place, circuit in enumerate(circuits):
            if circuit is None:
                continue
            when = decided[place]
            numbers = [column[place] for column in cells]
            for key in _keys(circuit, numbers):
                # An appeal docketed after the decision is not its appeal.
                found = [
                    row for row in records.get(key, [])
                    if pd.isna(when) or pd.isna(docketed[row])
                    or docketed[row] <= when]
                if found:
                    positions[place] = min(found, key = lambda row: (
                        np.inf if pd.isna(when) or pd.isna(judged[row])
                        else abs((judged[row] - when).days), -row))
                    break
        return positions

    def read(self, source: Any, **kwargs: Any) -> pd.DataFrame:
        """Returns the database's table of appeals.

        A CSV file (such as the one that the `fjc-idb` command makes) keeps
        its codes and docket numbers as text, and its dates are read as
        dates.

        Args:
            source: the path of a file of the table, or a table.
            **kwargs: parameters for loading the file (see
                `amos.Merger.read`, or `pandas.read_csv` for a CSV file).

        Returns:
            The table.

        """
        if isinstance(source, str | pathlib.Path) and (
                ".csv" in pathlib.Path(str(source)).suffixes):
            path = utilities.locate(source, self.clerk)
            header = utilities.read_csv(path, nrows = 0).columns
            table = utilities.read_csv(
                path,
                dtype = {c: "string" for c in _TEXT_COLUMNS if c in header},
                low_memory = False,
                **kwargs)
            for column in table.columns:
                if column.endswith("_date"):
                    table[column] = pd.to_datetime(
                        table[column], errors = "coerce", format = "ISO8601")
            return table
        return super().read(source, **kwargs)


def appeals(
    start_year: int | None = None,
    stop_year: int | None = None,
    folder: pathlib.Path | str | None = None,
    *,
    district: bool = False,
    download: bool = True,
    fix_city_spellings: bool = True) -> pd.DataFrame:
    """Returns the Integrated Database's table of appeals for a range of years.

    fjc-idb reads the yearly files, keeps the records of appeals that were
    still pending only from the latest year (since each year's file repeats
    them), decodes the database's codes, and gives the columns descriptive
    names. Its codebooks of the columns are in its documentation
    (https://WithPrecedent.github.io/fjc_idb).

    Args:
        start_year: the first year to include. The database's years are the
            federal government's fiscal years, which end on September 30.
            Defaults to `None`, in which case it is the database's first year
            (`options._IDB_FIRST_YEAR`).
        stop_year: the last year to include. Defaults to `None`, in which
            case it is this year.
        folder: the folder of the database's yearly files, in fjc-idb's
            layout. Defaults to `None`, in which case "fjc_idb" in
            `utilities.data_folder()` is used.
        district: whether to add the district court's record of the case
            that each appeal is from (see `fjc_idb.merge_district`), from the
            district courts' files of the same years. Defaults to `False`.
        download: whether to download the yearly files that are missing (see
            `download`). Defaults to `True`.
        fix_city_spellings: whether to correct the names of cities that the
            database's codebook misspells. Defaults to `True`.

    Raises:
        FileNotFoundError: if the folder has no yearly files of appeals for
            the years.
        ValueError: if `start_year` is after `stop_year`.

    Returns:
        The table, with a row for each appeal (or each time an appeal ended,
            if it was reopened or remanded) and columns such as "circuit",
            "docket_number" (such as "No. 17-1234"), "appeal_type",
            "docket_date", "judgment_date", "disposition", "published", and
            "judge_1" (and, with `district`, "district_match" and the
            district court's columns).

    """
    start, stop = _span(start_year, stop_year)
    root = _folder(folder)
    if download:
        _download(start, stop, root, district = district)
    if district:
        return fjc_idb.merge_district(
            start, stop, root, fix_city_spellings = fix_city_spellings,
            export = False)
    data = fjc_idb.main.import_data(
        "appellate", start, stop, fjc_idb.main._get_data_path("appellate", root))
    return fjc_idb.main.parse_data(
        data, "appellate", fix_city_spellings = fix_city_spellings)


def download(
    start_year: int | None = None,
    stop_year: int | None = None,
    folder: pathlib.Path | str | None = None,
    *,
    district: bool = False,
    overwrite: bool = False) -> list[pathlib.Path]:
    """Downloads the Integrated Database's yearly files that are missing.

    fjc-idb downloads each year's file from the Federal Judicial Center's
    website, with the codebook, and needs no account. A year whose file is
    in the folder is skipped unless `overwrite` is true. The Federal
    Judicial Center updates the file of the current year during the year, so
    overwrite it to get the latest records. Years that it has not published
    are skipped (fjc-idb logs a warning).

    Args:
        start_year: the first year to download. Defaults to `None`, in which
            case it is the database's first year (`options._IDB_FIRST_YEAR`).
        stop_year: the last year to download. Defaults to `None`, in which
            case it is this year.
        folder: the folder to keep the files in, in fjc-idb's layout
            ("appellate", "district/civil", "district/criminal", and "docs").
            Defaults to `None`, in which case "fjc_idb" in
            `utilities.data_folder()` is used.
        district: whether to also download the district courts' files of
            civil cases and criminal defendants, which `appeals` uses with
            `district = True`. They are much larger than the files of
            appeals. Defaults to `False`.
        overwrite: whether to download the files of years that are in the
            folder again. Defaults to `False`.

    Raises:
        ValueError: if `start_year` is after `stop_year`.

    Returns:
        The paths of the files that were downloaded.

    """
    start, stop = _span(start_year, stop_year)
    return _download(
        start, stop, _folder(folder), district = district,
        overwrite = overwrite)


""" Private Functions """


def _circuit(court: Any) -> str | None:
    """Returns the database's code for the circuit of a court.

    Args:
        court: a CourtListener court id (such as "ca1" or "cadc") or the
            number that the federal rules give a court of appeals (such as 1,
            or 12 for the D.C. Circuit).

    Returns:
        The database's code for the court's circuit (such as "1", or "0" for
            the D.C. Circuit), or `None` if `court` is not one of the
            database's courts.

    """
    if court is None or (not isinstance(court, str) and pd.isna(court)):
        return None
    if isinstance(court, str):
        text = court.strip().lower()
        if text in options._IDB_CIRCUITS:
            return options._IDB_CIRCUITS[text]
        if not text.isdigit():
            return None
        court = int(text)
    try:
        return _COURT_NUMBERS.get(int(court))
    except (TypeError, ValueError):
        return None


def _circuits(data: pd.DataFrame, courts: Sequence[str]) -> list[str | None]:
    """Returns the database's code for the circuit of each case.

    Args:
        data: the parsed cases.
        courts: the columns of `data` with the court of each case, tried in
            order.

    Returns:
        The code of each row of `data` (see `_circuit`), or `None` for a row
            whose court is not one of the database's.

    """
    found: list[str | None] = [None] * len(data)
    for column in courts:
        if column not in data.columns:
            continue
        for place, court in enumerate(data[column].tolist()):
            if found[place] is None:
                found[place] = _circuit(court)
    return found


def _decided(data: pd.DataFrame, dates: Sequence[str]) -> list[Any]:
    """Returns the date of each decision.

    Args:
        data: the parsed cases.
        dates: the columns of `data` with the date of each decision, or its
            year, tried in order. A year is taken to be its last day.

    Returns:
        The date of each row of `data` as a `pandas.Timestamp`, or `pd.NaT`
            if no column has it.

    """
    decided = pd.Series(pd.NaT, index = data.index, dtype = "datetime64[ns]")
    for column in dates:
        if column not in data.columns:
            continue
        values = data[column]
        if pd.api.types.is_numeric_dtype(values.dtype):
            years = pd.to_numeric(values, errors = "coerce")
            known = years.between(1, 9999)
            when = pd.to_datetime(
                years[known].astype("int64").astype(str) + "-12-31",
                errors = "coerce")
            when = when.reindex(data.index)
        else:
            when = pd.to_datetime(
                values, errors = "coerce", format = "ISO8601")
        decided = decided.fillna(when.astype("datetime64[ns]"))
    return decided.tolist()


def _download(
    start: int,
    stop: int,
    root: pathlib.Path,
    *,
    district: bool = False,
    overwrite: bool = False) -> list[pathlib.Path]:
    """Downloads the database's yearly files that are missing.

    fjc-idb is only asked for the files of a kind if some year of that kind
    is missing (or `overwrite` is true), since it reads the Federal Judicial
    Center's page of the files each time it is asked.

    Args:
        start: the first year to download.
        stop: the last year to download.
        root: the folder to keep the files in, in fjc-idb's layout.
        district: whether to also download the district courts' files of
            civil cases and criminal defendants. Defaults to `False`.
        overwrite: whether to download the files of years that are in the
            folder again. Defaults to `False`.

    Returns:
        The paths of the files that were downloaded.

    """
    saved: list[pathlib.Path] = []
    for kind in _KINDS if district else _KINDS[:1]:
        folder = fjc_idb.main._get_data_path(kind, root)
        prefix = fjc_idb.options._SOURCE_PREFIXES[kind]
        missing = [
            year for year in range(start, stop + 1)
            if overwrite or not any(
                folder.glob(f"{prefix}{year % 100:02d}*.sas7bdat"))]
        if missing:
            logger.info(
                "downloading the FJC IDB %s files of %s-%s to %s",
                kind, min(missing), max(missing), root)
            saved.extend(fjc_idb.download(
                kind, min(missing), max(missing), root,
                overwrite = overwrite))
    return saved


def _folder(folder: pathlib.Path | str | None = None) -> pathlib.Path:
    """Returns the folder where the database's files are kept.

    Args:
        folder: the folder. Defaults to `None`.

    Returns:
        `folder` (with "~" expanded), or "fjc_idb" in
            `utilities.data_folder()` if it is `None`. It is not created.

    """
    if folder:
        return pathlib.Path(folder).expanduser()
    return utilities.data_folder() / "fjc_idb"


def _keys(circuit: str, cells: Sequence[Any]) -> list[tuple[str, str, int]]:
    """Returns the circuit, year, and number of each docket number of a case.

    Args:
        circuit: the database's code for the case's circuit.
        cells: the case's cells of the columns with its docket numbers, in
            the order in which they are tried: lists of docket numbers, text
            with one or more of them, or missing values.

    Returns:
        A key for each docket number, in order and without repeats: the
            circuit, the last two digits of the year the appeal was
            docketed, and its number that year (so that "17-01234" and
            "17-1234" are the same).

    """
    keys: list[tuple[str, str, int]] = []
    for cell in cells:
        if isinstance(cell, str):
            texts = [cell]
        elif isinstance(cell, list | tuple | np.ndarray):
            texts = [str(text) for text in cell]
        else:
            continue
        for text in texts:
            for year, number in _DOCKET.findall(text):
                key = (circuit, year, int(number))
                if key not in keys:
                    keys.append(key)
    return keys


def _records(
    table: pd.DataFrame,
) -> tuple[dict[tuple[str, str, int], list[int]], list[Any], list[Any]]:
    """Returns the database's records of each docket.

    Args:
        table: the database's table of appeals. It needs "circuit" (fjc-idb's
            label or the database's code) and "docket_number" (such as "No.
            17-1234") or "docket_num_idb" (the database's form, such as
            "1701234"). "docket_date" and "judgment_date" are used if they are
            there.

    Raises:
        KeyError: if `table` lacks "circuit" or both columns of docket
            numbers.

    Returns:
        The positions of the rows of `table` for each key (as `_keys` makes
            them), and the date that each row's appeal was docketed and its
            date of judgment (`pd.NaT` if it is unknown).

    """
    if "circuit" not in table.columns or not (
            {"docket_number", "docket_num_idb"} & set(table.columns)):
        message = (
            'the table of appeals needs the columns "circuit" and '
            '"docket_number" (or "docket_num_idb"), as fjc-idb names them'
        )
        raise KeyError(message)
    codes = {label: code for code, label in fjc_idb.rules.circuits.items()}
    circuits = table["circuit"].astype("string").str.strip()
    # fjc-idb's labels (such as "First Circuit") become the database's codes,
    # and codes stay as they are.
    circuits = circuits.map(codes).fillna(circuits)
    if "docket_number" in table.columns:
        parts = table["docket_number"].astype("string").str.extract(
            r"(\d{2})-(\d+)")
    else:
        idb = table["docket_num_idb"].astype("string").str.strip().str.zfill(7)
        parts = pd.DataFrame({0: idb.str[:2], 1: idb.str[2:]})
    numbers = pd.to_numeric(parts[1], errors = "coerce")
    records: dict[tuple[str, str, int], list[int]] = {}
    for row, (circuit, year, number) in enumerate(zip(
            circuits.tolist(), parts[0].tolist(), numbers.tolist(),
            strict = True)):
        if pd.isna(circuit) or pd.isna(year) or pd.isna(number):
            continue
        records.setdefault((circuit, year, int(number)), []).append(row)

    def dates(column: str) -> list[Any]:
        """Returns a column of dates of `table`.

        Args:
            column: name of the column.

        Returns:
            Its dates, or `pd.NaT` for each row if `table` lacks it.

        """
        if column not in table.columns:
            return [pd.NaT] * len(table)
        return pd.to_datetime(
            table[column], errors = "coerce", format = "ISO8601").tolist()

    return records, dates("docket_date"), dates("judgment_date")


def _span(start_year: int | None, stop_year: int | None) -> tuple[int, int]:
    """Returns the first and last years of the database to use.

    Args:
        start_year: the first year, or `None` for the database's first year.
            A year before the database's first is taken to be its first.
        stop_year: the last year, or `None` for this year.

    Raises:
        ValueError: if the first year is after the last.

    Returns:
        The first and last years.

    """
    start = max(
        int(start_year or options._IDB_FIRST_YEAR), options._IDB_FIRST_YEAR)
    stop = int(stop_year or dt.datetime.now(tz = dt.UTC).year)
    if start > stop:
        message = (
            f"the Integrated Database's records of appeals begin in "
            f"{options._IDB_FIRST_YEAR}, so there are none for the years "
            f"{start_year}-{stop_year}"
        )
        raise ValueError(message)
    return start, stop


def _years(
    data: pd.DataFrame,
    start_year: int | None,
    stop_year: int | None,
    dates: Sequence[str] | str) -> tuple[int, int]:
    """Returns the years of the database that cover a table of cases.

    Args:
        data: the parsed cases.
        start_year: the first year, or `None` for the year of the earliest
            decision.
        stop_year: the last year, or `None` for the year after the latest
            decision (since the database's years end on September 30), but
            not after this year.
        dates: the columns of `data` with the date (or year) of each
            decision, tried in order.

    Raises:
        ValueError: if the first year is after the last (as it is if every
            decision was before the database's first year).

    Returns:
        The first and last years (see `_span`).

    """
    years = pd.Series(
        [when.year for when in _decided(data, utilities.listify(dates))
         if not pd.isna(when)],
        dtype = "int64")
    this_year = dt.datetime.now(tz = dt.UTC).year
    if start_year is None and not years.empty:
        start_year = max(int(years.min()), options._IDB_FIRST_YEAR)
    if stop_year is None and not years.empty:
        stop_year = min(int(years.max()) + 1, this_year)
    return _span(start_year, stop_year)
