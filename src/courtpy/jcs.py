"""Adds Judicial Common Space scores of judges and justices.

The Judicial Common Space (Epstein, Martin, Segal, and Westerland, "The
Judicial Common Space," 23 J.L. Econ. & Org. 303 (2007)) places judges of the
courts of appeals, justices of the Supreme Court, members of Congress, and
presidents on one scale of ideology, on which higher scores are more
conservative. Its authors post its scores at https://epstein.wustl.edu/jcs, as
an archive of files that `download` gets and unpacks. The files are kept
outside of projects (in "jcs" in `courtpy.utilities.data_folder()`):

| Kind | File | Rows |
| --- | --- | --- |
| `judges` | "judges.csv" | Each judge of a court of appeals, with the judge's name, circuit, and score. |
| `justices` | "justices.csv" | Each justice in each term of the Supreme Court. |
| `medians` | "medians.csv" | Each year, with the scores of the president and of the medians of the House, the Senate, the Supreme Court, and each court of appeals. |

The scores are added to cases in three ways:

* `judges` matches each judge to the Federal Judicial Center's number for
  the judge ("nid"), so the scores can be added to a roster
  (`courtpy.judges.Roster.from_fjc(jcs = True)`), where "jcs" is an
  attribute of each judge, like "party": `code_judges` adds "panel_jcs" (the
  mean score of each case's panel), and `merge_judges` adds "judge_jcs".
* `justices` has the Supreme Court Database's names for the justices, so the
  scores can be added to its votes (see `courtpy.scdb.votes`).
* `medians` has a row for each year, so it can be the table of
  `code_politics` (or a part of `courtpy.politics.build_table`).

```python
import courtpy

roster = courtpy.judges.Roster.from_fjc(jcs = True)
roster.save()
```

Contents:
    download: downloads and unpacks the files of Judicial Common Space
        scores.
    judges: returns the score of each judge of a court of appeals, with the
        judge's number in the Federal Judicial Center's files.
    justices: returns the score of each justice in each term.
    medians: returns the scores of the president and of the medians of
        Congress and the courts in each year.

"""

from __future__ import annotations

import logging
import pathlib
import re
import zipfile
from collections.abc import Sequence
from typing import Any

import pandas as pd

from . import judges as roster_judges
from . import options, utilities

logger = logging.getLogger(__name__)

# Errors in the 2024 file (and names that judges have changed since they were
# appointed), by the name and circuit that the file gives.
_CORRECTIONS: dict[tuple[str, int], tuple[str, int]] = {
    ("Evans, Terrance", 7): ("Evans, Terence", 7),
    ("Hunsaker, Danielle", 9): ("Forrest, Danielle", 9),
    ("Kastas, Gregory", 12): ("Katsas, Gregory", 12),
    ("McKeauge, David", 6): ("McKeague, David", 6),
    ("Moore, John P.", 10): ("Porfilio, John", 10),
    ("Owen, Priscilla", 5): ("Richman, Priscilla", 5),
    ("Shepard, Bobby", 8): ("Shepherd, Bobby", 8),
    ("Stark, Leonard", 12): ("Stark, Leonard", 13),
    ("Torruella del Valle, Juan R.", 1): ("Torruella, Juan R.", 1),
    ("Van Osterhout, Martin D.", 8): ("Van Oosterhout, Martin D.", 8)}
_MEDIANS: dict[str, str] = {
    "president": "jcs_president",
    "house_median": "jcs_house_median",
    "senate_median": "jcs_senate_median",
    "sc_median": "jcs_supreme_court_median",
    **{f"circuit_median{number}": f"jcs_circuit{number}_median"
       for number in range(1, 14)},
    **{f"circuit_sd{number}": f"jcs_circuit{number}_sd"
       for number in range(1, 14)}}
_SUFFIXES: frozenset[str] = frozenset({"JR", "SR", "I", "II", "III", "IV"})


def download(
    folder: pathlib.Path | str | None = None,
    *,
    kinds: Sequence[str] | str | None = None,
    overwrite: bool = False,
    session: Any = None) -> dict[str, pathlib.Path]:
    """Downloads and unpacks the files of Judicial Common Space scores.

    The scores are posted as one archive (about 100 kilobytes), which is
    found by its link on https://epstein.wustl.edu/jcs. Its CSV files are
    saved as "judges.csv", "justices.csv", and "medians.csv" (see the
    module's documentation), and the archive is kept beside them.

    Args:
        folder: the folder to save them in. Defaults to `None`, in which case
            "jcs" in `utilities.data_folder()` is used.
        kinds: the files to return: any of "judges", "justices", and
            "medians". Defaults to `None`, for all of them. The archive is
            downloaded if any of them is missing.
        overwrite: whether to download the archive even if the files are
            there. The scores are updated every few years. Defaults to
            `False`.
        session: an object with a `get` method like a `requests.Session`.
            Defaults to `None`, in which case a `requests.Session` is made.

    Raises:
        requests.HTTPError: if the page or the archive cannot be downloaded.
        ValueError: if one of `kinds` is not a file that courtpy knows, the
            page has no link to an archive, or the archive lacks a file.

    Returns:
        The path of each file, by its kind.

    """
    files = options._JCS_FILES
    wanted = list(files) if kinds is None else utilities.listify(kinds)
    unknown = [kind for kind in wanted if kind not in files]
    if unknown:
        message = f"kinds must be among {list(files)}, not {unknown}"
        raise ValueError(message)
    folder = pathlib.Path(folder).expanduser() if folder else _folder()
    paths = {kind: folder / f"{kind}.csv" for kind in files}
    if overwrite or not all(paths[kind].is_file() for kind in wanted):
        session = utilities.open_session(session)
        address = _address(session)
        archive = utilities.download_file(
            address, folder / address.rsplit("/", 1)[-1], overwrite = True,
            session = session)
        with zipfile.ZipFile(archive) as unpacked:
            # The archive was made on a Mac, so it has a "__MACOSX" folder of
            # hidden files with the same names.
            members = [
                name for name in unpacked.namelist()
                if name.lower().endswith(".csv") and "__MACOSX" not in name]
            for kind, pattern in files.items():
                found = [
                    name for name in members
                    if re.search(pattern, name.rsplit("/", 1)[-1], re.I)]
                if not found:
                    message = (
                        f"the archive of Judicial Common Space scores at "
                        f"{address} has no CSV file of {kind}"
                    )
                    raise ValueError(message)
                paths[kind].write_bytes(unpacked.read(found[0]))
        logger.info("unpacked the Judicial Common Space scores to %s", folder)
    return {kind: paths[kind] for kind in wanted}


def judges(
    source: pd.DataFrame | pathlib.Path | str | None = None,
    roster: roster_judges.Roster | pd.DataFrame | pathlib.Path | str | None = (
        None)) -> pd.DataFrame:
    """Returns the score of each judge of a court of appeals.

    The file names each judge as "Last, First Middle" (with any suffix, such
    as "Jr.", after another comma) and gives the number of the judge's
    circuit (12 for the District of Columbia and 13 for the Federal
    Circuit, as the federal rules number them). Each judge is found among
    the judges of that court of appeals in a roster (see
    `courtpy.judges.Roster.match`), by the judge's full name, then by first
    and last names, and then by last name alone: the first of them that
    fits one judge of that court is the judge. If none does, a suffix (such
    as "Jr.") tells apart judges of the court with the same first and last
    names. The few names and circuits that the file has wrong are corrected
    first (see `_CORRECTIONS`). A judge who served on two courts of appeals
    has a row for each.

    Args:
        source: the file of judges (see `download`), as a path or a table,
            with the columns "name", "circuit", and "jcs". Defaults to
            `None`, in which case the current file is used, which is
            downloaded unless it was before.
        roster: the roster to find the judges in: a roster, the table of one,
            or the path of a saved one. Defaults to `None`, in which case the
            roster saved in "judges" in `utilities.data_folder()` is used.

    Raises:
        KeyError: if `source` lacks a column that is needed.

    Returns:
        A table with "name" and "circuit" (as the file has them), "nid" (the
            Federal Judicial Center's number for the judge, which is missing
            if the judge was not found), and "jcs" (the judge's score).

    """
    scores = _read(source, "judges")
    _require(
        scores, ["name", "circuit", "jcs"],
        "the file of Judicial Common Space scores of judges")
    found = roster_judges.Roster.create(roster)
    rows = [
        _match(found, name, circuit)
        for name, circuit in zip(
            scores["name"].tolist(), scores["circuit"].tolist(), strict = True)]
    nids = pd.array(
        [None if row is None else found.nids([row])[0] for row in rows],
        dtype = "Int64")
    missing = sum(row is None for row in rows)
    if missing:
        logger.info(
            "%d of %d judges with Judicial Common Space scores were not found "
            "in the roster", missing, len(rows))
    return pd.DataFrame({
        "name": scores["name"].to_numpy(),
        "circuit": scores["circuit"].to_numpy(),
        "nid": nids,
        "jcs": pd.to_numeric(scores["jcs"], errors = "coerce").to_numpy()})


def justices(
    source: pd.DataFrame | pathlib.Path | str | None = None) -> pd.DataFrame:
    """Returns the score of each justice in each term.

    The justices have the names that the Supreme Court Database gives them,
    so the scores can be added to its votes (see `courtpy.scdb.votes`) with
    the `merge_keys` technique of `amos`, by "term" and "justiceName".

    Args:
        source: the file of justices (see `download`), as a path or a table,
            with the columns "term", "justiceName", and "jcs". Defaults to
            `None`, in which case the current file is used, which is
            downloaded unless it was before.

    Raises:
        KeyError: if `source` lacks a column that is needed.

    Returns:
        A table with "term" (the year in which the term began, in October),
            "justiceName" (such as "ACBarrett"), and "jcs".

    """
    scores = _read(source, "justices")
    columns = ["term", "justiceName", "jcs"]
    _require(
        scores, columns,
        "the file of Judicial Common Space scores of justices")
    return scores[columns].reset_index(drop = True)


def medians(
    source: pd.DataFrame | pathlib.Path | str | None = None) -> pd.DataFrame:
    """Returns the scores of the president and of medians in each year.

    Args:
        source: the file of medians (see `download`), as a path or a table,
            with a "year" column and columns of scores. Defaults to `None`,
            in which case the current file is used, which is downloaded
            unless it was before.

    Raises:
        KeyError: if `source` lacks a "year" column.

    Returns:
        A table with "year" and, for each of these that the file has,
            "jcs_president" (the president's score),
            "jcs_house_median", "jcs_senate_median",
            "jcs_supreme_court_median", and "jcs_circuit{n}_median" and
            "jcs_circuit{n}_sd" (the median and standard deviation of the
            scores of each court of appeals, numbered as `judges` numbers
            them). A year with more than one row in the file (as when a
            new president or Congress took office) has their means.

    """
    scores = _read(source, "medians")
    _require(
        scores, ["year"], "the file of Judicial Common Space medians")
    columns = [column for column in _MEDIANS if column in scores.columns]
    numbers = scores[columns].apply(pd.to_numeric, errors = "coerce")
    numbers["year"] = pd.to_numeric(scores["year"], errors = "coerce")
    numbers = numbers.dropna(subset = ["year"]).astype({"year": int})
    means = numbers.groupby("year")[columns].mean().reset_index()
    renamed: pd.DataFrame = means.rename(columns = _MEDIANS)
    return renamed


""" Private Functions """


def _address(session: Any) -> str:
    """Returns the address of the archive of scores.

    Args:
        session: an object with a `get` method like a `requests.Session`.

    Raises:
        requests.HTTPError: if the page cannot be downloaded.
        ValueError: if the page has no link to an archive.

    Returns:
        The first link to a ".zip" file on `options._JCS_PAGE`.

    """
    for address, _ in utilities.page_links(options._JCS_PAGE, session = session):
        if address.lower().split("?", 1)[0].endswith(".zip"):
            return address
    message = (
        f"no link to an archive of scores was found on {options._JCS_PAGE}: "
        f"download it, unpack it, and pass the path of its CSV file instead"
    )
    raise ValueError(message)


def _folder() -> pathlib.Path:
    """Returns the folder where the files are kept.

    Returns:
        "jcs" in `utilities.data_folder()`. It is not created.

    """
    return utilities.data_folder() / "jcs"


def _match(
    roster: roster_judges.Roster,
    name: Any,
    circuit: Any) -> int | None:
    """Returns the row of a roster for a judge in the file of judges.

    Args:
        roster: the roster to find the judge in.
        name: the judge's name, as "Last, First Middle" or "Last, First
            Middle, Suffix".
        circuit: the number of the judge's court of appeals.

    Returns:
        The row of the judge's service on that court, or `None` if no form
            of the name fits exactly one of its judges.

    """
    if not isinstance(name, str) or pd.isna(circuit):
        return None
    name, court = _CORRECTIONS.get((name, int(circuit)), (name, int(circuit)))
    last, _, rest = name.partition(",")
    words = roster_judges.clean_name(rest.replace(",", " ")).split()
    given = [word for word in words if word not in _SUFFIXES]
    suffixes = [word for word in words if word in _SUFFIXES]
    first = given[0] if given else ""
    middle = " ".join(given[1:])
    forms = dict.fromkeys(
        roster_judges.clean_name(form) for form in (
            f"{first} {middle} {last}", f"{first} {last}", last))
    data = roster.data
    courts = [roster_judges._whole(number) for number in data["court_num"]]
    for form in forms:
        row = roster.match(form, court)
        # A name that fits a judge of another court (such as a district
        # court, which has no number) is not this judge.
        if row is not None and courts[row] == court:
            return row
    if not suffixes:
        return None
    # Judges of a court who share their first and last names, such as Otto
    # Kerner, Sr., and Otto Kerner, Jr., are told apart by their suffixes.
    fits = [
        row for row in range(len(data))
        if courts[row] == court
        and roster_judges.clean_name(str(data["last_name"].iloc[row]))
        == roster_judges.clean_name(last)
        and roster_judges.clean_name(str(data["first_name"].iloc[row])) == first
        and roster_judges.clean_name(
            str(data["suffix"].iloc[row]) if "suffix" in data.columns else "")
        == suffixes[0]]
    judges_found = {int(data["nid"].iloc[row]) for row in fits}
    return fits[0] if len(judges_found) == 1 else None


def _read(
    source: pd.DataFrame | pathlib.Path | str | None,
    kind: str) -> pd.DataFrame:
    """Returns a table from a table or a CSV file, or one of the files.

    Args:
        source: a table (which is copied), the path of a CSV file, or `None`.
        kind: the file to use if `source` is `None`: "judges", "justices", or
            "medians". It is downloaded unless it was before (see
            `download`).

    Returns:
        The table, without the column of row numbers that the files have.

    """
    if isinstance(source, pd.DataFrame):
        table = source.copy()
    else:
        if source is None:
            source = download(kinds = kind)[kind]
        table = utilities.read_csv(source)
    numbered = [
        column for column in table.columns
        if str(column) == "" or str(column).startswith("Unnamed:")]
    return table.drop(columns = numbered)


def _require(data: pd.DataFrame, columns: Sequence[str], name: str) -> None:
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
        message = f"{name} needs the columns {missing}, which are missing"
        raise KeyError(message)
