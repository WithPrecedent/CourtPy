"""Adds the political context of each year to parsed cases.

Courts do not decide in isolation, so a study of their decisions may need to
account for who held the other branches of government (and the Supreme Court)
when each case was decided. `build_table` makes a table of years from three
sources:

| Columns | Meaning | Source |
| --- | --- | --- |
| `president_party` | The party of the president: -1 for Democratic and 1 for Republican. | courtpy (see `presidents`). |
| `supreme_court_median`, `supreme_court_min`, `supreme_court_max` | The Martin-Quinn scores of the median justice and of the most liberal and most conservative justices. Higher scores are more conservative. | The "court" file of https://mqscores.wustl.edu/measures.php (see `martin_quinn`). |
| `senate_median`, `house_median` | The median NOMINATE score of the members of each chamber of Congress. Higher scores are more conservative. | The file of members' ideology of https://voteview.com/data (see `nominate`). |

The sources' current files are downloaded when they are first needed (see
`download`), and kept outside of projects (in "politics" in
`courtpy.utilities.data_folder()`). The tables are combined by their years
with the `merge_keys` technique of `amos` (0.2.6 and later), which adds the
columns of another table's row that has the same keys.

`code_politics` (an `amos.Merger`, which is added to the `amos` library as
soon as courtpy is imported) is that technique with the year of each case as
its key: it adds the table's columns to each case, with "politics_" before
their names. With no table, it adds only "politics_president_party", which
needs no file. Any table with a "year" column can be used, so other measures
can be added in a spreadsheet:

```python
import courtpy

courtpy.politics.build_table().to_csv("politics.csv", index = False)
dataset = courtpy.code(cases, "code_politics", parameters = {
    "code_politics": {"source": "politics.csv"}})
```

Contents:
    CodePolitics: adds the columns of a table of years to each case.
    build_table: makes a table of the political context of each year.
    download: downloads the files of Martin-Quinn and NOMINATE scores.
    justices: returns the Martin-Quinn score of each justice in each term.
    martin_quinn: returns the Martin-Quinn scores of the Supreme Court in
        each year.
    nominate: returns the median NOMINATE scores of the Senate and the House
        in each year.
    presidents: returns the party of the president in each year.
    _CHAMBERS: the columns that `nominate` makes, by the name of each chamber
        of Congress in the file of members.
    _COURT: the columns that `martin_quinn` makes, by their names in the
        file of the court's scores.
    _JUSTICES: the columns of scores that `justices` makes, by their names
        in the file of the justices' scores.
    _PRESIDENTS: each year in which the party of the president changed, with
        the party that took office (-1 for Democratic and 1 for Republican).
    _address: returns the address of one of the sources' files.
    _folder: returns the folder where the sources' files are kept.
    _read: returns a table from a table or a CSV file, or one of the
        sources' files.
    _require: raises an error if a table lacks columns.

"""

from __future__ import annotations

import dataclasses
import pathlib
from collections.abc import Iterable, Sequence
from typing import Any

import amos
import pandas as pd

from . import options, utilities

_CHAMBERS: dict[str, str] = {
    'Senate': 'senate_median', 'House': 'house_median'}
_COURT: dict[str, str] = {
    'med': 'supreme_court_median',
    'min': 'supreme_court_min',
    'max': 'supreme_court_max'}
_JUSTICES: dict[str, str] = {
    'post_mn': 'martin_quinn', 'post_sd': 'martin_quinn_sd'}
# The party is known through the term that began in the last of these years.
_PRESIDENTS: tuple[tuple[int, int], ...] = (
    (1933, -1), (1953, 1), (1961, -1), (1969, 1), (1977, -1), (1981, 1),
    (1993, -1), (2001, 1), (2009, -1), (2017, 1), (2021, -1), (2025, 1))


@dataclasses.dataclass
class CodePolitics(amos.mergers.MergeKeys):
    """Adds the columns of a table of years to each case.

    Every column of the table (see `build_table`) is added to the cases, with
    a prefix before its name: each case gets the values of its year. A case
    whose year is missing, or is not in the table, gets missing values.

    It is the `merge_keys` technique of `amos`, with the year as the key. So
    it takes that merger's parameters, never adds, removes, or reorders
    rows, and records how many cases it matched in the dataset's history:

    | Parameter | Meaning |
    | --- | --- |
    | `source` | The table of years: the path of a data file (looked for in the current folder and then in the clerk's input folder), or a table. Without it, the table is the party of the president in each year (see `presidents`). |
    | `on` | The column of the cases to match by. It is "year" unless another is set. |
    | `other_on` | The column of the table to match by, if it has another name there. |
    | `prefix` | Text to put before the names of the table's columns. It is "politics_" unless another is set. |
    | `columns` | The columns of the table to add. By default, they are all of them. |
    | `indicator` | The name of a column to make that says whether each case's year was in the table. |
    | `duplicates` | What to do if the table has more than one row for a year: "error" (the default), "first", or "last". |

    Needs "year" (or the column named by "on").

    """

    def implement(
        self,
        item: amos.Dataset,
        *,
        source: Any = None,
        columns: Sequence[str] | str | None = None,
        prefix: str | None = 'politics_',
        on: Sequence[str] | str = 'year',
        **kwargs: Any) -> amos.Dataset:
        """Adds the political context of each case's year to `item`.

        Args:
            item: the dataset of parsed cases.
            source: a table with a row for each year, or the path of a data
                file of one. Defaults to `None`, in which case it is the
                party of the president in each year (see `presidents`).
            columns: the columns of the table to add, as a list or as text
                with commas between their names. Defaults to `None`, for
                all of them.
            prefix: text to put before the names of the table's columns.
                Defaults to "politics_".
            on: name of the column of the cases by which they are matched to
                the table. Defaults to "year".
            **kwargs: other parameters of the `merge_keys` technique of
                `amos`, such as "other_on", "indicator", and "duplicates".

        Raises:
            KeyError: if the cases or the table lack the column to match by.
            TypeError: if the column is numbers in one and text in the other.
            ValueError: if the table has more than one row for a year (and
                "duplicates" is "error").

        Returns:
            The dataset, with the new columns.

        """
        return super().implement(
            item,
            source = presidents() if source is None else source,
            columns = None if columns is None else utilities.listify(columns),
            prefix = prefix,
            on = on,
            **kwargs)


def build_table(
    supreme_court: pd.DataFrame | pathlib.Path | str | bool | None = None,
    congress: pd.DataFrame | pathlib.Path | str | bool | None = None,
    *,
    score: str = 'nominate_dim1') -> pd.DataFrame:
    """Makes a table of the political context of each year.

    Args:
        supreme_court: the file of the Supreme Court's Martin-Quinn scores by
            term (see `martin_quinn`). Defaults to `None`, in which case the
            current file is used, which is downloaded unless it was before
            (see `download`). `False` leaves the court's scores out of the
            table.
        congress: the file of the ideology of every member of Congress (see
            `nominate`). Defaults to `None`, in which case the current file
            is used, which is downloaded unless it was before. `False`
            leaves the scores of Congress out of the table.
        score: the column of `congress` with the score to use. Defaults to
            "nominate_dim1".

    Returns:
        A table with a "year" column and the columns of `presidents`,
            `martin_quinn`, and `nominate`, for every year that any of them
            has. A year that a source lacks has missing values for it.

    """
    tables = [presidents()]
    if supreme_court is not False:
        tables.append(martin_quinn(
            None if supreme_court is True else supreme_court))
    if congress is not False:
        tables.append(nominate(
            None if congress is True else congress, score = score))
    years = sorted(set().union(*(table['year'].tolist() for table in tables)))
    context = amos.Dataset(pd.DataFrame({'year': years}))
    merger = amos.mergers.MergeKeys()
    for table in tables:
        merger.apply(context, source = table, on = 'year')
    return context.data


def download(
    folder: pathlib.Path | str | None = None,
    *,
    kinds: Sequence[str] | str | None = None,
    overwrite: bool = False,
    session: Any = None) -> dict[str, pathlib.Path]:
    """Downloads the files of Martin-Quinn and NOMINATE scores.

    | Kind | File | Source |
    | --- | --- | --- |
    | `supreme_court` | "court.csv": the scores of the median justice and of the most liberal and most conservative justices in each term of the Supreme Court. | https://mqscores.wustl.edu/measures.php |
    | `justices` | "justices.csv": the score of each justice in each term. | The same page. |
    | `congress` | "HSall_members.csv": the ideology of every member of every Congress (about 6 megabytes). | https://voteview.com/data (its "Member Ideology" data for both chambers and all Congresses) |

    The Martin-Quinn scores are released each year, in a folder named for
    the year, so their files are found by the links on the page.

    Args:
        folder: the folder to save them in. Defaults to `None`, in which case
            "politics" in `utilities.data_folder()` is used.
        kinds: the files to download: any of "supreme_court", "justices", and
            "congress". Defaults to `None`, for all of them.
        overwrite: whether to download a file that is already there. The
            scores change as justices and members vote. Defaults to `False`.
        session: an object with a `get` method like a `requests.Session`.
            Defaults to `None`, in which case a `requests.Session` is made.

    Raises:
        requests.HTTPError: if a page or a file cannot be downloaded.
        ValueError: if one of `kinds` is not a file that courtpy knows, or
            the page of Martin-Quinn scores has no link to a file.

    Returns:
        The path of each file, by its kind.

    """
    files = {**options._MQ_FILES, **options._VOTEVIEW_FILES}
    wanted = list(files) if kinds is None else utilities.listify(kinds)
    unknown = [kind for kind in wanted if kind not in files]
    if unknown:
        message = f'kinds must be among {list(files)}, not {unknown}'
        raise ValueError(message)
    folder = pathlib.Path(folder).expanduser() if folder else _folder()
    session = utilities.open_session(session)
    paths = {}
    for kind in wanted:
        paths[kind] = folder / files[kind]
        if overwrite or not paths[kind].is_file():
            utilities.download_file(
                _address(kind, session), paths[kind], overwrite = True,
                session = session)
    return paths


def justices(
    source: pd.DataFrame | pathlib.Path | str | None = None) -> pd.DataFrame:
    """Returns the Martin-Quinn score of each justice in each term.

    The justices have the numbers that the Supreme Court Database gives them,
    so the scores can be added to its votes (see `courtpy.scdb.votes`) with
    the `merge_keys` technique of `amos`, by "term" and "justice".

    Args:
        source: the "justices" file of the Martin-Quinn scores (from
            https://mqscores.wustl.edu/measures.php), as a path or a table.
            It has a row for each justice in each term, with the columns
            "term", "justice", "justiceName", "post_mn", and "post_sd".
            Defaults to `None`, in which case the current file is used,
            which is downloaded unless it was before (see `download`).

    Raises:
        KeyError: if `source` lacks a column that is needed.

    Returns:
        A table with "term" (the year in which the term began, in October),
            "justice" and "justiceName" (the Supreme Court Database's number
            and name for the justice, such as 117 and "ACBarrett"),
            "martin_quinn" (the mean of the estimates of the justice's
            score), and "martin_quinn_sd" (their standard deviation).

    """
    scores = _read(source, 'justices')
    names = ['term', 'justice', 'justiceName']
    _require(
        scores, [*names, *_JUSTICES],
        'the file of the justices\' Martin-Quinn scores')
    return scores[[*names, *_JUSTICES]].rename(columns = _JUSTICES)


def martin_quinn(
    source: pd.DataFrame | pathlib.Path | str | None = None) -> pd.DataFrame:
    """Returns the Martin-Quinn scores of the Supreme Court in each year.

    Args:
        source: the "court" file of the Martin-Quinn scores (from
            https://mqscores.wustl.edu/measures.php), as a path or a table.
            It has a row for each term of the court, with the columns
            "term", "med", "min", and "max". Defaults to `None`, in which
            case the current file is used, which is downloaded unless it was
            before (see `download`).

    Raises:
        KeyError: if `source` lacks a column that is needed.

    Returns:
        A table with "year" (the year in which the term began, in October),
            "supreme_court_median", "supreme_court_min", and
            "supreme_court_max". A term with more than one row in the file
            (as 1937 and 2005 have, as "2005a" and "2005b", for the court
            before and after a justice was replaced) has their means.

    """
    scores = _read(source, 'supreme_court')
    _require(scores, ['term', *_COURT], 'the file of Martin-Quinn scores')
    year = scores['term'].astype(str).str.extract(r'(\d{4})', expand = False)
    scores = scores.assign(year = pd.to_numeric(year, errors = 'coerce'))
    scores = scores.dropna(subset = ['year']).astype({'year': int})
    means = scores.groupby('year')[list(_COURT)].mean().reset_index()
    return means.rename(columns = _COURT)


def nominate(
    source: pd.DataFrame | pathlib.Path | str | None = None,
    *,
    score: str = 'nominate_dim1') -> pd.DataFrame:
    """Returns the median NOMINATE scores of the Senate and House in each year.

    Args:
        source: the file of the ideology of every member of every Congress
            ("HSall_members.csv", which https://voteview.com/data offers as
            its "Member Ideology" data), as a path or a table. It has a row
            for each member of each Congress, with the columns "congress"
            and "chamber" and a column for each score. Defaults to `None`,
            in which case the current file is used, which is downloaded
            unless it was before (see `download`).
        score: the column with the score to use: "nominate_dim1" (the
            default, the first dimension of DW-NOMINATE, on which higher
            scores are more conservative), "nominate_dim2",
            "nokken_poole_dim1", or "nokken_poole_dim2".

    Raises:
        KeyError: if `source` lacks a column that is needed.

    Returns:
        A table with "year", "senate_median", and "house_median". A Congress
            sits for two years (the 97th sat in 1981 and 1982), so both have
            its medians.

    """
    members = _read(source, 'congress')
    _require(
        members, ['congress', 'chamber', score],
        'the file of the ideology of members of Congress')
    members = members[members['chamber'].isin(list(_CHAMBERS))]
    medians = members.assign(
        congress = members['congress'].astype(int),
        score = pd.to_numeric(members[score], errors = 'coerce')).pivot_table(
            index = 'congress', columns = 'chamber', values = 'score',
            aggfunc = 'median')
    medians = medians.reindex(columns = list(_CHAMBERS)).rename(
        columns = _CHAMBERS)
    # The first Congress met in 1789, and each sits for two years.
    first = medians.index.to_numpy() * 2 + 1787
    years = pd.concat([
        medians.assign(year = first), medians.assign(year = first + 1)])
    years = years.sort_values('year').reset_index(drop = True)
    years.columns.name = None
    return years[['year', *_CHAMBERS.values()]]


def presidents(start: int = 1933, end: int | None = None) -> pd.DataFrame:
    """Returns the party of the president in each year.

    A president who takes office in a year (in January) is the president of
    that year.

    Args:
        start: the first year. Defaults to 1933.
        end: the last year. Defaults to `None`, in which case it is the last
            year of the last term that courtpy knows the party of.

    Raises:
        ValueError: if `start` or `end` is a year that courtpy does not know
            the party of the president in.

    Returns:
        A table with "year" and "president_party" (-1 for Democratic and 1
            for Republican).

    """
    earliest, latest = _PRESIDENTS[0][0], _PRESIDENTS[-1][0] + 3
    end = latest if end is None else end
    if start < earliest or end > latest:
        message = (
            f'courtpy knows the party of the president from {earliest} to '
            f'{latest}, not from {start} to {end}'
        )
        raise ValueError(message)
    years = list(range(start, end + 1))
    parties = [
        next(party for first, party in reversed(_PRESIDENTS) if first <= year)
        for year in years]
    return pd.DataFrame({'year': years, 'president_party': parties})


""" Private Functions """


def _address(kind: str, session: Any) -> str:
    """Returns the address of one of the sources' files.

    Args:
        kind: "supreme_court", "justices", or "congress" (see `download`).
        session: an object with a `get` method like a `requests.Session`.

    Raises:
        requests.HTTPError: if the page of Martin-Quinn scores cannot be
            downloaded.
        ValueError: if that page has no link to the file.

    Returns:
        The address: for the Martin-Quinn scores, the link to the file on
            their page, and for Voteview, the file in its folder.

    """
    if kind in options._VOTEVIEW_FILES:
        return options._VOTEVIEW_URL + options._VOTEVIEW_FILES[kind]
    name = options._MQ_FILES[kind]
    links = utilities.page_links(options._MQ_PAGE, session = session)
    for address, _ in links:
        if address.rsplit('/', 1)[-1] == name:
            return address
    message = (
        f'no link to {name!r} was found on {options._MQ_PAGE}: download '
        f'the file and pass its path instead'
    )
    raise ValueError(message)


def _folder() -> pathlib.Path:
    """Returns the folder where the sources' files are kept.

    Returns:
        "politics" in `utilities.data_folder()`. It is not created.

    """
    return utilities.data_folder() / 'politics'


def _read(
    source: pd.DataFrame | pathlib.Path | str | None,
    kind: str) -> pd.DataFrame:
    """Returns a table from a table or a CSV file, or one of the sources'.

    Args:
        source: a table (which is copied), the path of a CSV file, or `None`.
        kind: the file to use if `source` is `None`: "supreme_court",
            "justices", or "congress". It is downloaded unless it was before
            (see `download`).

    Returns:
        The table.

    """
    if isinstance(source, pd.DataFrame):
        return source.copy()
    if source is None:
        source = download(kinds = kind)[kind]
    return utilities.read_csv(source)


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
        message = f'{name} needs the columns {missing}, which are missing'
        raise KeyError(message)
