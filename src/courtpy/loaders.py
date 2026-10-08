"""Techniques that load court cases into an `amos` project.

Loaders (from `amos` 0.2.3) make a project's data from a source named in its
settings, so a project with a loader needs no `item`. They are usually the
first step of the wrangler, and they work through the project's clerk (its
`nagata.FileManager`): a folder or file named by a relative path is looked
for in the current folder and then in the clerk's input folder (the
"input_folder" of the "files" section), and downloads are saved in the input
folder.

courtpy's loaders collect court cases (downloading or dividing them first, if
asked), parse them with rulebooks, and code them, so the label of a study
(such as "outcome_reversal", which a coder makes) exists when the data is
loaded:

```ini
[general]
seed = 43
label = outcome_reversal

[files]
input_folder = data

[appeals_project]
appeals_workers = wrangler, analyst, critic

[wrangler]
techniques = load_court_listener, drop_text

[load_court_listener_parameters]
source = court_listener
download = bulk
courts = federal_circuits
start_date = 2019-01-01
end_date = 2019-12-31
save = cases.csv
reuse = true
```

Every loader takes these parameters, set in its "{name}_parameters" section:

| Parameter | Meaning |
| --- | --- |
| `source` | The folder of saved cases (or, for `load_cases`, a saved table). |
| `coders` | Techniques to apply after parsing. Defaults to "code_parties, code_case_type, code_outcome" ("none" for `load_cases`). Use "none" for none. |
| `save` | File to save the coded table to (".csv" or ".parquet"). A relative path is in the clerk's interim folder. |
| `reuse` | Whether to load the saved table, if there is one, instead of collecting, parsing, and coding the cases again. Defaults to false. |
| `label`, `task`, `groups` | As in `amos`: they default to those in the "general" section. |

Contents:
    CaseLoader: base class for techniques that load court cases.
    LoadCases: loads a table of cases saved by `courtpy.save_cases`.
    LoadCourtListener: collects, parses, and codes CourtListener cases.
    LoadLexisNexis: divides, parses, and codes Lexis-Nexis cases.
    _NONE: settings that mean "none" (such as "download = none" or
        "coders = none"), in lower case.
    _date: returns a date setting as YYYY-MM-DD text.
    _integer: returns a whole-number setting.
    _parse: parses a folder of saved cases with settings that may be text.

"""

from __future__ import annotations

import abc
import dataclasses
import pathlib
from typing import Any, ClassVar

import amos
import pandas as pd

from . import (
    bulk,
    cases,
    coders,
    courtlistener,
    lexis,
    options,
    parsers,
    utilities,
)

_NONE: frozenset[str] = frozenset({'', 'none', 'false', 'no', '0'})


@dataclasses.dataclass
class CaseLoader(amos.Loader, abc.ABC):
    """Base class for techniques that load court cases.

    A subclass writes `load`, which collects and parses the cases at a source
    (a folder or file, already found with the clerk) and returns a table of
    them, one row for each case. `implement` then codes the table with the
    loader's "coders", saves it if "save" is set, and makes it the dataset
    that the rest of the workflow works on, with the label, task, and groups
    from the loader's parameters (which default to the "general" section of
    the settings). The label is checked after the coders run, so it can be a
    column that a coder makes. The dataset's history records the loader and
    then each coder.

    If "reuse" is true and the table was saved before (see "save"), the saved
    table is loaded instead, and nothing is collected, parsed, or coded again.

    Args:
        name: name used to refer to the technique in a workflow. Defaults to
            `None`, in which case it is the snake case name of the class.
        contents: not used. Defaults to `None`.
        parameters: "source", "coders", "save", "reuse", "label", "task", and
            "groups" (see the module's documentation), and parameters for
            `load`. Defaults to an empty `dict`.
        clerk: the file manager that finds folders and files. Defaults to
            `None`, in which case one for the current folder is made when it
            is needed. A loader built from a project's settings uses the
            project's clerk.

    Attributes:
        default_coders: the coders applied when "coders" is not set.
        default_source: the source used when "source" is not set, or `None`
            if a source must be set.

    """

    default_coders: ClassVar[tuple[str, ...]] = options._DEFAULT_CODERS
    default_source: ClassVar[str | None] = None

    """ Public Methods """

    def implement(self, item: amos.Dataset, **kwargs: Any) -> amos.Dataset:
        """Loads, codes, and saves the cases, as a dataset that continues `item`.

        Args:
            item: the dataset so far, whose seed and history are kept.
            **kwargs: "source", "coders", "save", "reuse", "label", "task", and
                "groups", and parameters for `load`.

        Raises:
            ValueError: if there is no "source" and the loader has no default
                source.

        Returns:
            The dataset of the coded cases.

        """
        source = kwargs.pop('source', None) or self.default_source
        if source is None:
            message = (
                f'{self.name!r} has nothing to load: set its "source" in the '
                f'"{self.name}_parameters" section of the settings'
            )
            raise ValueError(message)
        label = kwargs.pop('label', None) or item.label
        task = kwargs.pop('task', None)
        groups = kwargs.pop('groups', None) or item.groups or None
        names = kwargs.pop('coders', self.default_coders)
        save = kwargs.pop('save', None)
        reuse = utilities.to_bool(kwargs.pop('reuse', False))
        saved = self._interim(save) if save else None
        if saved is not None and reuse and saved.is_file():
            dataset = amos.Dataset(
                data = cases.load_cases(saved), seed = item.seed)
            dataset.history = item.history
            dataset.record(
                self.name, source = str(source), reused = str(saved),
                rows = len(dataset.data), columns = len(dataset.data.columns))
        else:
            data = self.load(self._locate(source), **kwargs)
            dataset = amos.Dataset(data = data, seed = item.seed)
            dataset.history = item.history
            dataset.record(
                self.name, source = str(source), rows = len(data),
                columns = len(data.columns))
            coders.code(dataset, names)
            if saved is not None:
                cases.save_cases(dataset.data, saved)
        return amos.Dataset.create(
            dataset, label = label, task = task, groups = groups)

    """ Private Methods """

    def _interim(self, path: Any) -> pathlib.Path:
        """Returns where to save a table of cases.

        Args:
            path: the "save" parameter: a path to a ".csv" or ".parquet" file.

        Returns:
            `path` itself if it is absolute, and otherwise `path` in the
                clerk's interim folder.

        """
        resolved = pathlib.Path(str(path)).expanduser()
        if resolved.is_absolute():
            return resolved
        return pathlib.Path(self._get_clerk().interim_folder) / resolved

    def _locate(self, path: Any) -> pathlib.Path:
        """Returns a folder or file named in the settings, found with the clerk.

        Args:
            path: a path from the settings, which may begin with "~" for the
                user's home folder.

        Returns:
            `path` itself if it is absolute or exists in the current folder,
                and otherwise `path` in the clerk's input folder (where a
                download saves its cases, even if the folder does not exist
                yet).

        """
        resolved = pathlib.Path(str(path)).expanduser()
        if resolved.is_absolute() or resolved.exists():
            return resolved
        return pathlib.Path(self._get_clerk().input_folder) / resolved


@dataclasses.dataclass
class LoadCases(CaseLoader):
    """Loads a table of cases saved by `courtpy.save_cases`.

    Columns that held lists are lists again, and columns of booleans and whole
    numbers with missing values get types that allow them. A saved table is
    usually coded already, so no coders are applied unless "coders" names
    some.

    """

    default_coders: ClassVar[tuple[str, ...]] = ()

    def load(self, source: Any, **kwargs: Any) -> pd.DataFrame:
        """Loads the table at `source`.

        Args:
            source: path of a CSV or parquet file saved by
                `courtpy.save_cases`, found with the clerk.
            **kwargs: not used.

        Raises:
            FileNotFoundError: if there is no file at `source`.

        Returns:
            The table, labeled by "case_id".

        """
        path = pathlib.Path(source)
        if not path.is_file():
            message = f'there is no table of cases at {path}'
            raise FileNotFoundError(message)
        return cases.load_cases(path)


@dataclasses.dataclass
class LoadCourtListener(CaseLoader):
    """Collects, parses, and codes cases from CourtListener.

    The source is a folder of cases saved by `courtpy.BulkData` or
    `courtpy.CourtListener` (by default, "court_listener" in the clerk's input
    folder). If "download" is set, the cases are downloaded into it first:
    "bulk" extracts them from CourtListener's bulk data (no API key needed),
    and "api" downloads them with the API (with the key from
    `courtpy.secrets`). Cases that were saved before are not downloaded again.

    | Parameter | Meaning |
    | --- | --- |
    | `download` | "bulk", "api", or "none" (the default). |
    | `courts` | CourtListener court ids (such as "ca1, ca2") or groups (such as "federal_appellate") to download. |
    | `start_date`, `end_date` | Dates filed to download (YYYY-MM-DD). |
    | `max_cases` | Most cases to download. |
    | `overwrite` | Whether to download cases that were already saved. |
    | `dockets` | For "api", whether to download each case's docket too. |
    | `bulk_folder`, `bulk_date`, `stream` | For "bulk", where to keep the bulk files, which date's files to use, and whether to read them without saving them. |
    | `rulebooks` | Rules to parse with. Defaults to "federal". |
    | `keep_text` | Whether to keep the text of the opinions in the table. |
    | `limit` | Most cases to parse, for trying out rules. |
    | `workers` | Number of processes to parse with. |

    """

    default_source: ClassVar[str | None] = 'court_listener'

    def load(
        self,
        source: Any,
        *,
        download: Any = None,
        courts: Any = None,
        start_date: Any = None,
        end_date: Any = None,
        max_cases: Any = None,
        overwrite: Any = False,
        dockets: Any = False,
        bulk_folder: Any = None,
        bulk_date: Any = None,
        stream: Any = False,
        rulebooks: Any = None,
        keep_text: Any = False,
        limit: Any = None,
        workers: Any = None,
        **kwargs: Any) -> pd.DataFrame:
        """Downloads the cases (if asked) and parses the folder `source`.

        Settings can be text (as a settings file gives them), so each is
        converted to the type it needs.

        Args:
            source: the folder of saved cases, found with the clerk.
            download: "bulk", "api", or "none". Defaults to `None` (none).
            courts: CourtListener court ids or groups to download. Defaults to
                `None`.
            start_date: earliest date filed to download (YYYY-MM-DD). Defaults
                to `None`.
            end_date: latest date filed to download (YYYY-MM-DD). Defaults to
                `None`.
            max_cases: most cases to download. Defaults to `None` (every case).
            overwrite: whether to download cases that were already saved.
                Defaults to `False`.
            dockets: for "api", whether to download each case's docket too.
                Defaults to `False`.
            bulk_folder: for "bulk", the folder to keep the bulk files in,
                found with the clerk. Defaults to `None`, in which case
                "bulk" in `courtpy.utilities.data_folder()` is used.
            bulk_date: for "bulk", the date of the files to use. Defaults to
                `None` (the newest).
            stream: for "bulk", whether to read the files without saving
                them. Defaults to `False`.
            rulebooks: rules to parse with. Defaults to `None`, in which case
                `options._DEFAULT_JURISDICTION` is used.
            keep_text: whether to keep the text of the opinions. Defaults to
                `False`.
            limit: most cases to parse. Defaults to `None` (every case).
            workers: number of processes to parse with. Defaults to `None`
                (one).
            **kwargs: not used.

        Raises:
            ValueError: if "download" is not "bulk", "api", or "none", or if
                no cases are found.

        Returns:
            The table of parsed cases, labeled by "case_id".

        """
        folder = pathlib.Path(source)
        method = str(download or '').strip().lower()
        if method not in _NONE:
            arguments: dict[str, Any] = {
                'courts': utilities.expand_courts(courts),
                'start_date': _date(start_date),
                'end_date': _date(end_date),
                'folder': folder,
                'max_cases': _integer(max_cases),
                'overwrite': utilities.to_bool(overwrite)}
            if method in {'bulk', 'true', 'yes', '1'}:
                data = bulk.BulkData(
                    folder = self._locate(bulk_folder) if bulk_folder else None,
                    date = _date(bulk_date),
                    stream = utilities.to_bool(stream))
                data.extract(**arguments)
            elif method == 'api':
                client = courtlistener.CourtListener()
                client.download(
                    **arguments, dockets = utilities.to_bool(dockets))
            else:
                message = (
                    f'download must be "bulk", "api", or "none", not '
                    f'{method!r}'
                )
                raise ValueError(message)
        return _parse(
            folder, 'court_listener', rulebooks = rulebooks,
            keep_text = keep_text, limit = limit, workers = workers)


@dataclasses.dataclass
class LoadLexisNexis(CaseLoader):
    """Divides, parses, and codes cases downloaded from Lexis-Nexis.

    The source is a folder of Lexis-Nexis cases, one per text file (by
    default, "lexis_nexis" in the clerk's input folder). If "batches" names
    files (or folders) of many cases, as Lexis-Nexis downloads them, they are
    divided into the source folder first (see `courtpy.lexis.split`).

    | Parameter | Meaning |
    | --- | --- |
    | `batches` | Lexis-Nexis downloads of many cases to divide first. |
    | `rulebooks` | Rules to parse with. Defaults to "federal". |
    | `keep_text` | Whether to keep the text of the opinions in the table. |
    | `limit` | Most cases to parse, for trying out rules. |
    | `workers` | Number of processes to parse with. |

    """

    default_source: ClassVar[str | None] = 'lexis_nexis'

    def load(
        self,
        source: Any,
        *,
        batches: Any = None,
        rulebooks: Any = None,
        keep_text: Any = False,
        limit: Any = None,
        workers: Any = None,
        **kwargs: Any) -> pd.DataFrame:
        """Divides any batches into the folder `source` and parses it.

        Args:
            source: the folder of Lexis-Nexis cases, found with the clerk.
            batches: files or folders of downloads of many cases, found with
                the clerk. The ".txt" files in a folder are used. Defaults to
                `None` (none).
            rulebooks: rules to parse with. Defaults to `None`, in which case
                `options._DEFAULT_JURISDICTION` is used.
            keep_text: whether to keep the text of the opinions. Defaults to
                `False`.
            limit: most cases to parse. Defaults to `None` (every case).
            workers: number of processes to parse with. Defaults to `None`
                (one).
            **kwargs: not used.

        Returns:
            The table of parsed cases, labeled by "case_id".

        """
        folder = pathlib.Path(source)
        if batches:
            files = []
            for batch in utilities.listify(batches):
                found = self._locate(batch)
                files.extend(sorted(found.glob('*.txt')) if found.is_dir() else [found])
            lexis.split(files, folder)
        return _parse(
            folder, 'lexis_nexis', rulebooks = rulebooks,
            keep_text = keep_text, limit = limit, workers = workers)


""" Private Functions """


def _date(value: Any) -> str | None:
    """Returns a date setting as YYYY-MM-DD text.

    Args:
        value: the setting: text such as "2019-01-01", or a date or date and
            time (which a settings file may have turned it into).

    Returns:
        The first ten characters of the setting as text (the date), or `None`
            if it is blank.

    """
    if value in (None, ''):
        return None
    return str(value)[:10]


def _integer(value: Any) -> int | None:
    """Returns a whole-number setting.

    Args:
        value: the setting, as a number or as text such as "500".

    Raises:
        ValueError: if the setting is not a whole number.

    Returns:
        The number, or `None` if the setting is blank.

    """
    if value in (None, ''):
        return None
    return int(value)


def _parse(
    folder: pathlib.Path,
    source: str,
    *,
    rulebooks: Any,
    keep_text: Any,
    limit: Any,
    workers: Any) -> pd.DataFrame:
    """Parses a folder of saved cases with settings that may be text.

    Args:
        folder: the folder of saved cases.
        source: the name of their source, such as "court_listener".
        rulebooks: rules to parse with, or `None` (or blank) for
            `options._DEFAULT_JURISDICTION`.
        keep_text: whether to keep the text of the opinions.
        limit: most cases to parse, or `None` for every case.
        workers: number of processes to parse with, or `None` for one.

    Raises:
        ValueError: if no cases are found in `folder`.

    Returns:
        The table of parsed cases, labeled by "case_id".

    """
    data = parsers.parse(
        folder,
        source = source,
        rulebooks = rulebooks or options._DEFAULT_JURISDICTION,
        keep_text = utilities.to_bool(keep_text),
        limit = _integer(limit),
        workers = _integer(workers) or 1)
    if data.empty:
        message = f'no {source} cases were found in {folder}'
        raise ValueError(message)
    return data
