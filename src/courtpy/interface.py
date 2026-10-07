"""Builds a table of cases from settings, and analyzes it with amos.

A study can be described in one settings file (ini, toml, json, or yaml) or
`dict`. Its "cases" section says which cases to collect and how to parse
them, and the other sections are an `amos` project that analyzes them:

```ini
[general]
seed = 43
label = outcome_reversal

[cases]
source = court_listener
download = bulk
courts = federal_appellate
start_date = 2015-01-01
end_date = 2015-12-31
save = cases.csv

[appeals_project]
appeals_workers = wrangler, analyst, critic

[wrangler]
techniques = drop_text

[analyst]
techniques = stratified, logit

[critic]
techniques = scorecard
```

`Project.create("study.ini")` collects and parses the cases, codes them, and
then applies the `amos` workers to them. The settings of the "cases" section:

| Setting | Meaning |
| --- | --- |
| `source` | "court_listener" (the default) or "lexis_nexis". |
| `folder` | Folder of the saved cases, in the root folder. Defaults to the name of the source. |
| `download` | For CourtListener: "bulk" to extract cases from the bulk data (no key needed), "api" to download them with the API (a key is needed), or "none" (the default) to use the cases already in `folder`. |
| `courts` | CourtListener court ids (such as "ca1, ca2") or groups (such as "federal_appellate") to download. |
| `start_date`, `end_date` | Dates filed to download (YYYY-MM-DD). |
| `max_cases` | Most cases to download. |
| `dockets` | For "api", whether to download each case's docket too (one more request per case). |
| `bulk_folder`, `bulk_date`, `stream` | For "bulk", where to keep the bulk files, which date's files to use, and whether to read them without saving them (see `courtpy.bulk.BulkData`). |
| `batches` | For Lexis-Nexis, files (or a folder) of many cases to divide into `folder` first. |
| `rulebooks` | Rules to parse with: built-in names (such as "federal") or CSV files or folders. Defaults to "federal". |
| `keep_text` | Whether to keep the text of the opinions in the table. Defaults to false. |
| `limit` | Most cases to parse, for trying out rules. |
| `workers` | Number of processes to parse with. Defaults to 1. |
| `coders` | Techniques to apply right after parsing. Defaults to "code_parties, code_case_type, code_outcome". Use "none" for none. |
| `save` | File to save the table to (".csv" or ".parquet"), in the root folder. |
| `reuse` | Whether to load the saved table, if there is one, instead of parsing again. Defaults to false. |

Contents:
    Project: an `amos.Project` that can collect and parse its own cases.
    build: collects, parses, and codes cases as the settings describe.
    code: applies coders to parsed cases.
    collect: downloads or divides cases as the settings describe.

"""

from __future__ import annotations

import dataclasses
import logging
import pathlib
from collections.abc import Hashable, Mapping, MutableMapping, Sequence
from typing import Any, cast

import amos
import chrisjen
import nagata
import pandas as pd

from . import bulk, cases, courtlistener, lexis, options, parsers, utilities

logger = logging.getLogger(__name__)
_NONE: frozenset[str] = frozenset({'', 'none', 'false', 'no', '0'})


@dataclasses.dataclass
class Project(amos.Project):
    """An `amos.Project` that can collect and parse its own cases.

    It works like an `amos.Project` (see its documentation). If no `item` is
    passed and the settings have a "cases" section, the cases are collected,
    parsed, and coded with `build`, and the resulting `amos.Dataset` is the
    item. Its history records the coders, followed by every technique of the
    analysis.

    """

    """ Class Methods """

    @classmethod
    def create(
        cls,
        idea: chrisjen.Idea | MutableMapping[str, Any] | pathlib.Path | str,
        *,
        clerk: nagata.FileManager | pathlib.Path | str | None = None,
        **kwargs: Any) -> Project:
        """Creates a project, building its cases first if needed.

        Args:
            idea: settings that describe the project: an `Idea`, a `dict`, or
                the path to a settings file.
            clerk: file manager, or the root folder for one. Defaults to
                `None`, in which case the "root_folder" of the "files" section
                of the settings (or the current folder) is used.
            **kwargs: other arguments for `amos.Project.create`, such as
                `item`, `name`, `id`, and `automatic`.

        Returns:
            A `Project` instance.

        """
        if not isinstance(idea, chrisjen.Idea):
            idea = chrisjen.Idea.create(idea)
        section = idea.get(options._CASES_SECTION)
        if kwargs.get('item') is None and section is not None:
            kwargs['item'] = build(
                section,
                root = _root(clerk, idea),
                parameters = idea.parameters)
        project = super().create(idea, clerk = clerk, **kwargs)
        return cast('Project', project)


def build(
    settings: Mapping[str, Any] | None = None,
    *,
    root: pathlib.Path | str = '.',
    parameters: Mapping[str, Mapping[str, Any]] | None = None,
    **overrides: Any) -> amos.Dataset:
    """Collects, parses, and codes cases as the settings describe.

    Args:
        settings: the settings of the "cases" section (see the module's
            documentation). Defaults to `None`.
        root: folder that relative paths in the settings are in. Defaults to
            the current folder.
        parameters: parameters for the coders, by their names (as in
            "{coder}_parameters" sections of a project's settings). Defaults
            to `None`.
        **overrides: settings that take precedence over `settings`.

    Raises:
        ValueError: if no cases are found.

    Returns:
        An `amos.Dataset` of the cases, without a label (an `amos.Project`
            sets it from its "general" section).

    """
    values = {**dict(settings or {}), **overrides}
    root = pathlib.Path(root)
    source = str(values.get('source', 'court_listener'))
    folder = _resolve(root, values.get('folder', source))
    save = values.get('save')
    saved = _resolve(root, save) if save else None
    if saved is not None and saved.is_file() and utilities.to_bool(
        values.get('reuse')):
        logger.info('loading the cases saved in %s', saved)
        return amos.Dataset(cases.load_cases(saved))
    collect(values, root = root)
    data = parsers.parse(
        folder,
        source = source,
        rulebooks = values.get('rulebooks') or values.get(
            'jurisdiction', options._DEFAULT_JURISDICTION),
        keep_text = utilities.to_bool(values.get('keep_text')),
        limit = _integer(values.get('limit')),
        workers = _integer(values.get('workers')) or 1)
    if data.empty:
        message = f'no {source} cases were found in {folder}'
        raise ValueError(message)
    dataset = code(
        data,
        values.get('coders', options._DEFAULT_CODERS),
        parameters = parameters)
    if saved is not None:
        cases.save_cases(dataset.data, saved)
        logger.info('saved %d cases to %s', len(dataset.data), saved)
    return dataset


def code(
    data: pd.DataFrame | amos.Dataset,
    coders: str | Sequence[str] | None = options._DEFAULT_CODERS,
    *,
    parameters: Mapping[str, Mapping[str, Any]] | None = None) -> amos.Dataset:
    """Applies coders (or any other `amos` techniques) to parsed cases.

    Args:
        data: the parsed cases, or a dataset of them.
        coders: names of the techniques to apply, in order. Defaults to
            `options._DEFAULT_CODERS`. "none" (or `None`) applies none.
        parameters: parameters for the techniques, by their names. Defaults
            to `None`.

    Returns:
        A dataset of the coded cases, whose history records each technique.

    """
    dataset = data if isinstance(data, amos.Dataset) else amos.Dataset(data)
    for name in utilities.listify(coders):
        if name.lower() in _NONE:
            continue
        kind = cast(
            'type[amos.Operation]',
            chrisjen.library.borrow(name, genre = 'operation'))
        settings = dict((parameters or {}).get(name, {}))
        technique = kind(parameters = cast('dict[Hashable, Any]', settings))
        technique.apply(dataset)
    return dataset


def collect(
    settings: Mapping[str, Any],
    *,
    root: pathlib.Path | str = '.') -> list[pathlib.Path]:
    """Downloads or divides cases as the settings describe.

    Args:
        settings: the settings of the "cases" section (see the module's
            documentation).
        root: folder that relative paths in the settings are in. Defaults to
            the current folder.

    Raises:
        ValueError: if "download" is not "bulk", "api", or "none", if a
            download is asked for a source other than CourtListener, or if no
            courts are named.

    Returns:
        The paths of the cases saved, if any.

    """
    root = pathlib.Path(root)
    source = str(settings.get('source', 'court_listener'))
    folder = _resolve(root, settings.get('folder', source))
    method = str(settings.get('download') or '').strip().lower()
    if source == 'lexis_nexis' and settings.get('batches'):
        batches = [_resolve(root, b) for b in utilities.listify(settings['batches'])]
        return lexis.split(
            [p for b in batches for p in (sorted(b.glob('*.txt')) if b.is_dir() else [b])],
            folder)
    if method in _NONE:
        return []
    if source != 'court_listener':
        message = f'cases can only be downloaded from court_listener, not {source}'
        raise ValueError(message)
    arguments: dict[str, Any] = {
        'courts': utilities.expand_courts(settings.get('courts')),
        'start_date': _date(settings.get('start_date')),
        'end_date': _date(settings.get('end_date')),
        'folder': folder,
        'max_cases': _integer(settings.get('max_cases')),
        'overwrite': utilities.to_bool(settings.get('overwrite'))}
    if method in {'bulk', 'true', 'yes', '1'}:
        bulk_folder = settings.get('bulk_folder')
        data = bulk.BulkData(
            folder = _resolve(root, bulk_folder) if bulk_folder else None,
            date = _date(settings.get('bulk_date')),
            stream = utilities.to_bool(settings.get('stream')))
        return data.extract(**arguments)
    if method == 'api':
        client = courtlistener.CourtListener()
        return client.download(
            **arguments, dockets = utilities.to_bool(settings.get('dockets')))
    message = f'download must be "bulk", "api", or "none", not {method!r}'
    raise ValueError(message)


""" Private Functions """


def _date(value: Any) -> str | None:
    """Returns a date setting as YYYY-MM-DD text, or `None` if it is blank."""
    if value in (None, ''):
        return None
    return str(value)[:10]


def _integer(value: Any) -> int | None:
    """Returns a whole-number setting, or `None` if it is blank."""
    if value in (None, ''):
        return None
    return int(value)


def _resolve(root: pathlib.Path, path: Any) -> pathlib.Path:
    """Returns `path` in `root`, unless it is absolute."""
    resolved = pathlib.Path(str(path)).expanduser()
    return resolved if resolved.is_absolute() else root / resolved


def _root(
    clerk: nagata.FileManager | pathlib.Path | str | None,
    idea: chrisjen.Idea) -> pathlib.Path:
    """Returns the root folder of a project, as `amos.Project` finds it."""
    if isinstance(clerk, nagata.FileManager):
        return pathlib.Path(clerk.root_folder)
    if clerk is not None:
        return pathlib.Path(clerk)
    files = idea.get('files') or {}
    return pathlib.Path(files.get('root_folder', amos.options._DEFAULT_ROOT))
