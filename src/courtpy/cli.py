"""Command line interface for courtpy.

Run `courtpy --help` (or `courtpy {command} --help`) in a terminal for help.

| Command | Does |
| --- | --- |
| `courtpy key set` | Stores your CourtListener API key outside of any project. |
| `courtpy key show` | Shows where your key is found (with most of it hidden). |
| `courtpy key delete` | Removes your stored key. |
| `courtpy download bulk` | Extracts cases from CourtListener's bulk data (no key needed). |
| `courtpy download api` | Downloads cases with the CourtListener API (a key is needed). |
| `courtpy split` | Divides Lexis-Nexis files of many cases into one file per case. |
| `courtpy parse` | Parses saved cases into a table (a CSV or parquet file). |
| `courtpy rules` | Checks a rulebook and lists the variables it makes. |
| `courtpy run` | Collects, parses, and analyzes cases as a settings file describes. |

Each command is a private function that takes the parsed arguments and
returns the program's exit code (0 for success). `_parser` connects each
command's name to its function.

Contents:
    main: runs a command.
    _download: downloads cases from CourtListener ("download").
    _key_delete: removes the stored API key ("key delete").
    _key_set: stores the API key ("key set").
    _key_show: shows where the API key is found ("key show").
    _parse: parses saved cases into a table ("parse").
    _parser: returns the parser of the command line arguments.
    _rules: checks a rulebook and lists its variables ("rules").
    _run: runs a study from a settings file ("run").
    _split: divides Lexis-Nexis files of many cases ("split").

"""

from __future__ import annotations

import argparse
import getpass
import logging
import pathlib
import sys
from collections.abc import Sequence

import amos

from . import (
    __version__,
    bulk,
    cases,
    coders,
    courtlistener,
    lexis,
    options,
    parsers,
    rules,
    secrets,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Runs a courtpy command.

    Messages about progress are shown unless "--quiet" is passed. Errors that
    a user can fix (such as a missing API key, a misspelled file, or an
    invalid rule) are shown as one line instead of a traceback.

    Args:
        argv: the command's arguments. Defaults to `None`, in which case the
            arguments of the program are used.

    Returns:
        0 if the command succeeded, and 1 if it failed.

    """
    parser = _parser()
    arguments = parser.parse_args(argv)
    logging.basicConfig(
        level = logging.WARNING if arguments.quiet else logging.INFO,
        format = '%(levelname)s: %(message)s')
    try:
        return int(arguments.command(arguments) or 0)
    except (
        courtlistener.CourtListenerError,
        secrets.MissingAPIKeyError,
        FileNotFoundError,
        KeyError,
        ValueError) as error:
        print(f'courtpy: {error}', file = sys.stderr)
        return 1


""" Commands """


def _download(arguments: argparse.Namespace) -> int:
    """Downloads cases from CourtListener ("courtpy download").

    "bulk" extracts the cases from the bulk data with `bulk.BulkData`, and
    "api" downloads them with `courtlistener.CourtListener` (which needs an
    API key).

    Args:
        arguments: the parsed arguments: "method" ("bulk" or "api"),
            "courts", "start", "end", "folder", "max_cases", and "overwrite",
            and "dockets" (for "api") or "bulk_folder", "bulk_date", and
            "stream" (for "bulk").

    Returns:
        0.

    """
    common = {
        'courts': arguments.courts,
        'start_date': arguments.start,
        'end_date': arguments.end,
        'folder': arguments.folder,
        'max_cases': arguments.max_cases,
        'overwrite': arguments.overwrite}
    if arguments.method == 'bulk':
        data = bulk.BulkData(
            folder = arguments.bulk_folder,
            date = arguments.bulk_date,
            stream = arguments.stream)
        saved = data.extract(**common)
    else:
        client = courtlistener.CourtListener()
        saved = client.download(**common, dockets = arguments.dockets)
    print(f'{len(saved)} cases saved in {arguments.folder}')
    return 0


def _key_delete(arguments: argparse.Namespace) -> int:  # noqa: ARG001
    """Removes the stored API key ("courtpy key delete").

    The key is removed from the keyring and the secrets file. If a key is
    still found elsewhere (in an environment variable or a `.env` file, which
    courtpy does not change), where it is found is shown.

    Args:
        arguments: the parsed arguments, which this command does not use.

    Returns:
        0.

    """
    removed = secrets.delete_api_key()
    if removed:
        print('Removed the API key from ' + ' and '.join(removed))
    else:
        print('No stored API key was found.')
    found = secrets.find_api_key()
    if found is not None:
        print(f'A key is still found in {found[1]}, which courtpy does not change.')
    return 0


def _key_set(arguments: argparse.Namespace) -> int:
    """Stores the API key ("courtpy key set").

    The key is asked for without being shown as it is typed, or read from
    standard input with "--stdin" (for scripts).

    Args:
        arguments: the parsed arguments: "store" (where to store the key) and
            "stdin" (whether to read it from standard input).

    Returns:
        0.

    """
    if arguments.stdin:
        key = sys.stdin.readline()
    else:
        key = getpass.getpass('CourtListener API key (it will not be shown): ')
    place = secrets.set_api_key(key, store = arguments.store)
    print(f'Stored the API key in {place}.')
    return 0


def _key_show(arguments: argparse.Namespace) -> int:  # noqa: ARG001
    """Shows where the API key is found, with most of it hidden.

    This is "courtpy key show". It also shows the path of the secrets file.

    Args:
        arguments: the parsed arguments, which this command does not use.

    Returns:
        0 if a key was found, and 1 (with an explanation of how to store one)
            if not.

    """
    found = secrets.find_api_key()
    if found is None:
        print(secrets.MissingAPIKeyError())
        return 1
    key, place = found
    print(f'API key {secrets.mask(key)} found in {place}.')
    print(f'(The secrets file is {secrets.secrets_path()}.)')
    return 0


def _parse(arguments: argparse.Namespace) -> int:
    """Parses saved cases into a table ("courtpy parse").

    Unless "--no-code" is passed, the default coders are applied (see
    `coders.code`) before the table is saved.

    Args:
        arguments: the parsed arguments: "folder", "source", "rulebooks",
            "keep_text", "limit", "workers", "no_code", and "output".

    Returns:
        0 if cases were parsed, and 1 if the folder had none.

    """
    data = parsers.parse(
        arguments.folder,
        source = arguments.source,
        rulebooks = arguments.rulebooks or options._DEFAULT_JURISDICTION,
        keep_text = arguments.keep_text,
        limit = arguments.limit,
        workers = arguments.workers)
    if data.empty:
        print(f'courtpy: no {arguments.source} cases in {arguments.folder}', file = sys.stderr)
        return 1
    if not arguments.no_code:
        data = coders.code(data).data
    path = cases.save_cases(data, arguments.output)
    print(f'{len(data)} cases parsed into {path}')
    return 0


def _rules(arguments: argparse.Namespace) -> int:
    """Checks a rulebook and lists the variables it makes ("courtpy rules").

    An invalid rule raises a `ValueError` naming its file and row, which
    `main` shows.

    Args:
        arguments: the parsed arguments: "rulebooks" (names of built-in
            rulebooks or paths to CSV files).

    Returns:
        0.

    """
    rulebook = rules.Rulebook.load(*arguments.rulebooks)
    print(f'{len(rulebook)} rules are valid ({rulebook.name}).')
    print(f'They search: {", ".join(rulebook.targets)}')
    if rulebook.sections:
        print(f'They make the sections: {", ".join(rulebook.sections)}')
    print(f'They make {len(rulebook.variables)} variables:')
    for name in rulebook.variables:
        print(f'  {name}')
    return 0


def _run(arguments: argparse.Namespace) -> int:
    """Collects, parses, and analyzes cases as a settings file describes.

    This is "courtpy run". The project's report is shown, and its results are
    exported if "--export" is passed (see `amos.Project.export`).

    Args:
        arguments: the parsed arguments: "settings" (the path to a settings
            file) and "export".

    Returns:
        0.

    """
    project = amos.Project.create(arguments.settings)
    print(project.report.contents if project.report else project.result)
    if arguments.export:
        folder = project.export()
        print(f'Results exported to {folder}')
    return 0


def _split(arguments: argparse.Namespace) -> int:
    """Divides Lexis-Nexis files of many cases ("courtpy split").

    Args:
        arguments: the parsed arguments: "files" (text files, or folders whose
            ".txt" files are used) and "folder" (where to save the cases).

    Returns:
        0.

    """
    files = [pathlib.Path(f) for f in arguments.files]
    expanded = [p for f in files for p in (sorted(f.glob('*.txt')) if f.is_dir() else [f])]
    saved = lexis.split(expanded, arguments.folder)
    print(f'{len(saved)} cases saved in {arguments.folder}')
    return 0


""" Private Functions """


def _parser() -> argparse.ArgumentParser:
    """Returns the parser of the command line arguments.

    Each command is a subparser whose "command" default is the function that
    runs it, which `main` calls with the parsed arguments.

    Returns:
        The parser, with the options shared by every command ("--version"
            and "--quiet") and a subparser for each command.

    """
    parser = argparse.ArgumentParser(
        prog = 'courtpy',
        description = 'Collect, parse, and analyze court opinions.')
    parser.add_argument('--version', action = 'version', version = __version__)
    parser.add_argument(
        '-q', '--quiet', action = 'store_true', help = 'show only warnings')
    commands = parser.add_subparsers(title = 'commands', required = True)

    key = commands.add_parser('key', help = 'manage the CourtListener API key')
    key_commands = key.add_subparsers(title = 'key commands', required = True)
    key_set = key_commands.add_parser('set', help = 'store the API key')
    key_set.add_argument(
        '--store', choices = ['auto', 'keyring', 'file'], default = 'auto',
        help = 'where to store it (default: the keyring, if there is one)')
    key_set.add_argument(
        '--stdin', action = 'store_true',
        help = 'read the key from standard input instead of asking')
    key_set.set_defaults(command = _key_set)
    key_commands.add_parser(
        'show', help = 'show where the key is found').set_defaults(
            command = _key_show)
    key_commands.add_parser(
        'delete', help = 'remove the stored key').set_defaults(
            command = _key_delete)

    download = commands.add_parser(
        'download', help = 'download cases from CourtListener')
    download.add_argument(
        'method', choices = ['bulk', 'api'],
        help = '"bulk" for the bulk data (no key) or "api" for the API (key)')
    download.add_argument(
        '--courts', nargs = '+', required = True,
        help = 'court ids (such as ca1) or groups (such as federal_appellate)')
    download.add_argument('--start', help = 'earliest date filed (YYYY-MM-DD)')
    download.add_argument('--end', help = 'latest date filed (YYYY-MM-DD)')
    download.add_argument(
        '--folder', default = 'court_listener',
        help = 'folder to save the cases in (default: court_listener)')
    download.add_argument(
        '--max-cases', type = int, help = 'most cases to save')
    download.add_argument(
        '--overwrite', action = 'store_true',
        help = 'download cases that were already saved')
    download.add_argument(
        '--dockets', action = 'store_true',
        help = 'api: also download dockets (one request per case)')
    download.add_argument(
        '--bulk-folder', help = 'bulk: folder to keep the bulk files in')
    download.add_argument(
        '--bulk-date', help = 'bulk: date of the files (default: newest)')
    download.add_argument(
        '--stream', action = 'store_true',
        help = 'bulk: read the files without saving them')
    download.set_defaults(command = _download)

    split = commands.add_parser(
        'split', help = 'divide Lexis-Nexis files of many cases')
    split.add_argument('files', nargs = '+', help = 'files or folders')
    split.add_argument(
        '--folder', default = 'lexis_nexis',
        help = 'folder to save the cases in (default: lexis_nexis)')
    split.set_defaults(command = _split)

    parse = commands.add_parser('parse', help = 'parse saved cases into a table')
    parse.add_argument('folder', help = 'folder of saved cases')
    parse.add_argument(
        '--source', choices = sorted(parsers.SOURCES), default = 'court_listener')
    parse.add_argument(
        '--rulebooks', nargs = '+',
        help = 'built-in rulebooks or CSV files (default: federal)')
    parse.add_argument(
        '--output', default = 'cases.csv',
        help = 'file to save the table to (default: cases.csv)')
    parse.add_argument(
        '--keep-text', action = 'store_true', help = "keep the opinions' text")
    parse.add_argument('--limit', type = int, help = 'most cases to parse')
    parse.add_argument(
        '--workers', type = int, default = 1, help = 'processes to parse with')
    parse.add_argument(
        '--no-code', action = 'store_true',
        help = 'do not code parties, case types, and outcomes')
    parse.set_defaults(command = _parse)

    check = commands.add_parser(
        'rules', help = 'check a rulebook and list its variables')
    check.add_argument(
        'rulebooks', nargs = '+',
        help = 'built-in rulebooks (federal, lexis_nexis) or CSV files')
    check.set_defaults(command = _rules)

    run = commands.add_parser(
        'run', help = 'collect, parse, and analyze cases from a settings file')
    run.add_argument('settings', help = 'settings file (ini, toml, json, yaml)')
    run.add_argument(
        '--export', action = 'store_true', help = 'export the results')
    run.set_defaults(command = _run)
    return parser


if __name__ == '__main__':
    sys.exit(main())
