"""Keeps the CourtListener API key secret, outside of projects and packages.

The key is never written in a project's settings or in courtpy itself, so it
cannot be shared or committed by accident. Store it once, and courtpy finds it
whenever it is needed:

```python
import courtpy

courtpy.secrets.set_api_key("your-key")
```

or, from a terminal (which asks for the key without showing it):

```sh
courtpy key set
```

`get_api_key` looks for the key in these places, in order, and uses the first
one it finds:

1. The `COURTLISTENER_API_KEY` environment variable.
2. The system keyring: Windows Credential Manager, the macOS Keychain, or the
   Secret Service on Linux (if one is running).
3. The secrets file in courtpy's configuration folder (see `secrets_path`),
   which is in the user's home folder, not in any project.
4. A `.env` file in the current folder with a line such as
   `COURTLISTENER_API_KEY=your-key`. Keep such a file out of version control
   (courtpy's own `.gitignore` excludes it).

Contents:
    MissingAPIKeyError: raised when no API key can be found.
    delete_api_key: removes the stored API key.
    find_api_key: returns the API key and where it was found.
    get_api_key: returns the API key.
    mask: hides most of a key so that it can be shown.
    secrets_path: returns the path of the secrets file.
    set_api_key: stores the API key.

"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import pathlib
import sys
import tomllib
from typing import Any

from . import options, utilities

logger = logging.getLogger(__name__)
# Where `set_api_key` can store the key.
_STORES: tuple[str, ...] = ('auto', 'keyring', 'file')
# Section and setting of the secrets file that hold the key.
_FILE_SECTION: str = 'court_listener'
_FILE_SETTING: str = 'api_key'


class MissingAPIKeyError(LookupError):
    """Raised when no CourtListener API key can be found.

    Its message explains how to get and store a key.

    """

    def __init__(self) -> None:  # noqa: D107
        message = (
            'no CourtListener API key was found. Get one by signing in to '
            'https://www.courtlistener.com and opening your profile\'s "API" '
            'page. Then store it with "courtpy key set" (in a terminal) or '
            '"courtpy.secrets.set_api_key(...)" (in Python), or set the '
            f'{options._ENV_API_KEY} environment variable. Bulk data does '
            'not need a key.'
        )
        super().__init__(message)


def delete_api_key() -> list[str]:
    """Removes the API key from the keyring and the secrets file.

    A key in an environment variable or a `.env` file is not changed.

    Returns:
        Descriptions of the places the key was removed from.

    """
    removed = []
    backend = _keyring()
    if backend is not None:
        try:
            if backend.get_password(*_keyring_names()) is not None:
                backend.delete_password(*_keyring_names())
                removed.append('the system keyring')
        except Exception as error:  # noqa: BLE001
            logger.debug('the keyring could not be changed: %s', error)
    path = secrets_path()
    contents = _read_file(path)
    if contents.get(_FILE_SECTION, {}).get(_FILE_SETTING):
        del contents[_FILE_SECTION][_FILE_SETTING]
        _write_file(path, contents)
        removed.append(str(path))
    return removed


def find_api_key() -> tuple[str, str] | None:
    """Returns the API key and a description of where it was found.

    Returns:
        The key and where it was found, or `None` if there is no key in any of
            the places that `get_api_key` looks.

    """
    value = os.environ.get(options._ENV_API_KEY, '').strip()
    if value:
        return value, f'the {options._ENV_API_KEY} environment variable'
    backend = _keyring()
    if backend is not None:
        try:
            value = (backend.get_password(*_keyring_names()) or '').strip()
        except Exception as error:  # noqa: BLE001
            logger.debug('the keyring could not be read: %s', error)
            value = ''
        if value:
            return value, 'the system keyring'
    path = secrets_path()
    value = str(
        _read_file(path).get(_FILE_SECTION, {}).get(_FILE_SETTING, '')).strip()
    if value:
        return value, str(path)
    dotenv = pathlib.Path.cwd() / '.env'
    value = _read_dotenv(dotenv).get(options._ENV_API_KEY, '').strip()
    if value:
        return value, str(dotenv)
    return None


def get_api_key(required: bool = True) -> str | None:
    """Returns the CourtListener API key.

    Args:
        required: whether to raise an error if there is no key. Defaults to
            `True`.

    Raises:
        MissingAPIKeyError: if there is no key and `required` is `True`.

    Returns:
        The key, or `None` if there is no key and `required` is `False`.

    """
    found = find_api_key()
    if found is None:
        if required:
            raise MissingAPIKeyError
        return None
    return found[0]


def mask(key: str) -> str:
    """Hides most of `key` so that it can be shown.

    Args:
        key: a secret.

    Returns:
        The first and last four characters of `key`, with dots between them,
            or only dots if `key` is short.

    """
    dot = chr(0x2022)
    if len(key) <= 12:
        return dot * len(key)
    return key[:4] + dot * 8 + key[-4:]


def secrets_path() -> pathlib.Path:
    """Returns the path of the secrets file.

    Returns:
        "secrets.toml" in the folder from `utilities.config_folder`.

    """
    return utilities.config_folder() / 'secrets.toml'


def set_api_key(key: str, *, store: str = 'auto') -> str:
    """Stores the CourtListener API key outside of any project.

    Args:
        key: the API key, from the "API" page of a CourtListener profile.
        store: "keyring" for the system keyring, "file" for the secrets file
            (see `secrets_path`), or "auto" (the default) for the keyring if
            one is available and the file otherwise.

    Raises:
        RuntimeError: if `store` is "keyring" and the keyring cannot be used.
        ValueError: if `key` is blank or `store` is not one of those.

    Returns:
        A description of where the key was stored.

    """
    key = key.strip()
    if not key:
        message = 'the API key is blank'
        raise ValueError(message)
    if key.lower().startswith('token '):
        # People often copy the whole header value.
        key = key[6:].strip()
    if store not in _STORES:
        message = f'store must be one of {list(_STORES)}, not {store!r}'
        raise ValueError(message)
    if store in ('auto', 'keyring'):
        backend = _keyring()
        try:
            if backend is None:
                message = 'no system keyring is available'
                raise RuntimeError(message)  # noqa: TRY301
            backend.set_password(*_keyring_names(), key)
        except Exception as error:
            if store == 'keyring':
                message = f'the key could not be stored in the keyring: {error}'
                raise RuntimeError(message) from error
            logger.info('the keyring is not available (%s), so a file is used', error)
        else:
            return 'the system keyring'
    path = secrets_path()
    contents = _read_file(path)
    contents.setdefault(_FILE_SECTION, {})[_FILE_SETTING] = key
    _write_file(path, contents)
    return str(path)


""" Private Functions """


def _keyring() -> Any:
    """Returns the `keyring` module, if it has a usable backend.

    Returns:
        The module, or `None` if it is not installed or has no backend that
            stores secrets (as on a Linux server without a Secret Service).

    """
    try:
        import keyring  # noqa: PLC0415
        import keyring.backends.fail  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return None
    try:
        backend = keyring.get_keyring()
    except Exception:  # noqa: BLE001
        return None
    if isinstance(backend, keyring.backends.fail.Keyring):
        return None
    if getattr(backend, 'priority', 1) <= 0:
        return None
    return keyring


def _keyring_names() -> tuple[str, str]:
    """Returns the service and user name of the key in the keyring."""
    return options._KEYRING_SERVICE, options._KEYRING_USERNAME


def _read_dotenv(path: pathlib.Path) -> dict[str, str]:
    """Returns the settings in a `.env` file.

    Args:
        path: path to the file.

    Returns:
        The settings, by name. Lines such as `NAME=value` and
            `export NAME="value"` are read, and comments are ignored. An empty
            `dict` is returned if there is no file.

    """
    if not path.is_file():
        return {}
    found = {}
    for line in path.read_text(encoding = 'utf-8-sig').splitlines():
        line = line.strip()  # noqa: PLW2901
        if not line or line.startswith('#') or '=' not in line:
            continue
        name, value = line.removeprefix('export ').split('=', 1)
        found[name.strip()] = value.strip().strip('"').strip("'")
    return found


def _read_file(path: pathlib.Path) -> dict[str, Any]:
    """Returns the contents of the secrets file.

    Args:
        path: path to the file.

    Returns:
        The contents, or an empty `dict` if there is no file or it cannot be
            read.

    """
    if not path.is_file():
        return {}
    try:
        with path.open('rb') as file:
            return tomllib.load(file)
    except (OSError, tomllib.TOMLDecodeError) as error:
        logger.warning('the secrets file %s could not be read: %s', path, error)
        return {}


def _write_file(path: pathlib.Path, contents: dict[str, Any]) -> None:
    """Writes the secrets file, readable only by its owner.

    Args:
        path: path to the file.
        contents: sections of settings to write. Values are written as
            strings.

    """
    path.parent.mkdir(parents = True, exist_ok = True)
    lines = [
        '# Secrets for courtpy. Keep this file private: do not share it or',
        '# copy it into a project.']
    for section, settings in contents.items():
        if not isinstance(settings, dict) or not settings:
            continue
        lines.extend(['', f'[{section}]'])
        for name, value in settings.items():
            # A JSON string is also a valid TOML string.
            lines.append(f'{name} = {json.dumps(str(value))}')
    path.write_text('\n'.join(lines) + '\n', encoding = 'utf-8')
    if sys.platform != 'win32':
        # Windows keeps each user's application data private already.
        with contextlib.suppress(OSError):
            path.chmod(0o600)
