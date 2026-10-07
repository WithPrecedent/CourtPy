"""Tests the secrets module."""

from __future__ import annotations

import pathlib
import types

import pytest

import courtpy
from courtpy import secrets


class FakeKeyring:
    """A keyring that keeps passwords in a `dict`."""

    def __init__(self) -> None:
        self.passwords: dict[tuple[str, str], str] = {}

    def get_password(self, service: str, username: str) -> str | None:
        return self.passwords.get((service, username))

    def set_password(self, service: str, username: str, password: str) -> None:
        self.passwords[(service, username)] = password

    def delete_password(self, service: str, username: str) -> None:
        del self.passwords[(service, username)]


def test_missing_key() -> None:
    assert secrets.find_api_key() is None
    assert secrets.get_api_key(required = False) is None
    with pytest.raises(secrets.MissingAPIKeyError, match = 'courtpy key set'):
        secrets.get_api_key()


def test_file_store_outside_project(tmp_path: pathlib.Path) -> None:
    place = secrets.set_api_key('  Token abcdef0123456789  ')
    path = secrets.secrets_path()
    assert place == str(path)
    assert path.is_relative_to(tmp_path / 'config')
    assert secrets.get_api_key() == 'abcdef0123456789'
    assert 'abcdef0123456789' in path.read_text(encoding = 'utf-8')
    assert secrets.find_api_key() == ('abcdef0123456789', str(path))
    assert secrets.delete_api_key() == [str(path)]
    assert secrets.find_api_key() is None


def test_environment_variable_comes_first(monkeypatch: pytest.MonkeyPatch) -> None:
    secrets.set_api_key('from-the-file')
    monkeypatch.setenv(courtpy.options._ENV_API_KEY, 'from-the-environment')
    key, place = secrets.find_api_key()
    assert key == 'from-the-environment'
    assert courtpy.options._ENV_API_KEY in place


def test_dotenv_in_current_folder(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = tmp_path / 'project'
    project.mkdir()
    (project / '.env').write_text(
        '# my settings\nexport COURTLISTENER_API_KEY="from-dotenv"\nOTHER=1\n',
        encoding = 'utf-8')
    monkeypatch.chdir(project)
    assert secrets.find_api_key() == ('from-dotenv', str(project / '.env'))


def test_keyring_store(monkeypatch: pytest.MonkeyPatch) -> None:
    keyring = FakeKeyring()
    monkeypatch.setattr(secrets, '_keyring', lambda: keyring)
    assert secrets.set_api_key('in-the-keyring') == 'the system keyring'
    assert not secrets.secrets_path().exists()
    assert secrets.find_api_key() == ('in-the-keyring', 'the system keyring')
    assert secrets.delete_api_key() == ['the system keyring']
    assert secrets.find_api_key() is None


def test_keyring_failure_falls_back_to_file(monkeypatch: pytest.MonkeyPatch) -> None:
    broken = types.SimpleNamespace(
        set_password = lambda *args: (_ for _ in ()).throw(RuntimeError('locked')),
        get_password = lambda *args: None)
    monkeypatch.setattr(secrets, '_keyring', lambda: broken)
    assert secrets.set_api_key('fallback') == str(secrets.secrets_path())
    with pytest.raises(RuntimeError, match = 'could not be stored'):
        secrets.set_api_key('fallback', store = 'keyring')


def test_invalid_input() -> None:
    with pytest.raises(ValueError, match = 'blank'):
        secrets.set_api_key('   ')
    with pytest.raises(ValueError, match = 'store must be'):
        secrets.set_api_key('key', store = 'cloud')


def test_mask() -> None:
    assert secrets.mask('0123456789abcdef') == '0123••••••••cdef'
    assert secrets.mask('short') == '•' * 5
