import subprocess
import pytest
from geoseeq.utils import resolve_secret


def test_passthrough_plain_string():
    assert resolve_secret("plain-token") == "plain-token"


def test_passthrough_non_string():
    assert resolve_secret(None) is None


def test_op_uri_invokes_op_read(monkeypatch):
    calls = []
    def fake_check_output(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return "secret-value\n"
    monkeypatch.setattr(subprocess, "check_output", fake_check_output)
    assert resolve_secret("op://Vault/Item/field") == "secret-value"
    assert calls == [(["op", "read", "op://Vault/Item/field"], {"text": True})]


def test_op_cli_missing_raises_runtime_error(monkeypatch):
    def boom(*a, **kw):
        raise FileNotFoundError
    monkeypatch.setattr(subprocess, "check_output", boom)
    with pytest.raises(RuntimeError, match="op` CLI is not installed"):
        resolve_secret("op://Vault/Item/field")


def test_op_read_failure_raises_runtime_error(monkeypatch):
    def boom(*a, **kw):
        raise subprocess.CalledProcessError(1, ["op", "read", "op://x/y/z"])
    monkeypatch.setattr(subprocess, "check_output", boom)
    with pytest.raises(RuntimeError, match="`op read` failed"):
        resolve_secret("op://x/y/z")
