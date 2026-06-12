from unittest.mock import patch
import pytest
from geoseeq.utils import resolve_secret


def test_passthrough_plain_string():
    assert resolve_secret("plain-token") == "plain-token"


def test_passthrough_non_string():
    assert resolve_secret(None) is None


def test_op_uri_invokes_op_read():
    with patch("geoseeq.utils.subprocess.check_output", return_value="secret-value\n") as m:
        assert resolve_secret("op://Vault/Item/field") == "secret-value"
        m.assert_called_once_with(["op", "read", "op://Vault/Item/field"], text=True)


def test_op_cli_missing_raises_runtime_error():
    with patch("geoseeq.utils.subprocess.check_output", side_effect=FileNotFoundError):
        with pytest.raises(RuntimeError, match="op` CLI is not installed"):
            resolve_secret("op://Vault/Item/field")
