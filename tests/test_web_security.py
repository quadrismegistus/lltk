"""Web-security regression tests: path-traversal guard + optional Basic auth."""
import os
import pytest

try:
    import fastapi  # noqa: F401
    HAS_FASTAPI = True
except Exception:
    HAS_FASTAPI = False

requires_fastapi = pytest.mark.skipif(not HAS_FASTAPI, reason='fastapi not installed')


# ── Path traversal (core; no web deps) ───────────────────────────────────────

def test_path_traversal_blocked():
    """A crafted _id with '../' must not escape the corpus directory."""
    import lltk
    C = lltk.load('test_fixture')
    evil = C.text('../../../../../../etc/passwd')
    assert evil.get_path('txt') == ''
    assert evil.get_path('xml') == ''
    assert evil.path == ''


def test_legit_id_resolves_within_corpus():
    """A normal id still resolves to a path inside the corpus dir."""
    import lltk
    C = lltk.load('test_fixture')
    p = C.text('blake').get_path('txt')
    assert p
    assert os.path.realpath(p).startswith(os.path.realpath(C.path_txt) + os.sep)


# ── Optional HTTP Basic auth ─────────────────────────────────────────────────

@requires_fastapi
def test_auth_disabled_by_default(monkeypatch):
    monkeypatch.delenv('LLTK_WEB_USER', raising=False)
    monkeypatch.delenv('LLTK_WEB_PASSWORD', raising=False)
    from lltk.web.auth import require_auth, auth_enabled
    assert auth_enabled() is False
    assert require_auth(None) is None  # no-op passthrough when unconfigured


@requires_fastapi
def test_auth_enabled_rejects_missing_and_wrong(monkeypatch):
    from fastapi import HTTPException
    from fastapi.security import HTTPBasicCredentials
    from lltk.web.auth import require_auth, auth_enabled
    monkeypatch.setenv('LLTK_WEB_USER', 'u')
    monkeypatch.setenv('LLTK_WEB_PASSWORD', 'p')
    assert auth_enabled() is True
    with pytest.raises(HTTPException) as ei:
        require_auth(None)
    assert ei.value.status_code == 401
    with pytest.raises(HTTPException):
        require_auth(HTTPBasicCredentials(username='u', password='wrong'))
    with pytest.raises(HTTPException):
        require_auth(HTTPBasicCredentials(username='wrong', password='p'))


@requires_fastapi
def test_auth_accepts_correct_creds(monkeypatch):
    from fastapi.security import HTTPBasicCredentials
    from lltk.web.auth import require_auth
    monkeypatch.setenv('LLTK_WEB_USER', 'u')
    monkeypatch.setenv('LLTK_WEB_PASSWORD', 'p')
    assert require_auth(HTTPBasicCredentials(username='u', password='p')) is None


# ── ClickHouse URL resolution (credentials / read-only) ──────────────────────

def test_shim_dict_params_bind_not_inline():
    """A dict of params -> clickhouse-connect bound parameters (never inlined)."""
    from unittest.mock import MagicMock
    from lltk.db.metadb_ch import _LegacyResult
    adapter = MagicMock()
    adapter.client.query.return_value.result_rows = [(0,)]
    _LegacyResult(adapter, "SELECT count() WHERE c = {c:String}",
                  {'c': "x' OR 1=1--"}).fetchone()
    adapter.client.query.assert_called_once_with(
        "SELECT count() WHERE c = {c:String}", parameters={'c': "x' OR 1=1--"})


def test_shim_positional_params_still_inline_escaped():
    """Positional list params keep the (backslash-safe) inline path for back-compat."""
    from unittest.mock import MagicMock
    from lltk.db.metadb_ch import _LegacyResult
    adapter = MagicMock()
    adapter.client.query.return_value.result_rows = []
    _LegacyResult(adapter, "SELECT * WHERE a = ?", ["o'reilly"]).fetchall()
    sql = adapter.client.query.call_args[0][0]
    assert "'o''reilly'" in sql  # inlined + quote-escaped, not a bound param


def test_resolve_ch_url_precedence(monkeypatch):
    from lltk.db.metadb_ch import resolve_ch_url, _DEV_FALLBACK_CH_URL
    monkeypatch.delenv('LLTK_CLICKHOUSE_URL', raising=False)
    monkeypatch.delenv('LLTK_CLICKHOUSE_URL_READONLY', raising=False)
    # nothing set -> dev fallback
    assert resolve_ch_url() == _DEV_FALLBACK_CH_URL
    # explicit arg beats everything
    assert resolve_ch_url('clickhouse://explicit') == 'clickhouse://explicit'
    # env var
    monkeypatch.setenv('LLTK_CLICKHOUSE_URL', 'clickhouse://rw')
    assert resolve_ch_url() == 'clickhouse://rw'
    # readonly falls back to RW when the RO var is unset
    assert resolve_ch_url(readonly=True) == 'clickhouse://rw'
    # readonly prefers the RO var when set; non-readonly still uses RW
    monkeypatch.setenv('LLTK_CLICKHOUSE_URL_READONLY', 'clickhouse://ro')
    assert resolve_ch_url(readonly=True) == 'clickhouse://ro'
    assert resolve_ch_url() == 'clickhouse://rw'
