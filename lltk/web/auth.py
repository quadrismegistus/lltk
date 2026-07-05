"""Optional HTTP Basic auth for the LLTK web apps.

Auth is OFF by default so localhost development is frictionless. Set both
``LLTK_WEB_USER`` and ``LLTK_WEB_PASSWORD`` to require Basic auth on every
endpoint — do this before exposing an app on a non-loopback interface.

Wire into a FastAPI app with::

    app = FastAPI(dependencies=[Depends(require_auth)])
"""
import os
import sys
import secrets
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

_security = HTTPBasic(auto_error=False)


def auth_enabled() -> bool:
    """True iff both LLTK_WEB_USER and LLTK_WEB_PASSWORD are set."""
    return bool(os.environ.get('LLTK_WEB_USER') and os.environ.get('LLTK_WEB_PASSWORD'))


def require_auth(credentials: Optional[HTTPBasicCredentials] = Depends(_security)):
    """FastAPI dependency: enforce Basic auth when configured, else no-op.

    Uses secrets.compare_digest for constant-time comparison.
    """
    user = os.environ.get('LLTK_WEB_USER')
    pw = os.environ.get('LLTK_WEB_PASSWORD')
    if not user or not pw:
        return  # auth disabled (local dev default)
    ok = credentials is not None and (
        secrets.compare_digest(credentials.username, user)
        and secrets.compare_digest(credentials.password, pw)
    )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Unauthorized',
            headers={'WWW-Authenticate': 'Basic'},
        )


def warn_if_exposed(host) -> None:
    """Print a loud warning if binding to a non-loopback host without auth."""
    loopback = str(host) in ('127.0.0.1', 'localhost', '::1')
    if not loopback and not auth_enabled():
        print(
            f'\n  WARNING: binding to {host} with NO authentication.\n'
            f'  Set LLTK_WEB_USER and LLTK_WEB_PASSWORD to require login,\n'
            f'  or bind to 127.0.0.1 (the default) for local-only access.\n',
            file=sys.stderr,
        )
