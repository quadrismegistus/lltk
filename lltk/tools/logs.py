import contextlib
from logmap import logmap
from logmap.logmap import _nesting


class _LltkLog(logmap):
    """Thin subclass that adds the ``silent`` context-manager property
    expected by legacy call-sites (``with log.silent: ...``)."""

    @property
    def silent(self):
        return self._quiet_ctx()

    @contextlib.contextmanager
    def _quiet_ctx(self):
        was_quiet = _nesting.is_quiet
        _nesting.is_quiet = True
        try:
            yield self
        finally:
            _nesting.is_quiet = was_quiet


log = _LltkLog('lltk')

# Backward-compatible aliases
logger = log  # estc.py imports `logger` via star-import chain


def log_on():
    """Enable lltk log output."""
    _nesting.is_quiet = False


def log_off():
    """Silence lltk log output."""
    _nesting.is_quiet = True
