import pytest
import os

try:
    import chdb  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False

_HAS_CH_SERVER = os.environ.get('LLTK_TEST_CH_HOST') is not None


@pytest.fixture
def ch(tmp_path):
    """In-process ClickHouse (chdb) with full LLTK schema. No server needed."""
    if not _HAS_CHDB:
        pytest.skip('chdb not installed')

    from lltk.db.adapter import ChDBAdapter
    from lltk.db.schema import create_all_tables

    adapter = ChDBAdapter(database='lltk')
    create_all_tables(adapter, database='lltk')
    yield adapter
    adapter.close()
