import pytest

try:
    import chdb  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False


@pytest.fixture
def ch(tmp_path):
    """In-process ClickHouse (chdb) with full LLTK schema. No server needed."""
    if not _HAS_CHDB:
        pytest.skip('chdb not installed')

    from lltk.tools.db_adapter import ChDBAdapter
    from lltk.tools.clickhouse_schema import create_all_tables

    adapter = ChDBAdapter(database='lltk')
    create_all_tables(adapter, database='lltk')
    yield adapter
    adapter.close()
