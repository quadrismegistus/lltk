"""
Thin DB abstraction for LLTK — targets DuckDB (embedded) or ClickHouse (server).

The goal is NOT a full ORM; it's a consistent interface for the common
operations LLTK actually does: schema setup, bulk parquet ingest, SELECT
queries. Engine-specific SQL can bypass the adapter when needed.

Usage:

    from lltk.tools.db_adapter import get_adapter

    # DuckDB (default)
    db = get_adapter('duckdb:///~/lltk_data/data/metadb.duckdb')

    # ClickHouse
    db = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')

    # Or via env var
    os.environ['LLTK_DB_URL'] = 'clickhouse://lltk:lltk@localhost:8123/lltk'
    db = get_adapter()

    # Common interface
    df = db.query_df("SELECT corpus, COUNT(*) FROM texts GROUP BY corpus")
    db.insert_parquet('text_freqs', '~/lltk_data/corpora/ecco/data/freqs.parquet')
"""

from abc import ABC, abstractmethod
import os
from urllib.parse import urlparse, parse_qs


class DBAdapter(ABC):
    """Common interface implemented by DuckDB and ClickHouse backends."""

    @property
    @abstractmethod
    def engine(self) -> str:
        """'duckdb' or 'clickhouse'."""

    @abstractmethod
    def execute(self, sql: str, params=None):
        """Run a DDL/DML statement, no return."""

    @abstractmethod
    def query(self, sql: str, params=None) -> list:
        """Run SELECT, return list of tuples."""

    @abstractmethod
    def query_df(self, sql: str, params=None):
        """Run SELECT, return pandas DataFrame."""

    @abstractmethod
    def insert_parquet(self, table: str, path_or_glob: str):
        """Bulk-load rows from parquet file(s) into `table`. Schema must match."""

    @abstractmethod
    def close(self):
        """Release the underlying connection."""

    # Convenience: used by migration code
    def table_exists(self, name: str) -> bool:
        raise NotImplementedError


class DuckDBAdapter(DBAdapter):
    """DuckDB-backed adapter — embedded, single-process."""

    def __init__(self, path: str, read_only: bool = False):
        import duckdb
        self.path = os.path.expanduser(path)
        os.makedirs(os.path.dirname(self.path), exist_ok=True) if os.path.dirname(self.path) else None
        self.conn = duckdb.connect(self.path, read_only=read_only)

    @property
    def engine(self):
        return 'duckdb'

    def execute(self, sql, params=None):
        return self.conn.execute(sql, params or [])

    def query(self, sql, params=None):
        return self.conn.execute(sql, params or []).fetchall()

    def query_df(self, sql, params=None):
        return self.conn.execute(sql, params or []).fetchdf()

    def insert_parquet(self, table, path_or_glob):
        # DuckDB supports globs natively in read_parquet.
        path_esc = os.path.expanduser(path_or_glob).replace("'", "''")
        self.conn.execute(f"""
            INSERT INTO {table}
            SELECT * FROM read_parquet('{path_esc}')
        """)

    def table_exists(self, name):
        # Split schema.table if present
        if '.' in name:
            schema, tbl = name.split('.', 1)
            row = self.conn.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, tbl],
            ).fetchone()
        else:
            row = self.conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [name],
            ).fetchone()
        return row is not None

    def close(self):
        self.conn.close()


class ClickHouseAdapter(DBAdapter):
    """ClickHouse-backed adapter — server-based, multi-user.

    Uses the HTTP interface (port 8123) via clickhouse-connect. For bulk
    ingest the native TCP protocol (9000) is faster, but HTTP keeps setup
    simple and handles Parquet ingestion natively.
    """

    def __init__(self, host='localhost', port=8123, database='lltk',
                 username='default', password=''):
        from clickhouse_connect import get_client
        self.client = get_client(
            host=host, port=port, database=database,
            username=username, password=password,
        )
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self._password = password

    @property
    def engine(self):
        return 'clickhouse'

    def execute(self, sql, params=None):
        if params:
            return self.client.command(sql, parameters=params)
        return self.client.command(sql)

    def query(self, sql, params=None):
        result = self.client.query(sql, parameters=params or {})
        return result.result_rows

    def query_df(self, sql, params=None):
        return self.client.query_df(sql, parameters=params or {})

    def insert_parquet(self, table, path_or_glob):
        """Bulk-load parquet into ClickHouse, streaming one row group at a time.

        pyarrow.iter_batches doesn't support chunked output for nested types
        like Map, so we iterate by row group instead — parquet files have
        natural chunking via their row groups (our writers use
        ROW_GROUP_SIZE=50_000). This keeps memory bounded regardless of
        total file size.
        """
        import glob as _glob
        import pyarrow.parquet as _pq

        paths = sorted(_glob.glob(os.path.expanduser(path_or_glob))) \
            if '*' in path_or_glob else [os.path.expanduser(path_or_glob)]
        tbl = table.split('.', 1)[1] if '.' in table else table
        for p in paths:
            pf = _pq.ParquetFile(p)
            for i in range(pf.num_row_groups):
                rg = pf.read_row_group(i)
                self.client.insert_arrow(tbl, rg)

    def table_exists(self, name):
        if '.' in name:
            schema, tbl = name.split('.', 1)
            row = self.client.query(
                "SELECT 1 FROM system.tables WHERE database = {s:String} "
                "AND name = {t:String}",
                parameters={'s': schema, 't': tbl},
            ).result_rows
        else:
            row = self.client.query(
                "SELECT 1 FROM system.tables WHERE database = {s:String} "
                "AND name = {t:String}",
                parameters={'s': self.database, 't': name},
            ).result_rows
        return bool(row)

    def close(self):
        self.client.close()


class ChDBAdapter(DBAdapter):
    """In-process ClickHouse via chdb — same SQL dialect, no server needed.

    Intended for tests: full MergeTree/RMT/FINAL semantics without a running
    ClickHouse instance. Exposes a `.client` shim so code that calls
    `adapter.client.insert()` / `insert_arrow()` / `command()` works unchanged.
    """

    def __init__(self, database='lltk'):
        from chdb import session as chdb_session
        self._session = chdb_session.Session()
        self.database = database
        self.host = 'chdb'
        self.port = 0
        self.username = 'default'
        self._password = ''
        self._session.query(f'CREATE DATABASE IF NOT EXISTS {database}')
        self._session.query("SET enable_full_text_index = 1")
        self.client = self._ChDBClient(self._session, database)

    class _ChDBClient:
        """Minimal shim matching the clickhouse-connect client methods used by LLTK."""

        def __init__(self, session, database):
            self._session = session
            self._database = database

        def command(self, sql, parameters=None, settings=None):
            self._session.query(sql)

        def insert(self, table, data, column_names=None, settings=None):
            if not data:
                return
            tbl = table if '.' in table else f'{self._database}.{table}'
            cols = f"({', '.join(column_names)})" if column_names else ''
            for row in data:
                vals = ', '.join(self._format_value(v) for v in row)
                self._session.query(f'INSERT INTO {tbl} {cols} VALUES ({vals})')

        def insert_arrow(self, table, arrow_table):
            import pyarrow as pa
            tbl = table if '.' in table else f'{self._database}.{table}'
            cols = arrow_table.column_names
            col_clause = f"({', '.join(cols)})"
            for batch in arrow_table.to_batches():
                for row_idx in range(batch.num_rows):
                    vals = []
                    for col_name in cols:
                        v = batch.column(col_name)[row_idx].as_py()
                        vals.append(self._format_value(v))
                    self._session.query(
                        f"INSERT INTO {tbl} {col_clause} VALUES ({', '.join(vals)})"
                    )

        def query(self, sql, parameters=None):
            import pandas as pd
            r = self._session.query(sql, 'ArrowStream')
            if r is None or r.bytes() == b'':
                return type('R', (), {'result_rows': []})()
            tbl = pa.ipc.open_stream(r.bytes()).read_all()
            rows = [tuple(r) for r in tbl.to_pydict().values()]
            if rows:
                rows = list(zip(*rows))
            return type('R', (), {'result_rows': rows})()

        def query_df(self, sql, parameters=None):
            import pandas as pd
            r = self._session.query(sql, 'ArrowStream')
            if r is None or r.bytes() == b'':
                return pd.DataFrame()
            import pyarrow as pa
            tbl = pa.ipc.open_stream(r.bytes()).read_all()
            return tbl.to_pandas()

        def close(self):
            pass

        @staticmethod
        def _format_value(v):
            if v is None:
                return 'NULL'
            if isinstance(v, str):
                return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
            if isinstance(v, bool):
                return '1' if v else '0'
            if isinstance(v, (int, float)):
                return str(v)
            if isinstance(v, list):
                inner = ', '.join(ChDBAdapter._ChDBClient._format_value(x) for x in v)
                return f'[{inner}]'
            return str(v)

    @property
    def engine(self):
        return 'clickhouse'

    def execute(self, sql, params=None):
        self._session.query(sql)

    def query(self, sql, params=None):
        r = self._session.query(sql, 'CSV')
        if r is None or r.bytes() == b'':
            return []
        import csv, io
        reader = csv.reader(io.StringIO(r.bytes().decode()))
        return [tuple(row) for row in reader]

    def query_df(self, sql, params=None):
        import pandas as pd
        r = self._session.query(sql, 'ArrowStream')
        if r is None or r.bytes() == b'':
            return pd.DataFrame()
        import pyarrow as pa
        tbl = pa.ipc.open_stream(r.bytes()).read_all()
        return tbl.to_pandas()

    def insert_parquet(self, table, path_or_glob):
        raise NotImplementedError('ChDBAdapter does not support insert_parquet')

    def table_exists(self, name):
        if '.' in name:
            schema, tbl = name.split('.', 1)
        else:
            schema, tbl = self.database, name
        r = self._session.query(
            f"SELECT 1 FROM system.tables WHERE database = '{schema}' AND name = '{tbl}'",
            'CSV',
        )
        return r is not None and r.bytes() != b''

    def close(self):
        pass


def get_adapter(url: str = None) -> DBAdapter:
    """Return a DBAdapter from a URL or the LLTK_DB_URL env var.

    URL formats:
        duckdb:///absolute/path/to/file.duckdb
        duckdb:///absolute/path?read_only=true
        clickhouse://host:port/database
        clickhouse://user:pass@host:port/database

    Defaults to DuckDB at the legacy LLTK path when nothing is specified.
    """
    if url is None:
        url = os.environ.get('LLTK_DB_URL')
    if url is None:
        # Default to the existing LLTK DuckDB path
        default_path = os.path.expanduser('~/lltk_data/data/metadb.duckdb')
        return DuckDBAdapter(path=default_path)

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme == 'duckdb':
        # urlparse(duckdb:///path) → path='/path'; keep the leading slash.
        path = parsed.path
        qs = parse_qs(parsed.query)
        read_only = qs.get('read_only', ['false'])[0].lower() in ('true', '1')
        return DuckDBAdapter(path=path, read_only=read_only)

    if scheme == 'clickhouse':
        host = parsed.hostname or 'localhost'
        port = parsed.port or 8123
        username = parsed.username or 'default'
        password = parsed.password or ''
        database = parsed.path.lstrip('/') or 'lltk'
        return ClickHouseAdapter(
            host=host, port=port, database=database,
            username=username, password=password,
        )

    if scheme == 'chdb':
        database = parsed.path.lstrip('/') or 'lltk'
        return ChDBAdapter(database=database)

    raise ValueError(f'Unknown DB URL scheme: {scheme!r}. Use duckdb://, clickhouse://, or chdb://')
