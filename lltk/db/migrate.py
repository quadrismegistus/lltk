"""
Migrate LLTK tables from DuckDB to ClickHouse.

Streams each table through parquet as the intermediate format — lets
DuckDB export efficiently and ClickHouse ingest efficiently without
either side paying the complex-type serialization cost in Python.

Usage (module):

    from lltk.db.adapter import get_adapter
    from lltk.db.migrate import migrate_tables

    src = get_adapter('duckdb:///~/lltk_data/data/metadb.duckdb')
    dst = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')
    migrate_tables(src, dst, tables=['texts', 'matches', 'match_groups'])

Usage (CLI):

    python -m lltk.db.migrate --tables texts matches match_groups
"""

import argparse
import os
import tempfile
import time

from logmap import logmap
from lltk.db.adapter import get_adapter, DuckDBAdapter, ClickHouseAdapter
from lltk.db.schema import create_all_tables


# Map: ClickHouse table name → (DuckDB source query, optional column projection)
# Kept explicit so we can handle schema differences (JSON `meta` column,
# table lives in an ATTACHed db, etc.).
TABLE_SOURCES = {
    'texts':              "SELECT * FROM texts",
    'corpus_info':        "SELECT * FROM corpus_info",
    'matches':            "SELECT * FROM match_db.matches",
    'match_groups':       "SELECT * FROM match_db.match_groups",
    'wordcounts':         "SELECT * FROM wc_db.wordcounts",
    'word_year_corpus':   "SELECT * FROM wi_db.word_year_corpus",
    'year_corpus_totals': "SELECT * FROM wi_db.year_corpus_totals",
}


def migrate_tables(src: DuckDBAdapter, dst: ClickHouseAdapter,
                   tables=None, tmp_dir=None, keep_parquet=False):
    """Copy specified tables from DuckDB to ClickHouse via parquet staging.

    Args:
        src: DuckDB adapter (read side)
        dst: ClickHouse adapter (write side)
        tables: list of table names (keys of TABLE_SOURCES). Default: all.
        tmp_dir: where to stage parquet files. Default: system temp.
        keep_parquet: leave the staged parquets on disk after import.

    Returns dict[table] → (row_count, seconds).
    """
    if src.engine != 'duckdb':
        raise ValueError(f'src must be duckdb, got {src.engine}')
    if dst.engine != 'clickhouse':
        raise ValueError(f'dst must be clickhouse, got {dst.engine}')

    tables = tables or list(TABLE_SOURCES.keys())
    unknown = [t for t in tables if t not in TABLE_SOURCES]
    if unknown:
        raise ValueError(f'Unknown tables: {unknown}. Known: {list(TABLE_SOURCES)}')

    # Ensure destination schema exists
    create_all_tables(dst, database=dst.database)

    staging = tmp_dir or tempfile.mkdtemp(prefix='lltk_migrate_')
    os.makedirs(staging, exist_ok=True)

    with logmap(f'Migrating {len(tables)} tables (staging in {staging})...') as log:
        results = {}
        for tname in tables:
            t0 = time.time()
            src_query = TABLE_SOURCES[tname]

            # Match columns by name between DuckDB -> ClickHouse.
            # Use ClickHouse's column order as canonical; project DuckDB's
            # SELECT accordingly so the INSERT positions match.
            dst_cols = [r[0] for r in dst.query(
                f"SELECT name FROM system.columns "
                f"WHERE database = '{dst.database}' AND table = '{tname}' "
                f"ORDER BY position"
            )]
            src_cols = {r[0] for r in src.query(f"DESCRIBE ({src_query})")}
            col_projs = [c if c in src_cols else f'NULL AS {c}' for c in dst_cols]
            projected_sql = f"SELECT {', '.join(col_projs)} FROM ({src_query})"

            pq_path = os.path.join(staging, f'{tname}.parquet')
            pq_esc = pq_path.replace("'", "''")

            # Export from DuckDB with columns in ClickHouse order
            src.execute(f"""
                COPY ({projected_sql}) TO '{pq_esc}'
                (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            size_mb = os.path.getsize(pq_path) / 1e6

            # Truncate destination first (idempotent re-runs)
            dst.execute(f"TRUNCATE TABLE IF EXISTS {dst.database}.{tname}")

            # Ingest into ClickHouse
            dst.insert_parquet(f'{dst.database}.{tname}', pq_path)

            n = dst.query(f"SELECT COUNT(*) FROM {dst.database}.{tname}")[0][0]
            dt = time.time() - t0
            log.debug(f'{tname}: {n:,} rows, {size_mb:.1f} MB staged, {dt:.1f}s')
            results[tname] = (n, dt)

            if not keep_parquet:
                os.remove(pq_path)

        if not keep_parquet:
            try:
                os.rmdir(staging)
            except OSError:
                pass

        return results


def main():
    ap = argparse.ArgumentParser(description='Migrate LLTK tables DuckDB → ClickHouse')
    ap.add_argument('--src', default=None,
                    help='DuckDB URL (default: ~/lltk_data/data/metadb.duckdb)')
    ap.add_argument('--dst', default='clickhouse://lltk:lltk@localhost:8123/lltk',
                    help='ClickHouse URL')
    ap.add_argument('--tables', nargs='*', default=None,
                    help='Tables to migrate (default: all)')
    ap.add_argument('--keep-parquet', action='store_true',
                    help='Leave staged parquets on disk')
    args = ap.parse_args()

    src = get_adapter(args.src)
    dst = get_adapter(args.dst)
    results = migrate_tables(
        src, dst, tables=args.tables, keep_parquet=args.keep_parquet,
    )
    total_rows = sum(n for n, _ in results.values())
    total_time = sum(t for _, t in results.values())
    logmap.debug(f'Done. {len(results)} tables, {total_rows:,} rows, {total_time:.1f}s total.')


if __name__ == '__main__':
    main()
