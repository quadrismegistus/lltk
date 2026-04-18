"""
Fresh ingest of freqs from JSON files → ClickHouse text_freqs table.

No intermediate parquet. No central DuckDB. Reads freqs JSONs directly in
parallel workers, batches into pyarrow tables, streams via insert_arrow
in small chunks that stay under HTTP/memory limits.

The only source of truth is the freqs/ JSONs on disk. Per-corpus parquets
exist only when explicitly exported (e.g. for `corpus.publish()`).

Usage:

    from lltk.tools.db_adapter import get_adapter
    from lltk.tools.clickhouse_ingest import ingest_freqs_from_jsons

    ch = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')
    ingest_freqs_from_jsons(ch, corpora=['ecco'])   # one corpus
    ingest_freqs_from_jsons(ch)                      # all corpora
"""

import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed


def _read_freqs_json(path):
    """Read a single freqs JSON file into a dict[str → int]."""
    try:
        import orjson as _json
    except ImportError:
        import json as _json
    try:
        with open(path, 'rb') as f:
            data = f.read()
        d = _json.loads(data)
        # Values may be floats in some legacy files; coerce to int
        return {str(k): int(v) for k, v in d.items() if v}
    except Exception:
        return None


def _worker_read_batch(args):
    """Worker: read N freqs JSONs, return an arrow Table of (_id, corpus, freqs).

    Returns (batch_idx, arrow_table_or_none, n_read).
    """
    import pyarrow as pa
    batch_idx, corpus_root, entries = args
    ids, corpora, freqs = [], [], []
    for _id, corpus, rel_path in entries:
        abs_path = os.path.join(corpus_root, rel_path)
        d = _read_freqs_json(abs_path)
        if d is None:
            continue
        ids.append(_id)
        corpora.append(corpus)
        # Arrow map type: list of (key, value) tuples per row
        freqs.append(list(d.items()))

    if not ids:
        return (batch_idx, None, 0)

    map_type = pa.map_(pa.string(), pa.uint32())
    table = pa.Table.from_arrays(
        [pa.array(ids), pa.array(corpora), pa.array(freqs, type=map_type)],
        names=['_id', 'corpus', 'freqs'],
    )
    return (batch_idx, table, len(ids))


def ingest_freqs_from_jsons(ch_adapter, corpora=None, batch_size=500,
                             num_proc=None, truncate_first=False):
    """Stream freqs JSONs into ClickHouse text_freqs.

    Uses the texts table in ClickHouse as the source-of-truth for which
    (_id, corpus, path_freqs) tuples to ingest. Texts without path_freqs
    are skipped.

    Args:
        ch_adapter: ClickHouseAdapter instance
        corpora: list of corpus ids to ingest (None = all)
        batch_size: texts per worker batch (→ per insert_arrow call)
        num_proc: parallel workers for JSON reading (default cpu_count - 2)
        truncate_first: TRUNCATE lltk.text_freqs before ingesting
    """
    if ch_adapter.engine != 'clickhouse':
        raise ValueError(f'Expected ClickHouseAdapter, got {ch_adapter.engine}')

    from lltk.imports import PATH_CORPUS
    corpus_root = os.path.expanduser(PATH_CORPUS)

    if num_proc is None:
        num_proc = max(1, os.cpu_count() - 2)

    # Pull todo list from ClickHouse's texts table
    where = ''
    if corpora:
        cl = ', '.join(f"'{c}'" for c in corpora)
        where = f"AND corpus IN ({cl})"
    todo = ch_adapter.query(f"""
        SELECT _id, corpus, path_freqs
        FROM lltk.texts
        WHERE path_freqs IS NOT NULL {where}
    """)

    if not todo:
        print('No texts with path_freqs in texts table. Ingest that first.')
        return 0

    if truncate_first:
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            ch_adapter.execute(f"ALTER TABLE lltk.text_freqs DELETE WHERE corpus IN ({cl})")
            print(f'Deleted existing rows for corpora: {sorted(corpora)}')
        else:
            ch_adapter.execute('TRUNCATE TABLE lltk.text_freqs')
            print('Truncated lltk.text_freqs')

    print(f'Ingesting {len(todo):,} freqs JSONs into ClickHouse '
          f'({num_proc} workers, batch={batch_size})...')

    batches = [todo[i:i+batch_size] for i in range(0, len(todo), batch_size)]
    args_list = [(i, corpus_root, b) for i, b in enumerate(batches)]

    t0 = time.time()
    n_inserted = 0
    n_failed = 0

    # Use a single ClickHouse connection (HTTP client) in the main process;
    # workers just read JSONs (I/O bound) and return arrow tables.
    with ProcessPoolExecutor(max_workers=num_proc) as pool:
        futures = {pool.submit(_worker_read_batch, a): a[0] for a in args_list}
        for fut in as_completed(futures):
            batch_idx, table, n_read = fut.result()
            if table is None:
                continue
            try:
                ch_adapter.client.insert_arrow('text_freqs', table)
                n_inserted += n_read
            except Exception as e:
                n_failed += n_read
                print(f'  batch {batch_idx}: insert failed: {type(e).__name__}: {str(e)[:120]}', flush=True)
                continue

            if n_inserted % (batch_size * 20) == 0 or n_inserted == len(todo):
                rate = n_inserted / (time.time() - t0)
                eta = (len(todo) - n_inserted) / rate if rate > 0 else 0
                print(f'  {n_inserted:,}/{len(todo):,} ({rate:.0f}/s, '
                      f'ETA {eta:.0f}s)', flush=True)

    elapsed = time.time() - t0
    print(f'\nInserted {n_inserted:,} rows in {elapsed:.0f}s '
          f'({n_inserted/elapsed:.0f}/s); {n_failed} failed')

    # Final count
    n = ch_adapter.query("SELECT COUNT(*) FROM lltk.text_freqs")[0][0]
    print(f'lltk.text_freqs now has {n:,} rows total')
    return n_inserted
