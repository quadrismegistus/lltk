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


# Per-worker persistent ClickHouse client, lazily initialized on first call.
# ProcessPoolExecutor reuses worker processes across submissions, so we avoid
# reconnecting per batch.
_WORKER_CH_CLIENT = None


def _worker_get_client(ch_url):
    global _WORKER_CH_CLIENT
    if _WORKER_CH_CLIENT is None:
        from lltk.tools.db_adapter import get_adapter
        _WORKER_CH_CLIENT = get_adapter(ch_url).client
    return _WORKER_CH_CLIENT


def _worker_read_and_insert(args):
    """Worker: read N freqs JSONs, build an Arrow Table, and INSERT into
    ClickHouse directly. Running the insert in the worker parallelizes it
    across N workers' HTTP connections — the main thread is only a
    scheduler, not a bottleneck.

    Returns (batch_idx, n_inserted, error_or_none).
    """
    import pyarrow as pa
    batch_idx, ch_url, corpus_root, entries = args
    ids, corpora, freqs = [], [], []
    for _id, corpus, rel_path in entries:
        abs_path = os.path.join(corpus_root, rel_path)
        d = _read_freqs_json(abs_path)
        if d is None:
            continue
        ids.append(_id)
        corpora.append(corpus)
        freqs.append(list(d.items()))

    if not ids:
        return (batch_idx, 0, None)

    map_type = pa.map_(pa.string(), pa.uint32())
    table = pa.Table.from_arrays(
        [pa.array(ids), pa.array(corpora), pa.array(freqs, type=map_type)],
        names=['_id', 'corpus', 'freqs'],
    )

    try:
        client = _worker_get_client(ch_url)
        client.insert_arrow('text_freqs', table)
        return (batch_idx, len(ids), None)
    except Exception as e:
        return (batch_idx, 0, f'{type(e).__name__}: {str(e)[:150]}')


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

    # Build todo: texts with path_freqs that aren't yet in text_freqs.
    # With truncate_first=True we first TRUNCATE/DELETE and then re-ingest all.
    # Without it, we skip _ids already present (resumable default).
    where = ''
    if corpora:
        cl = ', '.join(f"'{c}'" for c in corpora)
        where = f"AND t.corpus IN ({cl})"

    if truncate_first:
        # Use SYNC mode so the delete completes before we query for the todo list.
        # Without SETTINGS mutations_sync=1, ALTER TABLE DELETE is async and the
        # subsequent LEFT ANTI JOIN could see rows still pending removal.
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            ch_adapter.execute(
                f"ALTER TABLE lltk.text_freqs DELETE WHERE corpus IN ({cl}) "
                f"SETTINGS mutations_sync=1"
            )
            print(f'Deleted existing rows for corpora: {sorted(corpora)}')
        else:
            ch_adapter.execute('TRUNCATE TABLE lltk.text_freqs')
            print('Truncated lltk.text_freqs')

    # Fetch todo list: skip rows already in text_freqs (resumable default).
    # After a truncate_first run the table is empty for the relevant corpora,
    # so the anti-join reduces to a plain scan.
    todo = ch_adapter.query(f"""
        SELECT t._id, t.corpus, t.path_freqs
        FROM lltk.texts t
        LEFT ANTI JOIN lltk.text_freqs f ON t._id = f._id
        WHERE t.path_freqs IS NOT NULL
          {where}
    """)

    if not todo:
        print('Nothing to do — all texts already ingested (or no path_freqs)')
        return 0

    n_already = ch_adapter.query(f"""
        SELECT COUNT(*) FROM lltk.text_freqs
        {"WHERE corpus IN (" + cl + ")" if corpora and not truncate_first else ""}
    """)[0][0] if not truncate_first else 0

    already_msg = f' (skipping {n_already:,} already ingested)' if n_already else ''
    print(f'Ingesting {len(todo):,} freqs JSONs into ClickHouse '
          f'({num_proc} workers, batch={batch_size}){already_msg}...')

    # Pass ClickHouse URL to workers so each can open its own connection.
    # Parallel worker inserts remove the main-thread serialization bottleneck.
    ch_url = os.environ.get(
        'LLTK_CLICKHOUSE_URL',
        f'clickhouse://{ch_adapter.username}:{ch_adapter._password}'
        f'@{ch_adapter.host}:{ch_adapter.port}/{ch_adapter.database}',
    )

    batches = [todo[i:i+batch_size] for i in range(0, len(todo), batch_size)]
    args_list = [(i, ch_url, corpus_root, b) for i, b in enumerate(batches)]

    from lltk.tools.tools import get_tqdm

    t0 = time.time()
    n_inserted = 0
    n_failed = 0

    pbar = get_tqdm(total=len(todo), desc='freqs → ClickHouse',
                    unit='text', unit_scale=True, smoothing=0.1)
    try:
        with ProcessPoolExecutor(max_workers=num_proc) as pool:
            futures = [pool.submit(_worker_read_and_insert, a) for a in args_list]
            for fut in as_completed(futures):
                batch_idx, n, err = fut.result()
                if err:
                    n_failed += batch_size
                    pbar.write(f'  batch {batch_idx}: insert failed: {err}')
                    continue
                n_inserted += n
                pbar.update(n)
    finally:
        pbar.close()

    elapsed = time.time() - t0
    print(f'Inserted {n_inserted:,} rows in {elapsed:.0f}s '
          f'({n_inserted/elapsed:.0f}/s); {n_failed} failed')

    # Final count
    n = ch_adapter.query("SELECT COUNT(*) FROM lltk.text_freqs")[0][0]
    print(f'lltk.text_freqs now has {n:,} rows total')
    return n_inserted
