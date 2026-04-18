"""
Per-text language detection via stopword intersection, ClickHouse-native.

Rather than streaming every text's freqs and summing stopword hits in Python,
we push the entire per-lang hit computation to ClickHouse: for each row in
text_freqs, it evaluates `freqs[stopword]` for each stopword per language
and sums. The result is one row per text with (en_hits, fr_hits, ...,
total_tokens), all columnar/SIMD-accelerated. Python then just picks the
argmax and computes coverage/confidence.

Expected throughput: full 2 M texts in minutes (was hours with Python
per-token iteration).
"""

import time
import pyarrow as pa


def detect_langs_clickhouse(ch_adapter, min_tokens=50,
                             coverage_threshold=0.05,
                             confidence_threshold=2.0,
                             batch_size=50_000, progress=True,
                             skip_existing=True):
    """Server-side stopword-intersection language detection.

    With skip_existing=True (default), only processes _ids not already in
    text_langs. ReplacingMergeTree on _id (versioned by detected_at) means
    re-runs always overwrite safely.

    Returns lang → row count distribution dict.
    """
    from lltk.tools.lang_detect import function_words_table
    from lltk.tools.tools import get_tqdm

    # Ensure destination table exists
    ch_adapter.execute("""
        CREATE TABLE IF NOT EXISTS lltk.text_langs (
            _id              String,
            lang_detected    LowCardinality(String),
            lang_coverage    Float32,
            lang_confidence  Float32,
            detected_at      DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(detected_at)
        ORDER BY _id
    """)

    # Organize stopwords into per-lang lists
    lang_to_words = {}
    for w, lg in function_words_table():
        lang_to_words.setdefault(lg, []).append(w)
    langs = sorted(lang_to_words.keys())
    print(f'Detecting across {len(langs)} languages '
          f'({sum(len(ws) for ws in lang_to_words.values())} stopwords total)')

    # Skip logic — count how much work remains
    if skip_existing:
        already = ch_adapter.query("SELECT count(DISTINCT _id) FROM lltk.text_langs")[0][0]
        n_freqs = ch_adapter.query("""
            SELECT count() FROM lltk.text_freqs f
            WHERE f._id NOT IN (SELECT _id FROM lltk.text_langs)
        """)[0][0]
        if already:
            print(f'  skipping {already:,} already-processed texts')
    else:
        already = 0
        n_freqs = ch_adapter.query("SELECT count() FROM lltk.text_freqs")[0][0]
    if n_freqs == 0:
        print('Nothing to do')
        return {}
    print(f'  {n_freqs:,} texts remaining')

    # Build the big SELECT. One arraySum + arrayMap per language.
    def _sql_str_array(ws):
        # quote + escape single quotes in stopwords
        parts = ["'" + w.replace("'", "''") + "'" for w in ws]
        return '[' + ', '.join(parts) + ']'

    lang_exprs = ',\n           '.join(
        f"arraySum(arrayMap(w -> freqs[w], {_sql_str_array(lang_to_words[lg])})) "
        f"AS {lg}_hits"
        for lg in langs
    )

    where = ("WHERE _id NOT IN (SELECT _id FROM lltk.text_langs)"
             if skip_existing else "")

    sql = f"""
        SELECT _id,
               {lang_exprs},
               arraySum(mapValues(freqs)) AS total_tokens
        FROM lltk.text_freqs
        {where}
    """

    # Streaming read; no writes on this client (held-open session stays read-only).
    # Writes go through a dedicated client.
    from lltk.tools.db_adapter import get_adapter
    import os as _os
    ch_url = _os.environ.get(
        'LLTK_CLICKHOUSE_URL',
        f'clickhouse://{ch_adapter.username}:{ch_adapter._password}'
        f'@{ch_adapter.host}:{ch_adapter.port}/{ch_adapter.database}',
    )
    write_adapter = get_adapter(ch_url)

    results = []
    pbar = get_tqdm(total=n_freqs, desc='detect_langs',
                    unit='text', unit_scale=True) if progress else None

    t0 = time.time()
    with ch_adapter.client.query_arrow_stream(sql) as stream:
        for arrow_batch in stream:
            ids = arrow_batch.column('_id').to_pylist()
            lang_hits_arrays = {lg: arrow_batch.column(f'{lg}_hits').to_pylist()
                                for lg in langs}
            totals = arrow_batch.column('total_tokens').to_pylist()

            for i, (_id, total) in enumerate(zip(ids, totals)):
                if total < min_tokens:
                    results.append((_id, 'null', 0.0, 0.0))
                    continue
                hits = {lg: lang_hits_arrays[lg][i] for lg in langs}
                # argmax + runner-up
                top_lang, top_hits, second_hits = None, 0, 0
                for lg, h in hits.items():
                    if h > top_hits:
                        second_hits = top_hits
                        top_hits = h
                        top_lang = lg
                    elif h > second_hits:
                        second_hits = h
                if top_hits == 0:
                    results.append((_id, 'unknown', 0.0, 0.0))
                    continue
                coverage = top_hits / total
                confidence = top_hits / second_hits if second_hits > 0 else 999.0
                if coverage < coverage_threshold or confidence < confidence_threshold:
                    results.append((_id, 'unknown', coverage, confidence))
                else:
                    results.append((_id, top_lang, coverage, confidence))

            if pbar:
                pbar.update(len(ids))
            if len(results) >= batch_size:
                _flush_langs(write_adapter, results)
                results.clear()
    if pbar:
        pbar.close()
    if results:
        _flush_langs(write_adapter, results)

    elapsed = time.time() - t0
    print(f'Done in {elapsed:.0f}s ({n_freqs/max(elapsed, 1):.0f}/s)')

    dist = ch_adapter.query("""
        SELECT lang_detected, count()
        FROM lltk.text_langs FINAL
        GROUP BY lang_detected
        ORDER BY count() DESC
    """)
    stats = {r[0]: r[1] for r in dist}
    print('\nDistribution:')
    for lg, n in stats.items():
        print(f'  {lg}: {n:,}')
    return stats


def _flush_langs(ch_adapter, rows):
    if not rows:
        return
    ids   = [r[0] for r in rows]
    langs = [r[1] for r in rows]
    covs  = [float(r[2]) for r in rows]
    confs = [min(float(r[3]), 999.0) for r in rows]
    table = pa.Table.from_arrays(
        [pa.array(ids, type=pa.string()),
         pa.array(langs, type=pa.string()),
         pa.array(covs, type=pa.float32()),
         pa.array(confs, type=pa.float32())],
        names=['_id', 'lang_detected', 'lang_coverage', 'lang_confidence'],
    )
    ch_adapter.client.insert_arrow('text_langs', table)
