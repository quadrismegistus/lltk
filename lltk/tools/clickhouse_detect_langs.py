"""
Per-text language detection, fully server-side.

Rather than stream rows back to Python for argmax, compute everything in
ClickHouse with one big INSERT INTO ... SELECT. Python just builds the
SQL, issues it, waits. CH handles the 2.2M-row scan, per-row stopword
sums, argmax, thresholding, and the write into text_langs in one pass.

Per-text compute: 1732 Map lookups + 9 flat scalar sums + argmax over
9 values. No intermediate arrays allocated per row (the old arrayMap
approach blew 55 GB).
"""

import time
import pyarrow as pa


def detect_langs_clickhouse(ch_adapter, min_tokens=50,
                             coverage_threshold=0.05,
                             confidence_threshold=2.0,
                             skip_existing=True, progress=True,
                             **unused):
    """Server-side stopword-intersection language detection.

    Writes to lltk.text_langs (ReplacingMergeTree, dedup on _id). With
    skip_existing=True (default), skips _ids already present.

    Returns lang → row count distribution dict.
    """
    from lltk.tools.lang_detect import function_words_table

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

    # Build per-language stopword lists
    lang_to_words = {}
    for w, lg in function_words_table():
        lang_to_words.setdefault(lg, []).append(w)
    langs = sorted(lang_to_words.keys())
    print(f'Detecting across {len(langs)} languages '
          f'({sum(len(ws) for ws in lang_to_words.values())} stopwords total)')

    # Flat sum of freqs[word] lookups, chunked to keep parser tree shallow
    def _sum_expr(words, chunk_size=20):
        terms = [
            f"toUInt64(freqs['{w.replace(chr(39), chr(39)*2)}'])"
            for w in words
        ]
        chunks = [
            '(' + ' + '.join(terms[i:i + chunk_size]) + ')'
            for i in range(0, len(terms), chunk_size)
        ]
        return '(' + ' + '.join(chunks) + ')'

    lang_hits_cols = ',\n           '.join(
        f"{_sum_expr(lang_to_words[lg])} AS {lg}_hits"
        for lg in langs
    )

    # hits_arr: [en_hits, fr_hits, ...]    langs_arr: ['en', 'fr', ...]
    hits_arr = '[' + ', '.join(f'{lg}_hits' for lg in langs) + ']'
    langs_arr = '[' + ', '.join(f"'{lg}'" for lg in langs) + ']'

    # Per-chunk filter for skip_existing — avoids re-hashing 500K rows per corpus
    if skip_existing:
        already = ch_adapter.query(
            "SELECT count(DISTINCT _id) FROM lltk.text_langs"
        )[0][0]
        if already:
            print(f'  skipping {already:,} already-processed texts')
        skip_sql = "AND _id NOT IN (SELECT _id FROM lltk.text_langs)"
    else:
        already = 0
        skip_sql = ''

    # Count how many rows remain
    total_remaining = ch_adapter.query(f"""
        SELECT count() FROM lltk.text_freqs WHERE 1 {skip_sql}
    """)[0][0]
    print(f'  {total_remaining:,} texts to process')
    if total_remaining == 0:
        return {}

    # One big INSERT. The NOT IN subquery is evaluated once as a hash set at
    # the start; per-corpus loops were re-evaluating it per query.
    t0 = time.time()
    sql = f"""
        INSERT INTO lltk.text_langs
            (_id, lang_detected, lang_coverage, lang_confidence, detected_at)
        WITH
          per_lang AS (
            SELECT _id,
                   arraySum(mapValues(freqs)) AS total_tokens,
                   {hits_arr} AS hits_arr
            FROM (
                SELECT _id, freqs, {lang_hits_cols}
                FROM lltk.text_freqs
                WHERE 1 {skip_sql}
            )
          ),
          ranked AS (
            SELECT _id, total_tokens, hits_arr,
                   arrayMax(hits_arr) AS top_hits,
                   {langs_arr}[indexOf(hits_arr, arrayMax(hits_arr))] AS top_lang,
                   arraySort(x -> -x, hits_arr)[2] AS second_hits
            FROM per_lang
          )
        SELECT
          _id,
          multiIf(
            total_tokens < {min_tokens},       'null',
            top_hits = 0,                      'unknown',
            toFloat64(top_hits) / toFloat64(total_tokens) < {coverage_threshold},
                                               'unknown',
            (top_hits * 1.0) / if(second_hits = 0, 1, second_hits) < {confidence_threshold}
              AND second_hits > 0,             'unknown',
                                                top_lang
          ) AS lang_detected,
          toFloat32(if(top_hits = 0, 0.0, toFloat64(top_hits) / toFloat64(total_tokens))) AS lang_coverage,
          toFloat32(if(second_hits = 0, 999.0, toFloat64(top_hits) / toFloat64(second_hits))) AS lang_confidence,
          now()
        FROM ranked
    """

    print('Running single server-side INSERT...')
    ch_adapter.execute(sql)
    elapsed = time.time() - t0
    print(f'  done in {elapsed:.1f}s ({total_remaining/max(elapsed, 1):.0f}/s)')

    # Report distribution
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
