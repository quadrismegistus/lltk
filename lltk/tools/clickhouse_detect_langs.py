"""
Per-text language detection — exclusivity-weighted via text_words + freq_words JOIN.

Uses frequency-ranked word lists (top 500 per language, from OpenSubtitles)
with exclusivity weighting: each word's contribution is scaled by 1/N where
N is the number of languages sharing that word. This strongly discriminates
between related languages (e.g. Spanish vs French vs Portuguese) that share
many common function words.

The flat lltk.text_words table (ORDER BY (word, _id)) makes the JOIN
sub-second via index range scans even across billions of word-count rows.
"""

import time
import pyarrow as pa


def detect_langs_clickhouse(ch_adapter, min_tokens=50,
                             coverage_threshold=0.05,
                             confidence_threshold=2.0,
                             skip_existing=True, progress=True):
    """Server-side weighted language detection via text_words.

    Prerequisites: lltk.text_words + lltk.text_stats populated (via
    `lltk db-text-words`). Writes to lltk.text_langs (ReplacingMergeTree).
    """
    from lltk.tools.lang_detect import function_words_table

    # Ensure destination + stopwords tables exist
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
    ch_adapter.execute("""
        CREATE TABLE IF NOT EXISTS lltk.stopwords (
            word   LowCardinality(String),
            lang   LowCardinality(String),
            weight Float32 DEFAULT 1.0
        ) ENGINE = MergeTree() ORDER BY word
    """)

    # (Re)populate stopwords with weighted frequency words
    lang_to_words = {}
    word_weights = {}
    for w, lg, wt in function_words_table():
        lang_to_words.setdefault(lg, []).append(w)
        word_weights[(w, lg)] = wt
    langs = sorted(lang_to_words.keys())
    n_total = sum(len(ws) for ws in lang_to_words.values())
    print(f'Detecting across {len(langs)} languages ({n_total} words total)')

    ch_adapter.execute("TRUNCATE TABLE lltk.stopwords")
    all_words, all_langs, all_weights = [], [], []
    for lg, ws in lang_to_words.items():
        for w in ws:
            all_words.append(w)
            all_langs.append(lg)
            all_weights.append(word_weights[(w, lg)])

    # Check if stopwords table has weight column; if not, recreate
    try:
        ch_adapter.client.insert_arrow(
            'stopwords',
            pa.Table.from_arrays(
                [pa.array(all_words), pa.array(all_langs),
                 pa.array(all_weights, type=pa.float32())],
                names=['word', 'lang', 'weight'],
            ),
        )
    except Exception:
        # Old schema without weight column — recreate
        ch_adapter.execute("DROP TABLE IF EXISTS lltk.stopwords")
        ch_adapter.execute("""
            CREATE TABLE lltk.stopwords (
                word   LowCardinality(String),
                lang   LowCardinality(String),
                weight Float32 DEFAULT 1.0
            ) ENGINE = MergeTree() ORDER BY word
        """)
        ch_adapter.client.insert_arrow(
            'stopwords',
            pa.Table.from_arrays(
                [pa.array(all_words), pa.array(all_langs),
                 pa.array(all_weights, type=pa.float32())],
                names=['word', 'lang', 'weight'],
            ),
        )

    # Sanity: text_words populated?
    n_text_words = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
    if n_text_words == 0:
        raise RuntimeError(
            'lltk.text_words is empty. Run `lltk db-text-words` first.'
        )

    # Skip filter
    if skip_existing:
        already = ch_adapter.query(
            "SELECT count(DISTINCT _id) FROM lltk.text_langs"
        )[0][0]
        if already:
            print(f'  skipping {already:,} already-processed texts')
        skip_sql = "AND tw._id NOT IN (SELECT _id FROM lltk.text_langs)"
    else:
        already = 0
        skip_sql = ''

    # Per-language weighted hit sums via JOIN
    sumif_cols = ',\n           '.join(
        f"sumIf(tw.count * sw.weight, sw.lang = '{lg}') AS {lg}_hits"
        for lg in langs
    )
    hits_arr = '[' + ', '.join(f'{lg}_hits' for lg in langs) + ']'
    langs_arr = '[' + ', '.join(f"'{lg}'" for lg in langs) + ']'

    sql = f"""
        INSERT INTO lltk.text_langs
            (_id, lang_detected, lang_coverage, lang_confidence, detected_at)
        WITH
          hits AS (
            SELECT tw._id AS _id,
                   {sumif_cols}
            FROM lltk.text_words tw
            INNER JOIN lltk.stopwords sw ON tw.word = sw.word
            WHERE 1 {skip_sql}
            GROUP BY tw._id
          ),
          joined AS (
            SELECT h._id AS _id,
                   ts.n_words AS total_tokens,
                   {hits_arr} AS hits_arr
            FROM hits h
            INNER JOIN lltk.text_stats ts ON h._id = ts._id
          ),
          ranked AS (
            SELECT _id, total_tokens, hits_arr,
                   arrayMax(hits_arr) AS top_hits,
                   {langs_arr}[indexOf(hits_arr, arrayMax(hits_arr))] AS top_lang,
                   arraySort(x -> -x, hits_arr)[2] AS second_hits
            FROM joined
          )
        SELECT
          _id,
          multiIf(
            total_tokens < {min_tokens}, 'null',
            top_hits = 0,                'unknown',
            toFloat64(top_hits) / toFloat64(total_tokens) < {coverage_threshold},
                                          'unknown',
            (top_hits * 1.0) / if(second_hits = 0, 1, second_hits)
              < {confidence_threshold} AND second_hits > 0,
                                          'unknown',
                                           top_lang
          ) AS lang_detected,
          toFloat32(if(top_hits = 0, 0.0,
                       toFloat64(top_hits) / toFloat64(total_tokens))) AS lang_coverage,
          toFloat32(if(second_hits = 0, 999.0,
                       toFloat64(top_hits) / toFloat64(second_hits))) AS lang_confidence,
          now()
        FROM ranked
    """

    # tqdm progress via system.processes polling
    import uuid, threading
    from lltk.tools.tools import get_tqdm
    qid = str(uuid.uuid4())

    stats_skip = (
        "WHERE _id NOT IN (SELECT _id FROM lltk.text_langs)"
        if skip_existing else ""
    )
    n_remaining = ch_adapter.query(
        f"SELECT count() FROM lltk.text_stats {stats_skip}"
    )[0][0]
    print(f'  {n_remaining:,} texts to process')
    if n_remaining == 0:
        return {}

    pbar = get_tqdm(total=n_remaining, desc='detect_langs',
                    unit='text', unit_scale=True) if progress else None
    stop_flag = {'done': False}

    def _watch():
        from lltk.tools.db_adapter import get_adapter
        import os as _os
        url = _os.environ.get(
            'LLTK_CLICKHOUSE_URL',
            f'clickhouse://{ch_adapter.username}:{ch_adapter._password}'
            f'@{ch_adapter.host}:{ch_adapter.port}/{ch_adapter.database}',
        )
        watcher = get_adapter(url)
        while not stop_flag['done']:
            try:
                rows = watcher.query(
                    f"SELECT written_rows FROM system.processes "
                    f"WHERE query_id = '{qid}'"
                )
                if rows and pbar:
                    now = rows[0][0]
                    if now > (pbar.n or 0):
                        pbar.update(min(now - pbar.n, n_remaining - pbar.n))
            except Exception:
                pass
            time.sleep(0.5)
        watcher.close()

    thread = threading.Thread(target=_watch, daemon=True)
    thread.start()

    t0 = time.time()
    print('Running server-side INSERT...')
    try:
        ch_adapter.client.command(sql, settings={'query_id': qid})
    finally:
        stop_flag['done'] = True
        thread.join(timeout=2)
        if pbar:
            pbar.close()

    elapsed = time.time() - t0
    print(f'  done in {elapsed:.1f}s ({n_remaining/max(elapsed, 1):.0f}/s)')

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
