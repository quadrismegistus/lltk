"""
Build lltk.text_words — the flat (word, _id, count) long-format inversion of
text_freqs — and lltk.text_stats (per-text totals).

text_words is ORDER BY (word, _id) so per-word queries hit a contiguous
index range, not a full-column Map scan. ngram timeseries, MFW, detect_langs,
stylometry etc. benefit.

Builds in chunks by corpus to keep peak memory bounded during the sort
step. ReplacingMergeTree on text_stats.
"""

import time


def build_text_words(ch_adapter, corpora=None, force=False, progress=True):
    """Populate lltk.text_words from lltk.text_freqs via ARRAY JOIN mapItems.

    Strategy:
      - Fresh build (text_words empty OR force=True with no corpora filter):
          one big INSERT that scans text_freqs once. Fastest.
      - Incremental (specific corpora, or partial resume):
          per-corpus loop, anti-join against existing rows for safe resume.

    Args:
      corpora: list of corpus ids to rebuild; default = all
      force:   TRUNCATE (if corpora=None) or DELETE per-corpus then re-ingest
    """
    from lltk.tools.tools import get_tqdm

    # Decide build mode: one big INSERT vs per-corpus loop.
    current_rows = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
    fresh_build = (corpora is None) and (force or current_rows == 0)

    # Handle destructive force
    if force and corpora is None:
        ch_adapter.execute("TRUNCATE TABLE lltk.text_words")
        current_rows = 0
    elif force and corpora:
        cl = ', '.join(f"'{c}'" for c in corpora)
        ch_adapter.execute(
            f"ALTER TABLE lltk.text_words DELETE WHERE corpus IN ({cl}) "
            f"SETTINGS mutations_sync=1"
        )

    t0 = time.time()

    if fresh_build:
        # One big INSERT — scans text_freqs once, streams ARRAY JOIN + write.
        # CH handles this with bounded memory via max_insert_block_size batching.
        print('Building text_words from text_freqs (fresh build, one INSERT)...')
        import uuid, threading
        qid = str(uuid.uuid4())

        # Progress thread (poll written_rows via system.processes)
        pbar = None
        if progress:
            # Estimate expected rows from avg unique words per text
            estimate = ch_adapter.query("""
                SELECT sum(length(mapKeys(freqs))) FROM lltk.text_freqs
            """)[0][0]
            pbar = get_tqdm(total=estimate, desc='text_words',
                            unit='row', unit_scale=True)

        stop_flag = {'done': False}

        def _watch():
            from lltk.db.adapter import get_adapter
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
                            pbar.update(now - pbar.n)
                except Exception:
                    pass
                time.sleep(1)
            watcher.close()

        thread = threading.Thread(target=_watch, daemon=True)
        thread.start()

        try:
            ch_adapter.client.command(
                """
                INSERT INTO lltk.text_words (word, _id, count, corpus)
                SELECT w.1 AS word, _id, w.2 AS count, corpus
                FROM lltk.text_freqs
                ARRAY JOIN CAST(freqs, 'Array(Tuple(String, UInt32))') AS w
                """,
                settings={'query_id': qid},
            )
        finally:
            stop_flag['done'] = True
            thread.join(timeout=2)
            if pbar:
                pbar.close()

        total_rows = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
        print(f'\ntext_words: {total_rows:,} rows in {time.time()-t0:.0f}s')
        return total_rows

    # Incremental: per-corpus loop with resume-safety
    if corpora is None:
        corpora = [r[0] for r in ch_adapter.query("""
            SELECT corpus, count() AS n
            FROM lltk.text_freqs
            GROUP BY corpus ORDER BY n
        """)]

    print(f'Building text_words incrementally ({len(corpora)} corpora)...')
    it = get_tqdm(corpora, desc='text_words') if progress else corpora
    for corpus in it:
        c_esc = corpus.replace("'", "''")
        where_skip = ''
        existing = ch_adapter.query(
            f"SELECT count() FROM lltk.text_words WHERE corpus = '{c_esc}'"
        )[0][0]
        if existing > 0 and not force:
            where_skip = (
                f"AND _id NOT IN (SELECT DISTINCT _id FROM lltk.text_words "
                f"WHERE corpus = '{c_esc}')"
            )
        ch_adapter.execute(f"""
            INSERT INTO lltk.text_words (word, _id, count, corpus)
            SELECT w.1, _id, w.2, corpus
            FROM lltk.text_freqs
            ARRAY JOIN CAST(freqs, 'Array(Tuple(String, UInt32))') AS w
            WHERE corpus = '{c_esc}' {where_skip}
        """)
        total_rows = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
        if progress and hasattr(it, 'set_postfix'):
            it.set_postfix(total=f'{total_rows:,}')

    total_rows = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
    print(f'\ntext_words: {total_rows:,} rows in {time.time()-t0:.0f}s')
    return total_rows


def build_text_stats(ch_adapter, force=False):
    """Populate lltk.text_stats (_id, corpus, n_words, n_unique_words)
    from text_freqs. Cheap: one row per text, not per word.
    """
    if force:
        ch_adapter.execute("TRUNCATE TABLE lltk.text_stats")

    skip_sql = ('' if force else
                'WHERE _id NOT IN (SELECT _id FROM lltk.text_stats)')

    print('Building text_stats from text_freqs...')
    t0 = time.time()
    ch_adapter.execute(f"""
        INSERT INTO lltk.text_stats (_id, corpus, n_words, n_unique_words)
        SELECT _id,
               corpus,
               arraySum(mapValues(freqs)) AS n_words,
               length(mapKeys(freqs)) AS n_unique_words
        FROM lltk.text_freqs
        {skip_sql}
    """)

    n = ch_adapter.query("SELECT count() FROM lltk.text_stats FINAL")[0][0]
    print(f'  text_stats: {n:,} rows in {time.time()-t0:.0f}s')
    return n
