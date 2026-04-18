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

    Args:
      corpora: list of corpus ids to rebuild; default = all
      force:   TRUNCATE before rebuilding (global) or DELETE per-corpus
    """
    from lltk.tools.tools import get_tqdm

    # Discover corpora from text_freqs
    if corpora is None:
        corpora = [r[0] for r in ch_adapter.query("""
            SELECT corpus, count() AS n
            FROM lltk.text_freqs
            GROUP BY corpus ORDER BY n
        """)]

    if force:
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            ch_adapter.execute(
                f"ALTER TABLE lltk.text_words DELETE WHERE corpus IN ({cl}) "
                f"SETTINGS mutations_sync=1"
            )
        else:
            ch_adapter.execute("TRUNCATE TABLE lltk.text_words")

    print(f'Building text_words from text_freqs ({len(corpora)} corpora)...')
    t0 = time.time()
    total_rows = 0

    it = get_tqdm(corpora, desc='text_words') if progress else corpora
    for corpus in it:
        tc = time.time()
        c_esc = corpus.replace("'", "''")

        # Skip already-built rows for this corpus (resumable).
        where_skip = ""
        if not force:
            existing = ch_adapter.query(f"""
                SELECT count() FROM lltk.text_words
                WHERE corpus = '{c_esc}'
            """)[0][0]
            if existing > 0:
                # Partial build: skip _ids already represented.
                where_skip = (
                    f"AND _id NOT IN (SELECT DISTINCT _id FROM lltk.text_words "
                    f"WHERE corpus = '{c_esc}')"
                )

        ch_adapter.execute(f"""
            INSERT INTO lltk.text_words (word, _id, count, corpus)
            SELECT w.1 AS word, _id, w.2 AS count, corpus
            FROM lltk.text_freqs
            ARRAY JOIN CAST(freqs, 'Array(Tuple(String, UInt32))') AS w
            WHERE corpus = '{c_esc}' {where_skip}
        """)

        n = ch_adapter.query(f"""
            SELECT count() FROM lltk.text_words WHERE corpus = '{c_esc}'
        """)[0][0]
        total_rows = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
        if progress and hasattr(it, 'set_postfix'):
            it.set_postfix(cur=f'{corpus}: {n:,}', total=f'{total_rows:,}')

    print(f'\ntext_words total: {total_rows:,} rows in {time.time()-t0:.0f}s')
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
