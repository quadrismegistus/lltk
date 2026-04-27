"""
Ngram queries against ClickHouse.

Design: no per-word pre-aggregation. `lltk.text_words` (ORDER BY word, _id)
already answers any ngram query in <1s via a prefix-range scan, so the big
`word_year_corpus` cache is redundant. We only cache `year_corpus_totals`
(tiny — years × corpora × genres) because it's the per-query denominator
for normalization, and re-aggregating `text_stats` on every request would
waste half a second each time.

Functions:
    build_year_corpus_totals(ch)         — (re)build the denominator cache
    ngram_ch(ch, words, ...)             — time-series numerator/denom
    ngram_examples_ch(ch, word, ...)     — texts scoring highest on a word
    ngram_collocates_ch(ch, word, ...)   — doc-level co-occurrence
    has_word_index_ch(ch)                — check text_words is populated
"""

import time

from logmap import logmap


# ── Build year_corpus_totals ─────────────────────────────────────────────

def build_year_corpus_totals(ch_adapter, corpora=None):
    """Aggregate texts × text_stats × match_groups → year_corpus_totals.

    Uses text_stats (one row per text) rather than rescanning text_freqs,
    so this is fast even at corpus scale: ~seconds over 2.8M texts.
    """
    corpus_filter = ''
    if corpora:
        cl = ', '.join(f"'{c}'" for c in corpora)
        corpus_filter = f"AND t.corpus IN ({cl})"

    with logmap('Building year_corpus_totals...') as log:
        t0 = time.time()
        ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.year_corpus_totals")
        ch_adapter.execute(f"""
            INSERT INTO lltk.year_corpus_totals
                (year, corpus, genre, n_texts, total_words,
                 n_texts_dedup, total_words_dedup)
            SELECT
                t.year                                                AS year,
                t.corpus                                              AS corpus,
                t.genre                                               AS genre,
                count()                                               AS n_texts,
                sum(ifNull(ts.n_words, 0))                            AS total_words,
                countIf(mg.rank IS NULL OR mg.rank = 0)               AS n_texts_dedup,
                sumIf(ifNull(ts.n_words, 0),
                      mg.rank IS NULL OR mg.rank = 0)                 AS total_words_dedup
            FROM (SELECT _id, year, corpus, genre FROM lltk.texts FINAL
                  WHERE year IS NOT NULL {corpus_filter}) AS t
            INNER JOIN (SELECT _id, n_words FROM lltk.text_stats FINAL) AS ts
                ON t._id = ts._id
            LEFT JOIN (SELECT _id, rank FROM lltk.match_groups FINAL) AS mg
                ON t._id = mg._id
            GROUP BY t.year, t.corpus, t.genre
        """)
        n = ch_adapter.query("SELECT count() FROM lltk.year_corpus_totals")[0][0]
        log.debug(f'year_corpus_totals: {n:,} rows ({time.time()-t0:.1f}s)')
        return n


# Back-compat alias — CLI `db-wordindex` still calls this name.
def build_word_index_ch(ch_adapter, vocab_size=None, min_count=None,
                         corpora=None):
    """Build only the denominator cache. `text_words` is the numerator
    and is built by `lltk db-text-words`, not here."""
    return build_year_corpus_totals(ch_adapter, corpora=corpora)


# ── WHERE-clause helpers ─────────────────────────────────────────────────

def _period_expr(by):
    """'year' or 'decade' → SQL expression for the time bucket."""
    if by in ('year', 'y'):
        return 't.year'
    if by in ('decade', 'd'):
        return 'intDiv(t.year, 10) * 10'
    raise ValueError(f'by must be year|decade, got {by!r}')


def _text_filters(genre=None, corpus=None,
                  year_min=None, year_max=None, lang=None):
    """Build WHERE clauses against lltk.texts (unqualified column names
    so they work inside a subquery over texts)."""
    wheres = ['year IS NOT NULL']
    if genre:
        g = genre if isinstance(genre, (list, tuple)) else [genre]
        wheres.append(
            f"genre IN ({', '.join(repr(str(x)) for x in g)})"
        )
    if corpus:
        c = corpus if isinstance(corpus, (list, tuple)) else [corpus]
        wheres.append(
            f"corpus IN ({', '.join(repr(str(x)) for x in c)})"
        )
    if year_min is not None:
        wheres.append(f'year >= {int(year_min)}')
    if year_max is not None:
        wheres.append(f'year <= {int(year_max)}')
    if lang:
        wheres.append(f"lang = '{str(lang)}'")
    return wheres


def _dedup_join(dedup, dedup_by='rank'):
    """Returns (join_sql, extra_where) for dedup via match_groups.

    dedup_by='rank'   — keep rank=0 representatives + texts with no group
    dedup_by='oldest' — keep one text per group (earliest year) + ungrouped
                        (for now we lean on rank=0 for oldest too — oldest
                        requires a window, can add later if needed)
    """
    if not dedup:
        return '', ''
    join = ('LEFT JOIN (SELECT _id, rank FROM lltk.match_groups FINAL) AS mg '
            'ON t._id = mg._id')
    where = 'AND (mg.rank IS NULL OR mg.rank = 0)'
    return join, where


# ── Ngram time-series ────────────────────────────────────────────────────

def ngram_ch(ch_adapter, words, genre=None, corpus=None,
             year_min=None, year_max=None, lang=None,
             normalize='per_million', dedup=False, dedup_by='rank',
             by_corpus=False, by='decade'):
    """Time-series of word frequency from lltk.text_words + year_corpus_totals.

    Returns a DataFrame with columns:
        period, word, value, raw_count, n_texts[, corpus]

    `value` is per_million if normalize='per_million', else raw count.
    """
    if isinstance(words, str):
        words = [words]
    words = [w.lower() for w in words if w and w.strip()]
    if not words:
        import pandas as pd
        return pd.DataFrame(columns=['period', 'word', 'value',
                                     'raw_count', 'n_texts'])
    words_sql = ', '.join(f"'{w}'" for w in words)

    period_sql = _period_expr(by)
    wheres = _text_filters(genre=genre, corpus=corpus,
                           year_min=year_min, year_max=year_max, lang=lang)
    dedup_join, dedup_where = _dedup_join(dedup, dedup_by=dedup_by)

    group_cols = [f'{period_sql} AS period', 'tw.word AS word']
    select_cols = ['period', 'word']
    if by_corpus:
        group_cols.append('t.corpus AS corpus')
        select_cols.append('corpus')

    where_sql = ' AND '.join(wheres)
    group_sql = ', '.join(f'{i+1}' for i in range(len(select_cols)))
    select_prefix = ', '.join(select_cols)

    num_sql = f"""
        SELECT
            {', '.join(group_cols)},
            sum(tw.count)               AS raw_count,
            uniqExact(tw._id)           AS n_texts
        FROM lltk.text_words tw
        INNER JOIN (SELECT _id, year, corpus, genre
                    FROM lltk.texts FINAL
                    WHERE {where_sql}) AS t
            ON tw._id = t._id
        {dedup_join}
        WHERE tw.word IN ({words_sql})
          {dedup_where}
        GROUP BY {group_sql}
        ORDER BY period, word
    """
    num_df = ch_adapter.query_df(num_sql)

    if normalize == 'per_million':
        # Denominator from cached year_corpus_totals
        denom_period = (
            'year' if by in ('year', 'y') else 'intDiv(year, 10) * 10'
        )
        denom_wheres = []
        if genre:
            g = genre if isinstance(genre, (list, tuple)) else [genre]
            denom_wheres.append(
                f"genre IN ({', '.join(repr(str(x)) for x in g)})"
            )
        if corpus:
            c = corpus if isinstance(corpus, (list, tuple)) else [corpus]
            denom_wheres.append(
                f"corpus IN ({', '.join(repr(str(x)) for x in c)})"
            )
        if year_min is not None:
            denom_wheres.append(f'year >= {int(year_min)}')
        if year_max is not None:
            denom_wheres.append(f'year <= {int(year_max)}')
        denom_where_sql = (' WHERE ' + ' AND '.join(denom_wheres)
                           if denom_wheres else '')

        denom_group = [f'{denom_period} AS period']
        if by_corpus:
            denom_group.append('corpus')
        denom_group_sql = ', '.join(f'{i+1}' for i in range(len(denom_group)))
        total_col = ('total_words_dedup' if dedup else 'total_words')
        denom_sql = f"""
            SELECT {', '.join(denom_group)},
                   sum({total_col}) AS total_words
            FROM lltk.year_corpus_totals
            {denom_where_sql}
            GROUP BY {denom_group_sql}
        """
        denom_df = ch_adapter.query_df(denom_sql)

        merge_on = ['period'] + (['corpus'] if by_corpus else [])
        num_df = num_df.merge(denom_df, on=merge_on, how='left')
        num_df['value'] = (
            num_df['raw_count'].astype(float) * 1_000_000.0
            / num_df['total_words'].replace(0, 1).astype(float)
        )
        num_df = num_df.drop(columns=['total_words'])
    else:
        num_df['value'] = num_df['raw_count'].astype(float)

    return num_df


# ── Drill-down: texts scoring highest on a word ─────────────────────────

def ngram_examples_ch(ch_adapter, word, genre=None, corpus=None,
                      year_min=None, year_max=None, lang=None,
                      limit=20, dedup=False, dedup_by='rank'):
    """Find texts where `word` appears most frequently per million.

    Sub-second — the `word` WHERE clause is a prefix-range lookup on the
    (word, _id) index.
    """
    word = word.lower()
    wheres = _text_filters(genre=genre, corpus=corpus,
                           year_min=year_min, year_max=year_max, lang=lang)
    dedup_join, dedup_where = _dedup_join(dedup, dedup_by=dedup_by)
    where_sql = ' AND '.join(wheres)

    sql = f"""
        SELECT
            t._id                                                   AS _id,
            t.corpus                                                AS corpus,
            t.title                                                 AS title,
            t.author                                                AS author,
            t.year                                                  AS year,
            t.genre                                                 AS genre,
            tw.count                                                AS count,
            toFloat64(tw.count) * 1000000.0
                / nullIf(toFloat64(ts.n_words), 0)                  AS per_million
        FROM lltk.text_words tw
        INNER JOIN (SELECT _id, corpus, title, author, year, genre
                    FROM lltk.texts FINAL
                    WHERE {where_sql}) AS t
            ON tw._id = t._id
        INNER JOIN (SELECT _id, n_words FROM lltk.text_stats FINAL) AS ts
            ON tw._id = ts._id
        {dedup_join}
        WHERE tw.word = '{word}'
          AND ts.n_words > 0
          {dedup_where}
        ORDER BY per_million DESC
        LIMIT {int(limit)}
    """
    return ch_adapter.query_df(sql)


# ── Readiness probe ─────────────────────────────────────────────────────

def has_word_index_ch(ch_adapter):
    """text_words is the live ngram index. year_corpus_totals is the
    normalization cache. Require both."""
    try:
        tw = ch_adapter.query("SELECT count() FROM lltk.text_words")[0][0]
        yc = ch_adapter.query(
            "SELECT count() FROM lltk.year_corpus_totals"
        )[0][0]
        return tw > 0 and yc > 0
    except Exception:
        return False
