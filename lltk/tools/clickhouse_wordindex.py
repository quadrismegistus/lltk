"""
Build word_year_corpus + year_corpus_totals tables in ClickHouse.

ClickHouse's native Map column + arrayJoin make this much simpler than
the DuckDB two-pass worker approach: everything is one GROUP BY over
lltk.text_freqs.

word_year_corpus columns:
    word, year, corpus, genre, word_count, n_texts,
    word_count_dedup, n_texts_dedup

_dedup columns restrict to match-group representatives (rank=0 or no
match group), so ngram queries can filter out duplicated editions.
"""

import time


def build_word_index_ch(ch_adapter, vocab_size=50_000, min_count=1,
                         corpora=None):
    """Build word_year_corpus + year_corpus_totals in one pass of CH aggregation."""

    corpus_filter = ''
    if corpora:
        cl = ', '.join(f"'{c}'" for c in corpora)
        corpus_filter = f"AND t.corpus IN ({cl})"

    # is_preferred: rank=0 in match_groups, OR no match group at all
    # Join-friendly: LEFT JOIN match_groups, rank IS NULL means no group.
    print('build_word_index: joining texts + match_groups + freqs...')

    # Step 1: build a pre-aggregated temp table year_corpus_totals
    print('Building year_corpus_totals...')
    t0 = time.time()
    ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.year_corpus_totals")
    ch_adapter.execute(f"""
        INSERT INTO lltk.year_corpus_totals
            (year, corpus, genre, n_texts, total_words,
             n_texts_dedup, total_words_dedup)
        SELECT t.year,
               t.corpus,
               t.genre,
               count()                                              AS n_texts,
               sum(arraySum(mapValues(f.freqs)))                    AS total_words,
               countIf(mg.rank IS NULL OR mg.rank = 0)              AS n_texts_dedup,
               sumIf(arraySum(mapValues(f.freqs)),
                     mg.rank IS NULL OR mg.rank = 0)                AS total_words_dedup
        FROM (SELECT _id, year, corpus, genre FROM lltk.texts FINAL
              WHERE year IS NOT NULL {corpus_filter}) AS t
        INNER JOIN (SELECT _id, freqs FROM lltk.text_freqs FINAL) AS f
          ON t._id = f._id
        LEFT JOIN (SELECT _id, rank FROM lltk.match_groups FINAL) AS mg
          ON t._id = mg._id
        GROUP BY t.year, t.corpus, t.genre
    """)
    n = ch_adapter.query("SELECT count() FROM lltk.year_corpus_totals")[0][0]
    print(f'  year_corpus_totals: {n:,} rows ({time.time()-t0:.1f}s)')

    # Step 2: build word_year_corpus. Unnest freqs Map via arrayJoin.
    print(f'Building word_year_corpus (min_count={min_count})...')
    t0 = time.time()
    ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.word_year_corpus")
    # Filter: alphabetic words, length >= 2, count >= min_count
    word_re = r"^[a-zA-Z][a-zA-Z''\\-]*$"
    ch_adapter.execute(f"""
        INSERT INTO lltk.word_year_corpus
            (word, year, corpus, genre, word_count, n_texts,
             word_count_dedup, n_texts_dedup)
        WITH unnested AS (
            SELECT t.year, t.corpus, t.genre,
                   lower(kv.1) AS word, kv.2 AS cnt,
                   (mg.rank IS NULL OR mg.rank = 0) AS is_pref
            FROM (SELECT _id, year, corpus, genre FROM lltk.texts FINAL
                  WHERE year IS NOT NULL {corpus_filter}) AS t
            INNER JOIN (SELECT _id, freqs FROM lltk.text_freqs FINAL) AS f
              ON t._id = f._id
            ARRAY JOIN mapItems(f.freqs) AS kv
            LEFT JOIN (SELECT _id, rank FROM lltk.match_groups FINAL) AS mg
              ON t._id = mg._id
            WHERE cnt >= {min_count}
              AND match(lower(kv.1), '{word_re}')
        )
        SELECT word, year, corpus, genre,
               sum(cnt)            AS word_count,
               count()             AS n_texts,
               sumIf(cnt, is_pref) AS word_count_dedup,
               countIf(is_pref)    AS n_texts_dedup
        FROM unnested
        GROUP BY word, year, corpus, genre
    """)
    n = ch_adapter.query("SELECT count() FROM lltk.word_year_corpus")[0][0]
    print(f'  word_year_corpus: {n:,} rows ({time.time()-t0:.1f}s)')

    # Optionally trim to top-N vocabulary
    if vocab_size and vocab_size > 0:
        t0 = time.time()
        vocab_count = ch_adapter.query(
            "SELECT countDistinct(word) FROM lltk.word_year_corpus"
        )[0][0]
        if vocab_count > vocab_size:
            print(f'Trimming to top {vocab_size:,} words '
                  f'(current vocabulary: {vocab_count:,})...')
            ch_adapter.execute(f"""
                ALTER TABLE lltk.word_year_corpus DELETE
                WHERE word NOT IN (
                    SELECT word FROM (
                        SELECT word, sum(word_count) AS total
                        FROM lltk.word_year_corpus
                        GROUP BY word
                        ORDER BY total DESC
                        LIMIT {vocab_size}
                    )
                )
                SETTINGS mutations_sync=1
            """)
            n2 = ch_adapter.query("SELECT count() FROM lltk.word_year_corpus")[0][0]
            print(f'  post-trim: {n2:,} rows ({time.time()-t0:.1f}s)')


def ngram_ch(ch_adapter, words, genre=None, corpus=None, year_min=None,
             year_max=None, dedup=False, by_corpus=False):
    """Query word_year_corpus for ngram time-series.

    words: str or list of str (lowercase matched)
    genre / corpus / year range: filters
    dedup: use *_dedup columns (only match-group representatives)
    by_corpus: include corpus in GROUP BY (returns per-corpus series)
    """
    if isinstance(words, str):
        words = [words]
    words_sql = ', '.join(f"'{w.lower()}'" for w in words)

    wheres = [f"word IN ({words_sql})"]
    if genre:
        g = genre if isinstance(genre, (list, tuple)) else [genre]
        gl = ', '.join(f"'{x}'" for x in g)
        wheres.append(f"genre IN ({gl})")
    if corpus:
        c = corpus if isinstance(corpus, (list, tuple)) else [corpus]
        cl = ', '.join(f"'{x}'" for x in c)
        wheres.append(f"corpus IN ({cl})")
    if year_min:
        wheres.append(f"year >= {int(year_min)}")
    if year_max:
        wheres.append(f"year <= {int(year_max)}")
    where_sql = ' AND '.join(wheres)

    group_cols = ['word', 'year']
    if by_corpus:
        group_cols.append('corpus')

    if dedup:
        agg_cols = 'sum(word_count_dedup) AS word_count, sum(n_texts_dedup) AS n_texts'
    else:
        agg_cols = 'sum(word_count) AS word_count, sum(n_texts) AS n_texts'

    group_sql = ', '.join(group_cols)
    return ch_adapter.query_df(f"""
        SELECT {group_sql}, {agg_cols}
        FROM lltk.word_year_corpus
        WHERE {where_sql}
        GROUP BY {group_sql}
        ORDER BY year
    """)


def has_word_index_ch(ch_adapter):
    try:
        n = ch_adapter.query("SELECT count() FROM lltk.word_year_corpus")[0][0]
        return n > 0
    except Exception:
        return False
