"""
Cross-corpus text matching in ClickHouse.

Ports the multi-tier matcher from MetaDB.match():
  Tier 0: id_link          — shared IDs declared in corpus LINKS
  Tier 1a: exact_norm      — same title_norm + author_norm
  Tier 1b: exact_norm_year — same title_norm + year (authorless only)
  Tier 2a: containment     — short title ⊆ long title, same author
  Tier 2b: containment_year— short title ⊆ long title, same year (authorless)
  Tier 3: fuzzy_title      — Jaro-Winkler > 0.85 within author blocks (opt-in)

SQL tiers stay server-side. Python tiers (containment + fuzzy) read
candidate rows and write pair batches via INSERT. Dedup is handled by
the matches table being ReplacingMergeTree ORDER BY (_id_a, _id_b) —
re-running is idempotent.

Match groups are built from connected components via NetworkX, then
inserted into lltk.match_groups with per-row rank based on
CORPUS_SOURCE_RANKS.
"""

import time
import pandas as pd
import networkx as nx
from logmap import logmap

from lltk.db.adapter import ch_quote


def _ordered_pair(a, b):
    return (a, b) if a < b else (b, a)

@logmap.fn
def match_clickhouse(ch_adapter, corpora=None, fuzzy=False, containment=True,
                     progress=True):
    """Run the full matching pipeline against ClickHouse lltk.texts.

    Writes to lltk.matches + lltk.match_groups.
    """
    from lltk.tools.tools import get_tqdm
    with logmap('Matching texts...') as log:

        corpus_filter = ''
        corpus_where = ''
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            corpus_filter = f'AND a.corpus IN ({cl}) AND b.corpus IN ({cl})'
            corpus_where = f'AND corpus IN ({cl})'
            log.debug(f'Matching {len(corpora)} corpora: {", ".join(corpora)}')

        # Tier 0: ID-based linking from corpus LINKS declarations
        with logmap('ID-based linking from corpus LINKS...') as log:
            _match_by_links_ch(ch_adapter, corpora)
            n = ch_adapter.query(
                "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'id_link'"
            )[0][0]
            log.debug(f'  ID links: {n:,} pairs')

        # Tier 1a: exact title_norm + author_norm (chain via LEAD window)
        with logmap('Exact title + author matching...') as log:
            ch_adapter.execute(f"""
                INSERT INTO lltk.matches (_id_a, _id_b, similarity, match_type)
                SELECT
                if(a._id < b._id, a._id, b._id) AS _id_a,
                if(a._id < b._id, b._id, a._id) AS _id_b,
                1.0 AS similarity,
                'exact_norm' AS match_type
                FROM (
                    SELECT _id, title_norm, author_norm, corpus,
                        lead(_id) OVER (
                            PARTITION BY title_norm, author_norm
                            ORDER BY _id
                        ) AS next_id
                    FROM lltk.texts FINAL
                    WHERE title_norm != ''
                    AND author_norm != ''
                    AND length(title_norm) > 5
                ) a
                INNER JOIN (SELECT _id, corpus FROM lltk.texts FINAL) b ON a.next_id = b._id
                WHERE a.next_id != ''
                AND a._id != b._id
                {corpus_filter}
            """)
            n = ch_adapter.query(
                "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'exact_norm'"
            )[0][0]
            log.debug(f'  Exact (by author): {n:,} pairs')

        # Tier 1b: exact title_norm + year (authorless only)
        with logmap('Exact title + year matching (authorless)...') as log:
            ch_adapter.execute(f"""
                INSERT INTO lltk.matches (_id_a, _id_b, similarity, match_type)
                SELECT
                if(a._id < b._id, a._id, b._id) AS _id_a,
                if(a._id < b._id, b._id, a._id) AS _id_b,
                1.0 AS similarity,
                'exact_norm_year' AS match_type
                FROM (
                    SELECT _id, title_norm, year, corpus,
                        lead(_id) OVER (
                            PARTITION BY title_norm, year
                            ORDER BY _id
                        ) AS next_id
                    FROM lltk.texts FINAL
                    WHERE title_norm != ''
                    AND (author_norm IS NULL OR author_norm = '')
                    AND year IS NOT NULL
                    AND length(title_norm) > 10
                ) a
                INNER JOIN (SELECT _id, corpus FROM lltk.texts FINAL) b ON a.next_id = b._id
                WHERE a.next_id != ''
                AND a._id != b._id
                {corpus_filter}
            """)
            n = ch_adapter.query(
                "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'exact_norm_year'"
            )[0][0]
            log.debug(f'  Exact (by year, authorless): {n:,} pairs')

        # Tier 2: containment (Python)
        if containment:
            _containment_pass(ch_adapter, corpora=corpora, corpus_where=corpus_where,
                            progress=progress)

        # Tier 3: fuzzy (Python, opt-in)
        if fuzzy:
            _fuzzy_pass(ch_adapter, corpora=corpora, corpus_where=corpus_where,
                        progress=progress)

        # Build match groups
        with logmap('Computing match groups...') as log:
            _compute_match_groups_ch(ch_adapter)
            total = ch_adapter.query("SELECT count() FROM lltk.matches FINAL")[0][0]
            n_groups = ch_adapter.query(
                "SELECT countDistinct(group_id) FROM lltk.match_groups FINAL"
            )[0][0]
            n_in_groups = ch_adapter.query(
                "SELECT count() FROM lltk.match_groups FINAL"
            )[0][0]
            log.debug(f'Done: {total:,} match pairs, {n_in_groups:,} texts in {n_groups:,} groups')


def _match_by_links_ch(ch_adapter, corpora=None):
    from lltk.corpus.utils import load_manifest, load_corpus
    from lltk.db.metadb import DB_BLACKLIST

    manifest = load_manifest()
    corpus_ids = set(corpora or [d.get('id', name) for name, d in manifest.items()])

    top_level_cols = {'_id', 'corpus', 'id', 'title', 'author', 'year',
                      'genre', 'genre_raw', 'is_translated',
                      'title_norm', 'author_norm', 'path_freqs', 'lang',
                      'n_words', 'original_lang'}

    for corpus_id in corpus_ids:
        if corpus_id in DB_BLACKLIST:
            continue
        try:
            corpus = load_corpus(corpus_id)
        except Exception:
            continue
        links = dict(getattr(corpus, 'LINKS', None) or {})
        match_links = getattr(corpus, 'MATCH_LINKS', None) or {}
        links.update(match_links)
        if not links:
            continue

        for target_corpus_id, (my_col, their_col) in links.items():
            if corpora and target_corpus_id not in corpus_ids:
                continue
            my_expr = (f"a.{my_col}" if my_col in top_level_cols
                       else f"JSONExtractString(a.meta, '{my_col}')")
            their_expr = (f"b.{their_col}" if their_col in top_level_cols
                          else f"JSONExtractString(b.meta, '{their_col}')")

            try:
                ch_adapter.execute(f"""
                    INSERT INTO lltk.matches (_id_a, _id_b, similarity, match_type)
                    SELECT
                      if(a._id < b._id, a._id, b._id),
                      if(a._id < b._id, b._id, a._id),
                      1.0,
                      'id_link'
                    FROM lltk.texts a
                    INNER JOIN lltk.texts b ON (
                        {my_expr} != '' AND {my_expr} = {their_expr}
                    )
                    WHERE a.corpus = '{corpus_id}'
                      AND b.corpus = '{target_corpus_id}'
                      AND a._id != b._id
                """)
            except Exception as e:
                logmap.debug(f'  id_link {corpus_id}.{my_col} -> {target_corpus_id}.{their_col}: {e}')


def _containment_pass(ch_adapter, corpora=None, corpus_where='', progress=True):
    """Tier 2: short title substring of long title, within author blocks + authorless-by-year."""
    from lltk.tools.tools import get_tqdm

    # Pull existing pairs once (to skip in containment loop)
    existing = set()
    for row in ch_adapter.query("SELECT _id_a, _id_b FROM lltk.matches FINAL"):
        existing.add((row[0], row[1]))

    batch = []

    def _check(rows, match_type='containment', min_short=8, min_sim=0.3):
        for i in range(len(rows)):
            for j in range(i + 1, len(rows)):
                a_id, a_title, a_corp = rows[i][0], rows[i][1], rows[i][2]
                b_id, b_title, b_corp = rows[j][0], rows[j][1], rows[j][2]
                if a_corp == b_corp:
                    continue
                pair = _ordered_pair(a_id, b_id)
                if pair in existing:
                    continue
                short, long = ((a_title, b_title) if len(a_title) <= len(b_title)
                               else (b_title, a_title))
                if len(short) < min_short:
                    continue
                if short in long:
                    sim = len(short) / len(long)
                    if sim < min_sim:
                        continue
                    batch.append((*pair, sim, match_type))
                    existing.add(pair)

    def _flush(rows):
        if not rows:
            return
        df = pd.DataFrame(rows, columns=['_id_a', '_id_b', 'similarity', 'match_type'])
        df = df.drop_duplicates(subset=['_id_a', '_id_b'])
        import pyarrow as pa
        tbl = pa.Table.from_pandas(df, preserve_index=False)
        ch_adapter.client.insert_arrow('matches', tbl)

    # (a) Within author blocks — one big SQL pull, group in Python
    with logmap('Containment (by author)...') as log:
        log.debug('  fetching all eligible (author, title, _id, corpus) rows...')
        rows_all = ch_adapter.query(f"""
            SELECT author_norm, _id, title_norm, corpus
            FROM lltk.texts
            WHERE author_norm != '' AND title_norm != ''
            AND length(title_norm) > 3
            AND author_norm IN (
                SELECT author_norm FROM lltk.texts
                WHERE author_norm != '' AND title_norm != ''
                    AND length(title_norm) > 3
                GROUP BY author_norm
                HAVING count() > 1 AND count() <= 500
            )
            {corpus_where}
        """)
        log.debug(f'  fetched {len(rows_all):,} rows; grouping...')

        # Group by author in Python
        from collections import defaultdict
        groups = defaultdict(list)
        for author, _id, title, corp in rows_all:
            groups[author].append((_id, title, corp))

        it = (log.progress(groups.items(), desc='containment by author', total=len(groups))
            if progress else groups.items())
        for author, rows in it:
            _check(rows)
            if len(batch) >= 10000:
                _flush(batch); batch = []
        if batch:
            _flush(batch); batch = []

        n = ch_adapter.query(
            "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'containment'"
        )[0][0]
        log.debug(f'  By author: {n:,} pairs')

    # (b) Authorless: match by year — same one-query-then-group pattern
    with logmap('Containment (by year, authorless)...') as log:
        rows_all = ch_adapter.query(f"""
            SELECT year, _id, title_norm, corpus
            FROM lltk.texts
            WHERE (author_norm IS NULL OR author_norm = '')
            AND title_norm != ''
            AND year IS NOT NULL AND length(title_norm) > 5
            AND year IN (
                SELECT year FROM lltk.texts
                WHERE (author_norm IS NULL OR author_norm = '')
                    AND title_norm != ''
                    AND year IS NOT NULL AND length(title_norm) > 5
                GROUP BY year
                HAVING count() > 1 AND count() <= 500
            )
            {corpus_where}
        """)
        log.debug(f'  fetched {len(rows_all):,} rows; grouping...')

        year_groups = defaultdict(list)
        for year, _id, title, corp in rows_all:
            year_groups[year].append((_id, title, corp))

        it = (log.progress(year_groups.items(), desc='containment by year',
                    total=len(year_groups)) if progress else year_groups.items())
        for year, rows in it:
            _check(rows, match_type='containment_year', min_short=15)
            if len(batch) >= 10000:
                _flush(batch); batch = []
        if batch:
            _flush(batch)

        n = ch_adapter.query(
            "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'containment_year'"
        )[0][0]
        log.debug(f'  By year (authorless): {n:,} pairs')


def _fuzzy_pass(ch_adapter, corpora=None, corpus_where='', progress=True):
    """Tier 3: Jaro-Winkler > 0.85 within author blocks. Opt-in, slower."""
    from lltk.tools.tools import get_tqdm
    from lltk.db.metadb import _jaro_winkler
    import pyarrow as pa

    with logmap('Fuzzy title matching within author blocks...') as log:
        rows_all = ch_adapter.query(f"""
            SELECT author_norm, _id, title_norm, corpus, year
            FROM lltk.texts
            WHERE author_norm != '' AND title_norm != ''
              AND length(title_norm) > 3
              AND author_norm IN (
                  SELECT author_norm FROM lltk.texts
                  WHERE author_norm != '' AND title_norm != ''
                    AND length(title_norm) > 3
                  GROUP BY author_norm
                  HAVING count() > 1 AND count() <= 200
              )
              {corpus_where}
        """)
        from collections import defaultdict
        fuzzy_groups = defaultdict(list)
        for author, _id, title, corp, year in rows_all:
            fuzzy_groups[author].append((_id, title, corp, year))

        batch = []

        def _flush(rows):
            if not rows:
                return
            df = pd.DataFrame(rows, columns=['_id_a', '_id_b', 'similarity', 'match_type'])
            df = df.drop_duplicates(subset=['_id_a', '_id_b'])
            tbl = pa.Table.from_pandas(df, preserve_index=False)
            ch_adapter.client.insert_arrow('matches', tbl)

        it = (get_tqdm(fuzzy_groups.items(), desc='fuzzy by author',
                       total=len(fuzzy_groups)) if progress else fuzzy_groups.items())
        for author, rows in it:
            for i in range(len(rows)):
                for j in range(i + 1, len(rows)):
                    a_id, a_title, a_corp, a_year = rows[i]
                    b_id, b_title, b_corp, b_year = rows[j]
                    if a_corp == b_corp:
                        continue
                    if a_year and b_year and abs(a_year - b_year) > 20:
                        continue
                    sim = _jaro_winkler(a_title, b_title)
                    if sim > 0.85:
                        pair = _ordered_pair(a_id, b_id)
                        batch.append((*pair, sim, 'fuzzy_title'))
            if len(batch) >= 10000:
                _flush(batch); batch = []
        if batch:
            _flush(batch)

        n = ch_adapter.query(
            "SELECT count() FROM lltk.matches FINAL WHERE match_type = 'fuzzy_title'"
        )[0][0]
        log.debug(f'Fuzzy: {n:,} pairs')


def _compute_match_groups_ch(ch_adapter):
    from lltk.db.metadb import CORPUS_SOURCE_RANKS
    import pyarrow as pa

    pairs = ch_adapter.query("SELECT _id_a, _id_b FROM lltk.matches FINAL")
    ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.match_groups")
    if not pairs:
        return

    G = nx.Graph()
    G.add_edges_from(pairs)

    rows = []
    for gid, component in enumerate(nx.connected_components(G)):
        ranked = sorted(
            component,
            key=lambda x: CORPUS_SOURCE_RANKS.get(
                x.split('/')[0].lstrip('_'), 1000),
        )
        for rank, _id in enumerate(ranked):
            rows.append((_id, gid, rank))

    df = pd.DataFrame(rows, columns=['_id', 'group_id', 'rank'])
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    ch_adapter.client.insert_arrow('match_groups', tbl)


# ── Read helpers ──

def find_matches_ch(ch_adapter, query):
    """Search matches by title substring. Returns DataFrame."""
    q = ch_quote(query)
    return ch_adapter.query_df(f"""
        SELECT m.group_id, t._id, t.title, t.author, t.year, t.corpus
        FROM (SELECT _id, group_id, rank FROM lltk.match_groups FINAL) AS m
        INNER JOIN (SELECT _id, title, author, year, corpus FROM lltk.texts FINAL) AS t
          ON m._id = t._id
        WHERE m.group_id IN (
            SELECT m2.group_id
            FROM (SELECT _id, group_id FROM lltk.match_groups FINAL) AS m2
            INNER JOIN (SELECT _id, title FROM lltk.texts FINAL) AS t2
              ON m2._id = t2._id
            WHERE positionCaseInsensitive(t2.title, '{q}') > 0
        )
        ORDER BY m.group_id, m.rank
    """)


def get_group_ch(ch_adapter, _id):
    """Return all texts in the same match group as `_id`."""
    from lltk.db.metadb_ch import _validate_id, _sql_str
    _validate_id(_id)
    _id_esc = _sql_str(_id)
    return ch_adapter.query_df(f"""
        SELECT t._id, t.title, t.author, t.year, t.corpus, m.rank
        FROM (SELECT _id, group_id, rank FROM lltk.match_groups FINAL) AS m
        INNER JOIN (SELECT _id, title, author, year, corpus FROM lltk.texts FINAL) AS t
          ON m._id = t._id
        WHERE m.group_id = (
            SELECT group_id FROM lltk.match_groups FINAL WHERE _id = '{_id_esc}'
        )
        ORDER BY m.rank
    """)


def match_stats_ch(ch_adapter):
    total = ch_adapter.query("SELECT count() FROM lltk.matches FINAL")[0][0]
    by_type = ch_adapter.query_df("""
        SELECT match_type, count() AS n
        FROM lltk.matches FINAL
        GROUP BY match_type ORDER BY n DESC
    """)
    n_groups = ch_adapter.query(
        "SELECT countDistinct(group_id) FROM lltk.match_groups FINAL"
    )[0][0]
    group_sizes = ch_adapter.query_df("""
        SELECT group_size, count() AS n_groups FROM (
            SELECT group_id, count() AS group_size
            FROM lltk.match_groups FINAL
            GROUP BY group_id
        )
        GROUP BY group_size ORDER BY group_size
    """)
    return {
        'total_matches': total,
        'total_groups': n_groups,
        'by_type': by_type,
        'group_sizes': group_sizes,
    }
