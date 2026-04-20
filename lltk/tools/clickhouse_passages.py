"""
ClickHouse passages store — build, retrieve, and search ~500-word text chunks.

Replaces the old SQLite + FTS5 metadb_passages.sqlite backend.

Public functions (called via MetaDBCH):
    build_passages_ch(adapter, ...)   — ingest corpus txt → lltk.passages
    get_passages_ch(adapter, ids, scheme)  — bulk retrieval for llm/abstraction
    search_passages_ch(adapter, query, ...)  — keyword search with CH full_text index
    search_passages_count_ch(adapter, query)  — count matches
"""

from __future__ import annotations

import os
import re
import time

_NEAR_RE = re.compile(r'^NEAR\s*\(([^,)]+)(?:,\s*\d+)?\)\s*$', re.IGNORECASE)
_PHRASE_RE = re.compile(r'^"([^"]+)"$')


# ── Query helpers ──────────────────────────────────────────────────────────────

def _escape(s: str) -> str:
    """Escape a string for single-quoted SQL literals (doubles apostrophes)."""
    return s.replace("'", "''")


def _query_to_ch_condition(query: str) -> str:
    """Convert a search query string to a CH WHERE fragment.

    Supports:
      - "phrase search"  → positionCaseInsensitive
      - NEAR(a b, N)     → AND on each token (position not enforced)
      - word1 word2      → AND on each token via hasTokenCaseInsensitive
      - word             → hasTokenCaseInsensitive (uses full_text index)

    All values are escaped via _escape().
    """
    query = query.strip()

    # Phrase: "foo bar"
    m = _PHRASE_RE.match(query)
    if m:
        phrase = _escape(m.group(1))
        return f"positionCaseInsensitive(text, '{phrase}') > 0"

    # NEAR(a b c, N) → AND on tokens
    m = _NEAR_RE.match(query)
    if m:
        terms = [t.strip() for t in m.group(1).split() if t.strip()]
        if terms:
            return ' AND '.join(
                f"hasTokenCaseInsensitive(text, '{_escape(t)}')" for t in terms
            )

    # Multi-word without quotes → AND on each word
    terms = query.split()
    if len(terms) > 1:
        return ' AND '.join(
            f"hasTokenCaseInsensitive(text, '{_escape(t)}')" for t in terms
        )

    # Single token
    return f"hasTokenCaseInsensitive(text, '{_escape(query)}')"


def _extract_snippet(text: str, query: str, context_words: int = 30) -> str:
    """Return a ~context_words excerpt around the first query term in text."""
    query = query.strip()

    m = _PHRASE_RE.match(query)
    if m:
        search_terms = [m.group(1).lower()]
    else:
        nm = _NEAR_RE.match(query)
        if nm:
            search_terms = [t.lower() for t in nm.group(1).split() if t.strip()]
        else:
            search_terms = [t.lower() for t in query.split()]

    words = text.split()
    half = context_words // 2

    for i, w in enumerate(words):
        wl = w.lower().rstrip('.,;:!?"\')-')
        if any(t in wl for t in search_terms):
            start = max(0, i - half)
            end = min(len(words), i + half)
            snippet = ' '.join(words[start:end])
            if start > 0:
                snippet = '...' + snippet
            if end < len(words):
                snippet += '...'
            return snippet

    # No match — return beginning of text
    snippet = ' '.join(words[:context_words])
    if len(words) > context_words:
        snippet += '...'
    return snippet


# ── Build ──────────────────────────────────────────────────────────────────────

def build_passages_ch(adapter, n: int = 500, num_proc: int | None = None,
                      corpora=None, force: bool = False):
    """Chunk corpus txt files into ~n-word passages and insert into lltk.passages.

    Idempotent: skips texts already in lltk.passages_meta (unless force=True).
    """
    from lltk.tools.metadb import _chunk_text_to_passages, DB_BLACKLIST
    from lltk.tools.tools import pmap, get_tqdm
    from lltk.tools.tools import normalize_lang

    try:
        from lltk.imports import log
    except Exception:
        log = None

    if num_proc is None:
        num_proc = max(1, os.cpu_count() - 2)

    if force:
        adapter.execute('TRUNCATE TABLE lltk.passages')
        adapter.execute('TRUNCATE TABLE lltk.passages_meta')

    # Already-processed _ids (skip on incremental runs)
    done_df = adapter.query_df(
        "SELECT DISTINCT _id FROM lltk.passages_meta FINAL"
    )
    done_ids = set(done_df['_id'].tolist()) if len(done_df) else set()
    if done_ids and log:
        log(f'[passages] {len(done_ids)} texts already in passages table')

    # Gather tasks
    from lltk.corpus.utils import load as load_corpus
    from lltk.corpus.synthetic import SyntheticCorpus

    if corpora is None:
        from lltk.corpus.utils import get_inducted_corpus_ids
        corpora = get_inducted_corpus_ids()

    all_tasks = []
    for cid in corpora:
        if cid in DB_BLACKLIST:
            continue
        try:
            corpus = load_corpus(cid)
        except Exception:
            continue
        if isinstance(corpus, SyntheticCorpus):
            continue
        if not hasattr(corpus, 'path_txt') or not os.path.isdir(
                getattr(corpus, 'path_txt', '')):
            continue
        manifest_lang = getattr(corpus, 'lang', None)
        try:
            meta = corpus.load_metadata()
        except Exception:
            continue
        if meta is None or not len(meta):
            continue

        for text_id in meta.index:
            _id = f'_{cid}/{text_id}'
            if _id in done_ids:
                continue
            t = corpus.text(text_id)
            txt_path = getattr(t, 'path_txt', None)
            if not txt_path or not os.path.exists(txt_path):
                continue
            lang = None
            if t._meta:
                for key in ('lang', 'language', 'language_1', 'estc_lang'):
                    val = t._meta.get(key)
                    if val and str(val).strip() and str(val) != 'nan':
                        lang = normalize_lang(str(val).strip())
                        if lang:
                            break
            if not lang and manifest_lang:
                lang = normalize_lang(manifest_lang)
            all_tasks.append((_id, cid, txt_path, lang, n))

    if not all_tasks:
        if log:
            log('[passages] No new texts to process.')
        return 0

    if log:
        log(f'[passages] {len(all_tasks)} texts to chunk (n={n}, workers={num_proc})')

    SCHEME = 'p500'
    batch_size = 200
    total_passages = 0
    t0 = time.time()

    for i in get_tqdm(range(0, len(all_tasks), batch_size),
                      desc='[passages] batches'):
        batch = all_tasks[i:i + batch_size]
        results = pmap(
            _chunk_text_to_passages,
            batch,
            num_proc=num_proc,
            desc=None,
            use_threads=True,
        )

        psg_rows = []
        meta_rows = []
        for _id, corpus_id, passages_list in results:
            if not passages_list:
                continue
            for _id_, seq, text, n_words, lang in passages_list:
                psg_rows.append([_id_, corpus_id, SCHEME, seq, text,
                                  int(n_words), lang or ''])
            meta_rows.append([_id, corpus_id, SCHEME, len(passages_list)])
            total_passages += len(passages_list)

        if psg_rows:
            adapter.client.insert(
                'lltk.passages',
                psg_rows,
                column_names=['_id', 'corpus', 'scheme', 'seq', 'text',
                               'n_words', 'lang'],
                settings={'async_insert': 1},
            )
        if meta_rows:
            adapter.client.insert(
                'lltk.passages_meta',
                meta_rows,
                column_names=['_id', 'corpus', 'scheme', 'n_passages'],
                settings={'async_insert': 1},
            )

    elapsed = time.time() - t0
    if log:
        log(f'[passages] {total_passages:,} passages from '
            f'{len(all_tasks)} texts in {elapsed:.0f}s')
    return total_passages


# ── Retrieval ──────────────────────────────────────────────────────────────────

def get_passages_ch(adapter, ids, scheme: str = 'p500'):
    """Return a DataFrame of passages for the given text _ids.

    For >10K ids uses a tmp Memory table JOIN to stay within max_query_size.
    Columns: _id, scheme, seq, text, n_words, lang.
    """
    import pandas as pd

    ids = list(ids)
    if not ids:
        return pd.DataFrame(columns=['_id', 'scheme', 'seq', 'text',
                                     'n_words', 'lang'])

    scheme_esc = _escape(scheme)

    if len(ids) > 10_000:
        adapter.execute(
            "CREATE TABLE IF NOT EXISTS tmp.passage_ids "
            "(_id String) ENGINE=Memory"
        )
        adapter.execute("TRUNCATE TABLE tmp.passage_ids")
        adapter.client.insert(
            'tmp.passage_ids', [[i] for i in ids], column_names=['_id']
        )
        return adapter.query_df(f"""
            SELECT p._id, p.scheme, p.seq, p.text, p.n_words, p.lang
            FROM lltk.passages p
            JOIN tmp.passage_ids t ON p._id = t._id
            WHERE p.scheme = '{scheme_esc}'
            ORDER BY p._id, p.seq
        """)

    ids_sql = ', '.join(f"'{_escape(i)}'" for i in ids)
    return adapter.query_df(f"""
        SELECT _id, scheme, seq, text, n_words, lang
        FROM lltk.passages
        WHERE _id IN ({ids_sql})
          AND scheme = '{scheme_esc}'
        ORDER BY _id, seq
    """)


# ── Search ─────────────────────────────────────────────────────────────────────

def search_passages_ch(adapter, query: str, genre=None, corpus=None,
                       lang=None, year_min=None, year_max=None,
                       limit: int = 20, offset: int = 0,
                       snippet_words: int = 30) -> list[dict]:
    """Keyword search over lltk.passages using the CH full_text index.

    Metadata filters (genre/corpus/lang/year) are resolved via lltk.texts FINAL
    before the passages scan. Results are sorted by (_id, seq) — no BM25 rank.
    Snippet extraction is done in Python.

    Returns list of dicts: _id, seq, snippet, n_words, title, author, year,
    corpus, genre, lang.
    """
    # 1. Metadata pre-filter
    meta_clauses = []
    if genre:
        meta_clauses.append(f"genre = '{_escape(genre)}'")
    if corpus:
        meta_clauses.append(f"corpus = '{_escape(corpus)}'")
    if lang:
        meta_clauses.append(f"lang = '{_escape(lang)}'")
    if year_min is not None:
        meta_clauses.append(f'year >= {int(year_min)}')
    if year_max is not None:
        meta_clauses.append(f'year <= {int(year_max)}')

    filtered_ids: set | None = None
    if meta_clauses:
        where_sql = ' AND '.join(meta_clauses)
        id_df = adapter.query_df(
            f"SELECT _id FROM lltk.texts FINAL WHERE {where_sql}"
        )
        filtered_ids = set(id_df['_id'].tolist())
        if not filtered_ids:
            return []

    # 2. Passages query
    text_cond = _query_to_ch_condition(query)
    total_needed = offset + limit

    if filtered_ids is not None:
        if len(filtered_ids) > 10_000:
            adapter.execute(
                "CREATE TABLE IF NOT EXISTS tmp.passage_ids "
                "(_id String) ENGINE=Memory"
            )
            adapter.execute("TRUNCATE TABLE tmp.passage_ids")
            adapter.client.insert(
                'tmp.passage_ids',
                [[i] for i in filtered_ids],
                column_names=['_id'],
            )
            sql = f"""
                SELECT p._id, p.seq, p.text, p.n_words, p.lang
                FROM lltk.passages p
                JOIN tmp.passage_ids f ON p._id = f._id
                WHERE p.scheme = 'p500'
                  AND {text_cond}
                ORDER BY p._id, p.seq
                LIMIT {total_needed}
            """
        else:
            ids_sql = ', '.join(f"'{_escape(i)}'" for i in filtered_ids)
            sql = f"""
                SELECT _id, seq, text, n_words, lang
                FROM lltk.passages
                WHERE _id IN ({ids_sql})
                  AND scheme = 'p500'
                  AND {text_cond}
                ORDER BY _id, seq
                LIMIT {total_needed}
            """
    else:
        sql = f"""
            SELECT _id, seq, text, n_words, lang
            FROM lltk.passages
            WHERE scheme = 'p500'
              AND {text_cond}
            ORDER BY _id, seq
            LIMIT {total_needed}
        """

    rows_df = adapter.query_df(sql)
    if not len(rows_df):
        return []

    rows_df = rows_df.iloc[offset:]

    # 3. Enrich with text metadata
    result_ids = rows_df['_id'].unique().tolist()
    ids_sql = ', '.join(f"'{_escape(i)}'" for i in result_ids)
    meta_df = adapter.query_df(f"""
        SELECT _id, title, author, year, corpus, genre, lang
        FROM lltk.texts FINAL
        WHERE _id IN ({ids_sql})
    """)
    meta_map = {r['_id']: r.to_dict() for _, r in meta_df.iterrows()}

    results = []
    for _, r in rows_df.iterrows():
        _id = r['_id']
        m = meta_map.get(_id, {})
        results.append({
            '_id': _id,
            'seq': int(r['seq']),
            'snippet': _extract_snippet(r['text'], query,
                                        context_words=snippet_words),
            'n_words': int(r['n_words']),
            'title': m.get('title', ''),
            'author': m.get('author', ''),
            'year': m.get('year'),
            'corpus': m.get('corpus', ''),
            'genre': m.get('genre', ''),
            'lang': r['lang'] or m.get('lang', ''),
        })
    return results


def search_passages_count_ch(adapter, query: str) -> int:
    """Count passages matching query (no metadata filter)."""
    text_cond = _query_to_ch_condition(query)
    result = adapter.query_df(f"""
        SELECT count() AS n FROM lltk.passages
        WHERE scheme = 'p500' AND {text_cond}
    """)
    return int(result['n'].iloc[0]) if len(result) else 0
