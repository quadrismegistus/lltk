"""
ClickHouse passage embeddings — build, retrieve, and search via multilingual encoders.

Public functions (called via MetaDBCH):
    build_embeddings_ch(adapter, ...)      — encode passages -> lltk.passage_embeddings
    get_text_embedding_ch(adapter, _id)    — mean-pooled text vector
    similar_texts_ch(adapter, _id, ...)    — text-level cosine similarity
    search_embeddings_ch(adapter, query)   — semantic passage search (server-side)
"""

from __future__ import annotations

import os
import time
from typing import Optional

from logmap import logmap

from lltk.db.adapter import ch_quote

_ENCODER_CACHE: dict = {}

DEFAULT_MODEL = 'intfloat/multilingual-e5-large'


def _model_short(model_name: str) -> str:
    return model_name.split('/')[-1]


def _load_encoder(model_name: str = DEFAULT_MODEL, device: str = 'auto'):
    cache_key = (model_name, device)
    if cache_key in _ENCODER_CACHE:
        return _ENCODER_CACHE[cache_key]

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as e:
        raise ImportError(
            "sentence-transformers is required for embeddings; install with: "
            "pip install 'sentence-transformers>=2.2' 'torch>=2.0'"
        ) from e

    if device == 'auto':
        import torch
        if torch.cuda.is_available():
            device = 'cuda'
        elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
            device = 'mps'
        else:
            device = 'cpu'

    encoder = SentenceTransformer(model_name, device=device)
    _ENCODER_CACHE[cache_key] = encoder
    return encoder


def _encode_batch(texts: list[str], encoder, prefix: str = 'passage: ',
                  batch_size: int = 64) -> list[list[float]]:
    prefixed = [prefix + t for t in texts]
    embeddings = encoder.encode(
        prefixed,
        batch_size=batch_size,
        show_progress_bar=False,
        normalize_embeddings=True,
    )
    return embeddings.tolist()


def _escape(s: str) -> str:
    return ch_quote(s)


# -- Build -----------------------------------------------------------------

def build_embeddings_ch(adapter, model_name: str = DEFAULT_MODEL,
                        device: str = 'auto', batch_size: int = 200,
                        encode_batch_size: int = 64, corpora=None,
                        force: bool = False, scheme: str = 'p500') -> int:
    """Encode passages from lltk.passages -> lltk.passage_embeddings.

    Prerequisite: lltk.passages must be populated (run db-passages first).
    Returns the number of passages embedded.
    """
    from lltk.tools.tools import get_tqdm

    model_short = _model_short(model_name)

    with logmap('Building passage embeddings...') as log:

        if force:
            adapter.execute('TRUNCATE TABLE lltk.passage_embeddings')
            adapter.execute('TRUNCATE TABLE lltk.passage_embeddings_meta')

        # Idempotency: skip already-embedded texts
        done_df = adapter.query_df(
            f"SELECT DISTINCT _id FROM lltk.passage_embeddings_meta FINAL "
            f"WHERE model = '{_escape(model_short)}' AND scheme = '{_escape(scheme)}'"
        )
        done_ids = set(done_df['_id'].tolist()) if len(done_df) else set()

        if done_ids:
            # Expand via match groups (same pattern as passages)
            group_df = adapter.query_df("""
                SELECT DISTINCT _id
                FROM (SELECT * FROM lltk.match_groups FINAL) mg
                WHERE mg.group_id IN (
                    SELECT group_id FROM lltk.match_groups FINAL
                    WHERE _id IN (SELECT _id FROM lltk.passage_embeddings_meta FINAL)
                )
            """)
            if len(group_df):
                group_ids = set(group_df['_id'].tolist())
                n_skipped = len(group_ids - done_ids)
                done_ids |= group_ids
            else:
                n_skipped = 0
            log.debug(f'{len(done_df)} texts already embedded'
                      f' (+{n_skipped} match-group siblings skipped)')

        # Find passages to embed
        where_parts = [f"scheme = '{_escape(scheme)}'"]
        if corpora:
            corpus_sql = ', '.join(f"'{_escape(c)}'" for c in corpora)
            where_parts.append(
                f"_id IN (SELECT _id FROM lltk.passages WHERE corpus IN ({corpus_sql}))"
            )

        where_sql = ' AND '.join(where_parts)
        text_ids_df = adapter.query_df(
            f"SELECT DISTINCT _id FROM lltk.passages WHERE {where_sql} ORDER BY _id"
        )
        if not len(text_ids_df):
            log.debug('No passages found. Run db-passages first.')
            return 0

        all_text_ids = [i for i in text_ids_df['_id'].tolist() if i not in done_ids]
        if not all_text_ids:
            log.debug('All texts already embedded.')
            return 0

        log.debug(f'{len(all_text_ids)} texts to embed '
                  f'(model={model_short}, device={device})')

        # Load encoder
        encoder = _load_encoder(model_name, device)

        total_embedded = 0
        t0 = time.time()

        for i in get_tqdm(range(0, len(all_text_ids), batch_size),
                          desc='embedding batches'):
            batch_ids = all_text_ids[i:i + batch_size]
            ids_sql = ', '.join(f"'{_escape(tid)}'" for tid in batch_ids)

            # Fetch passages for this batch
            psg_df = adapter.query_df(
                f"SELECT _id, seq, text FROM lltk.passages "
                f"WHERE _id IN ({ids_sql}) AND scheme = '{_escape(scheme)}' "
                f"ORDER BY _id, seq"
            )
            if not len(psg_df):
                continue

            # Encode all passages in this batch
            texts = psg_df['text'].tolist()
            embeddings = _encode_batch(texts, encoder, prefix='passage: ',
                                       batch_size=encode_batch_size)

            # Build insert rows
            emb_rows = []
            meta_counts: dict[str, int] = {}
            for idx, (_, row) in enumerate(psg_df.iterrows()):
                _id = row['_id']
                emb_rows.append([_id, scheme, int(row['seq']), model_short,
                                 embeddings[idx]])
                meta_counts[_id] = meta_counts.get(_id, 0) + 1

            # Insert embeddings
            adapter.client.insert(
                'lltk.passage_embeddings',
                emb_rows,
                column_names=['_id', 'scheme', 'seq', 'model', 'embedding'],
                settings={'async_insert': 1},
            )

            # Insert meta
            meta_rows = [[_id, scheme, model_short, n]
                         for _id, n in meta_counts.items()]
            adapter.client.insert(
                'lltk.passage_embeddings_meta',
                meta_rows,
                column_names=['_id', 'scheme', 'model', 'n_passages'],
                settings={'async_insert': 1},
            )

            total_embedded += len(emb_rows)

        elapsed = time.time() - t0
        log.debug(f'{total_embedded:,} passages from '
                  f'{len(all_text_ids)} texts in {elapsed:.0f}s')
        return total_embedded


# -- Retrieval -------------------------------------------------------------

def get_text_embedding_ch(adapter, _id: str,
                          model: str = 'multilingual-e5-large',
                          scheme: str = 'p500'):
    """Return mean-pooled, re-normalized embedding for a text (numpy array)."""
    import numpy as np

    df = adapter.query_df(
        f"SELECT embedding FROM lltk.passage_embeddings "
        f"WHERE _id = '{_escape(_id)}' AND scheme = '{_escape(scheme)}' "
        f"AND model = '{_escape(model)}'"
    )
    if not len(df):
        return None

    vectors = np.array(df['embedding'].tolist(), dtype=np.float32)
    mean = vectors.mean(axis=0)
    norm = np.linalg.norm(mean)
    if norm > 0:
        mean /= norm
    return mean


def similar_texts_ch(adapter, _id: str,
                     model: str = 'multilingual-e5-large',
                     scheme: str = 'p500', limit: int = 20,
                     corpora: Optional[list[str]] = None,
                     lang: Optional[str] = None) -> list[dict]:
    """Find texts most similar to _id by mean-pooled passage cosine similarity."""
    import numpy as np

    query_vec = get_text_embedding_ch(adapter, _id, model=model, scheme=scheme)
    if query_vec is None:
        return []

    # Build filter for candidate texts
    where_parts = [
        f"scheme = '{_escape(scheme)}'",
        f"model = '{_escape(model)}'",
        f"_id != '{_escape(_id)}'",
    ]
    if corpora:
        corpus_sql = ', '.join(f"'{_escape(c)}'" for c in corpora)
        where_parts.append(
            f"_id IN (SELECT _id FROM lltk.texts FINAL "
            f"WHERE corpus IN ({corpus_sql}))"
        )
    if lang:
        where_parts.append(
            f"_id IN (SELECT _id FROM lltk.texts FINAL "
            f"WHERE lang = '{_escape(lang)}')"
        )
    where_sql = ' AND '.join(where_parts)

    df = adapter.query_df(
        f"SELECT _id, groupArray(embedding) AS embeddings "
        f"FROM lltk.passage_embeddings "
        f"WHERE {where_sql} "
        f"GROUP BY _id"
    )
    if not len(df):
        return []

    # Mean-pool + normalize each text, compute dot product
    ids = df['_id'].tolist()
    means = []
    for embs in df['embeddings']:
        arr = np.array(embs, dtype=np.float32)
        m = arr.mean(axis=0)
        n = np.linalg.norm(m)
        means.append(m / n if n > 0 else m)

    matrix = np.stack(means)
    sims = matrix @ query_vec

    top_idx = np.argsort(-sims)[:limit]

    # Enrich with metadata
    result_ids = [ids[i] for i in top_idx]
    ids_sql = ', '.join(f"'{_escape(i)}'" for i in result_ids)
    meta_df = adapter.query_df(
        f"SELECT _id, title, author, year, corpus, genre, lang "
        f"FROM lltk.texts FINAL WHERE _id IN ({ids_sql})"
    )
    meta_map = {r['_id']: r.to_dict() for _, r in meta_df.iterrows()}

    results = []
    for i in top_idx:
        _tid = ids[i]
        m = meta_map.get(_tid, {})
        results.append({
            '_id': _tid,
            'similarity': float(sims[i]),
            'title': m.get('title', ''),
            'author': m.get('author', ''),
            'year': m.get('year'),
            'corpus': m.get('corpus', ''),
            'genre': m.get('genre', ''),
            'lang': m.get('lang', ''),
        })
    return results


def match_by_embeddings_ch(adapter, model: str = 'multilingual-e5-large',
                           scheme: str = 'p500', threshold: float = 0.998,
                           same_corpus: bool = False,
                           batch_size: int = 500,
                           progress: bool = True) -> int:
    """Find duplicate/translation candidates via mean-pooled cosine similarity.

    Inserts cross-corpus pairs above `threshold` into lltk.matches as
    match_type='embedding_similarity', then recomputes match_groups.
    Returns number of new match pairs inserted.
    """
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from lltk.tools.tools import get_tqdm

    with logmap('Matching by embeddings...') as log:
        with logmap(f'Loading embeddings (model={model}, scheme={scheme})...') as load_log:
            df = adapter.query_df(f"""
                SELECT _id, groupArray(embedding) as embeddings
                FROM lltk.passage_embeddings
                WHERE model = '{_escape(model)}' AND scheme = '{_escape(scheme)}'
                GROUP BY _id
            """)
        if not len(df):
            log.debug('No embeddings found.')
            return 0

        ids = df['_id'].tolist()
        n_texts = len(ids)
        log.debug(f'{n_texts:,} texts loaded. Mean-pooling...')

        vecs = []
        for embs in df['embeddings']:
            arr = np.array(embs, dtype=np.float32)
            m = arr.mean(axis=0)
            n = np.linalg.norm(m)
            vecs.append(m / n if n > 0 else m)
        matrix = np.stack(vecs)

        if not same_corpus:
            meta_df = adapter.query_df(
                "SELECT _id, corpus FROM lltk.texts FINAL "
                "WHERE _id IN (SELECT DISTINCT _id FROM lltk.passage_embeddings)"
            )
            id_to_corpus = dict(zip(meta_df['_id'], meta_df['corpus']))
        else:
            id_to_corpus = {}

        existing = set()
        exist_df = adapter.query_df(
            "SELECT _id_a, _id_b FROM lltk.matches FINAL "
            "WHERE match_type = 'embedding_similarity'"
        )
        if len(exist_df):
            for _, r in exist_df.iterrows():
                existing.add((r['_id_a'], r['_id_b']))

        with logmap(f'Computing pairwise similarities (threshold={threshold})...') as sim_log:
            pairs = []
            iterator = range(0, n_texts, batch_size)
            if progress:
                iterator = get_tqdm(iterator, desc='embedding batches')

            for i in iterator:
                end_i = min(i + batch_size, n_texts)
                sims = matrix[i:end_i] @ matrix.T

                for bi in range(end_i - i):
                    gi = i + bi
                    candidates = np.where(sims[bi, gi + 1:] >= threshold)[0] + gi + 1
                    for gj in candidates:
                        id_a, id_b = ids[gi], ids[gj]
                        if id_a > id_b:
                            id_a, id_b = id_b, id_a
                        if (id_a, id_b) in existing:
                            continue
                        if not same_corpus and id_to_corpus.get(id_a) == id_to_corpus.get(id_b):
                            continue
                        pairs.append((id_a, id_b, float(sims[bi, gj]),
                                      'embedding_similarity'))

        if not pairs:
            log.debug('No new pairs above threshold.')
            return 0

        with logmap(f'Inserting {len(pairs):,} pairs...') as ins_log:
            pdf = pd.DataFrame(pairs, columns=['_id_a', '_id_b', 'similarity', 'match_type'])
            pdf = pdf.drop_duplicates(subset=['_id_a', '_id_b'])
            tbl = pa.Table.from_pandas(pdf, preserve_index=False)
            adapter.client.insert_arrow('matches', tbl)

        with logmap('Recomputing match groups...') as grp_log:
            from lltk.db.match import _compute_match_groups_ch
            _compute_match_groups_ch(adapter)

        log.debug(f'Done — {len(pdf):,} new pairs inserted.')
        return len(pdf)


def search_embeddings_ch(adapter, query_text: str,
                         model_name: str = DEFAULT_MODEL,
                         scheme: str = 'p500', limit: int = 20,
                         device: str = 'auto',
                         corpora: Optional[list[str]] = None,
                         lang: Optional[str] = None,
                         snippet_words: int = 60) -> list[dict]:
    """Semantic search: encode query, find nearest passages server-side."""
    model_short = _model_short(model_name)

    encoder = _load_encoder(model_name, device)
    query_vec = encoder.encode(
        ['query: ' + query_text],
        normalize_embeddings=True,
    )[0].tolist()

    # Build the query vector as a CH array literal
    vec_literal = '[' + ','.join(f'{v:.8f}' for v in query_vec) + ']'

    where_parts = [
        f"pe.scheme = '{_escape(scheme)}'",
        f"pe.model = '{_escape(model_short)}'",
    ]
    if corpora:
        corpus_sql = ', '.join(f"'{_escape(c)}'" for c in corpora)
        where_parts.append(
            f"pe._id IN (SELECT _id FROM lltk.texts FINAL "
            f"WHERE corpus IN ({corpus_sql}))"
        )
    if lang:
        where_parts.append(
            f"pe._id IN (SELECT _id FROM lltk.texts FINAL "
            f"WHERE lang = '{_escape(lang)}')"
        )
    where_sql = ' AND '.join(where_parts)

    sql = f"""
        SELECT pe._id, pe.seq,
               arraySum(arrayMap((a, b) -> a * b, pe.embedding,
                        {vec_literal})) AS score
        FROM lltk.passage_embeddings AS pe
        WHERE {where_sql}
        ORDER BY score DESC
        LIMIT {int(limit)}
    """
    df = adapter.query_df(sql)
    if not len(df):
        return []

    # Fetch passage text + metadata
    result_ids = df['_id'].unique().tolist()
    ids_sql = ', '.join(f"'{_escape(i)}'" for i in result_ids)

    meta_df = adapter.query_df(
        f"SELECT _id, title, author, year, corpus, genre, lang "
        f"FROM lltk.texts FINAL WHERE _id IN ({ids_sql})"
    )
    meta_map = {r['_id']: r.to_dict() for _, r in meta_df.iterrows()}

    # Get passage texts for snippets
    seq_conds = ' OR '.join(
        f"(_id = '{_escape(r['_id'])}' AND seq = {int(r['seq'])})"
        for _, r in df.iterrows()
    )
    psg_df = adapter.query_df(
        f"SELECT _id, seq, text FROM lltk.passages "
        f"WHERE scheme = '{_escape(scheme)}' AND ({seq_conds})"
    )
    psg_map = {(r['_id'], int(r['seq'])): r['text'] for _, r in psg_df.iterrows()}

    results = []
    for _, r in df.iterrows():
        _id = r['_id']
        seq = int(r['seq'])
        m = meta_map.get(_id, {})
        text = psg_map.get((_id, seq), '')
        alpha_count = sum(c.isalpha() for c in text)
        if alpha_count < 20:
            continue
        words = text.split()
        snippet = ' '.join(words[:snippet_words])
        if len(words) > snippet_words:
            snippet += '...'
        results.append({
            '_id': _id,
            'seq': seq,
            'score': float(r['score']),
            'snippet': snippet,
            'title': m.get('title', ''),
            'author': m.get('author', ''),
            'year': m.get('year'),
            'corpus': m.get('corpus', ''),
            'genre': m.get('genre', ''),
            'lang': m.get('lang', ''),
        })
    return results
