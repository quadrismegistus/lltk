"""
lltk.tools.genre_tags — normalize genre_raw strings into canonical tag lists.

Entry points:
    normalize_genre_raw(raw)  → List[str]   canonical tags for one raw string
    load_facets()             → dict         parsed facets.yml (cached)
    tag_facets(tag)           → List[str]   facets for a canonical tag

Importable without ClickHouse/DuckDB deps.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from typing import Dict, List, Optional

_FACETS_PATH = os.path.join(os.path.dirname(__file__), 'facets.yml')

# Separators that mark independent tag chunks within one raw string
_CHUNK_SEP = re.compile(r'\s*(?:;|\s\|\s)\s*')


@lru_cache(maxsize=1)
def load_facets() -> dict:
    """Return parsed facets.yml as {'tags': {...}, 'atoms': {...}}."""
    import yaml
    with open(_FACETS_PATH, encoding='utf-8') as f:
        return yaml.safe_load(f)


def _atoms() -> Dict[str, object]:
    """Case-folded atom lookup dict (built once, cached via load_facets lru_cache)."""
    raw = load_facets()['atoms']
    d: Dict[str, object] = {}
    for k, v in raw.items():
        key = k.lower().strip()
        if key in d and d[key] is not None and v is None:
            continue
        d[key] = v
    return d


def tag_facets(tag: str) -> List[str]:
    """Return facet list for a canonical tag, or [] if unknown."""
    tags = load_facets()['tags']
    entry = tags.get(tag, {})
    return list(entry.get('facets', []))


def _resolve_atom(atom: str, atoms: Dict[str, object]) -> List[str]:
    """Look up one lowercased atom; return list of canonical tags (empty = skip)."""
    atom = atom.lower().strip()
    if not atom:
        return []
    val = atoms.get(atom)
    if val is None and atom not in atoms:
        # Unknown atom — pass through as-is if it looks like a real single tag
        # (at least 3 chars, no digits, no comma — commas are form/mode separators).
        # Keeps novel LLM terms visible without swallowing unparsed compounds.
        if len(atom) >= 3 and not re.search(r'\d', atom) and ',' not in atom:
            return [atom]
        return []
    if val is None:
        return []           # explicit null → skip
    if isinstance(val, list):
        return [str(t) for t in val]
    return [str(val)]


def normalize_genre_raw(raw: str) -> List[str]:
    """Normalize one genre_raw string into a deduplicated list of canonical tags.

    Algorithm:
      1. Split on ';' or ' | ' → independent chunks
      2. For each chunk: exact lookup in atoms dict first
      3. If no exact match: split on first ',' → look up each part
      4. Deduplicate preserving order; drop empty results
    """
    if not raw or not raw.strip():
        return []

    atoms = _atoms()
    seen: dict = {}  # ordered dedup

    for chunk in _CHUNK_SEP.split(raw.strip()):
        chunk = chunk.strip()
        if not chunk:
            continue

        # 1. Exact match (handles multi-word critical terms and explicit overrides)
        exact = _resolve_atom(chunk, atoms)
        if exact or chunk.lower().strip() in atoms:
            for t in exact:
                seen[t] = None
            continue

        # 2. Comma-split: "Form, mode" → look up each part separately
        if ',' in chunk:
            parts = [p.strip() for p in chunk.split(',', 1)]
        else:
            parts = [chunk]

        for part in parts:
            for t in _resolve_atom(part, atoms):
                seen[t] = None

    return list(seen.keys())


# ── Materializer ──────────────────────────────────────────────────────────────

def is_recognized(author_first_name, year_estimated, gt_author, gt_year,
                   gt_title, gt_meta_str='{}') -> bool:
    """Check whether an LLM source 'recognized' a text.

    Recognition = author match OR year match, UNLESS year appears in the
    title (making year trivially readable from the prompt), in which case
    only author match counts.
    """
    # Author match: LLM name (lowered, len>1) is substring of ground truth
    author_match = False
    if (author_first_name and isinstance(author_first_name, str)
            and len(author_first_name.strip()) > 1
            and gt_author and isinstance(gt_author, str)):
        author_match = author_first_name.strip().lower() in gt_author.lower()

    # Year match: exact int equality
    year_match = False
    if year_estimated is not None and gt_year is not None:
        try:
            year_match = int(year_estimated) == int(gt_year)
        except (ValueError, TypeError):
            pass

    if not year_match and not author_match:
        return False
    if not year_match:
        return True  # author_match is True

    # Year matched — check if it's trivially in the title
    year_str = str(int(year_estimated)) if year_estimated is not None else ''
    if year_str:
        title_str = str(gt_title or '').lower()
        import json
        try:
            meta = json.loads(gt_meta_str) if gt_meta_str else {}
        except (json.JSONDecodeError, TypeError):
            meta = {}
        estc_title = str(meta.get('estc_title', '')).lower()
        estc_title_sub = str(meta.get('estc_title_sub', '')).lower()
        year_in_title = (year_str in title_str or year_str in estc_title
                         or year_str in estc_title_sub)
        if year_in_title:
            return author_match
    return True


def build_genre_tags(ch_adapter, batch_size: int = 50_000,
                     progress: bool = True, recognized_only: bool = True,
                     min_sources: int = 1) -> int:
    """Materialize lltk.text_genre_tags from annotations genre_raw.

    Reads every genre_raw annotation (one per _id per source),
    runs normalize_genre_raw(), expands to (_id, tag, facet) triples,
    then TRUNCATE + batch INSERT into text_genre_tags.

    With recognized_only=True, LLM sources are filtered: only count a
    source's tags if it 'recognized' the text (author_first_name substring
    match OR year_estimated exact match, with year-in-title gating).
    Non-LLM sources (corpus:*, human, heuristic) always pass.

    With min_sources > 1, a tag is only included if N+ distinct sources
    independently produced it for that _id.

    Returns the number of rows written.
    """
    from datetime import datetime

    if recognized_only:
        rows_df = ch_adapter.query_df("""
            SELECT
                g._id AS _id,
                g.source AS source,
                g.genre_raw AS genre_raw,
                a.author_first_name AS author_first_name,
                y.year_estimated AS year_estimated,
                t.author AS gt_author,
                t.year AS gt_year,
                t.title AS gt_title,
                t.meta AS gt_meta
            FROM (
                SELECT _id, source, argMax(value, annotated_at) AS genre_raw
                FROM lltk.annotations
                WHERE field = 'genre_raw' AND value != ''
                GROUP BY _id, source
            ) g
            LEFT JOIN (
                SELECT _id, source, argMax(value, annotated_at) AS author_first_name
                FROM lltk.annotations
                WHERE field = 'author_first_name'
                GROUP BY _id, source
            ) a ON g._id = a._id AND g.source = a.source
            LEFT JOIN (
                SELECT _id, source, argMax(value, annotated_at) AS year_estimated
                FROM lltk.annotations
                WHERE field = 'year_estimated'
                GROUP BY _id, source
            ) y ON g._id = y._id AND g.source = y.source
            LEFT JOIN (
                SELECT _id, author, year, title, meta
                FROM lltk.texts FINAL
            ) t ON g._id = t._id
        """)
    else:
        rows_df = ch_adapter.query_df("""
            SELECT _id, source, argMax(value, annotated_at) AS genre_raw
            FROM lltk.annotations
            WHERE field = 'genre_raw' AND value != ''
            GROUP BY _id, source
        """)

    if not len(rows_df):
        if progress:
            print('[db-tag-genres] No genre_raw annotations found.')
        return 0

    if progress:
        print(f'[db-tag-genres] {len(rows_df):,} genre_raw rows from '
              f'{rows_df["source"].nunique()} sources.')

    # Recognition gating with tiered fallback:
    # Tier 1: recognized LLM sources + all non-LLM sources
    # Tier 2 (fallback): unrecognized LLM sources, only for _ids with
    #         zero usable tags from tier 1
    if recognized_only:
        recognized_mask = []
        for _, row in rows_df.iterrows():
            source = str(row['source'])
            if not source.startswith('llm:'):
                recognized_mask.append(True)
                continue
            recognized_mask.append(is_recognized(
                row.get('author_first_name'),
                row.get('year_estimated'),
                row.get('gt_author'),
                row.get('gt_year'),
                row.get('gt_title'),
                row.get('gt_meta', '{}'),
            ))
        rows_df['_recognized'] = recognized_mask
        n_recog = sum(recognized_mask)
        n_unrecog = len(rows_df) - n_recog
        if progress:
            print(f'[db-tag-genres] Recognition: {n_recog:,} recognized, '
                  f'{n_unrecog:,} unrecognized LLM rows.')
    else:
        rows_df['_recognized'] = True

    if progress:
        print(f'[db-tag-genres] Normalizing...')

    # Expand to (_id, tag, source) triples, tracking recognition status
    now = datetime.now()
    # recognized tier: (id, tag) -> set of sources
    tag_sources_recog: dict = {}
    # fallback tier: (id, tag) -> set of sources (unrecognized LLM only)
    tag_sources_fallback: dict = {}

    for _id, source, raw, recog in zip(
            rows_df['_id'], rows_df['source'],
            rows_df['genre_raw'], rows_df['_recognized']):
        for tag in normalize_genre_raw(raw):
            key = (_id, tag)
            if recog:
                if key not in tag_sources_recog:
                    tag_sources_recog[key] = set()
                tag_sources_recog[key].add(source)
            else:
                if key not in tag_sources_fallback:
                    tag_sources_fallback[key] = set()
                tag_sources_fallback[key].add(source)

    # Apply min_sources filter to recognized tier
    if min_sources > 1:
        n_before = len(tag_sources_recog)
        tag_sources_recog = {k: v for k, v in tag_sources_recog.items()
                             if len(v) >= min_sources}
        if progress:
            print(f'[db-tag-genres] min_sources={min_sources} (recognized): '
                  f'{n_before - len(tag_sources_recog):,} tags dropped.')

    # Determine which _ids got usable tags from recognized sources
    ids_with_recog_tags = {k[0] for k in tag_sources_recog}

    # Fallback: for _ids with zero recognized tags, use unrecognized LLM tags
    if recognized_only:
        fallback_tags = {k: v for k, v in tag_sources_fallback.items()
                         if k[0] not in ids_with_recog_tags}
        if min_sources > 1:
            fallback_tags = {k: v for k, v in fallback_tags.items()
                             if len(v) >= min_sources}
        n_fallback_ids = len({k[0] for k in fallback_tags})
        if progress and fallback_tags:
            print(f'[db-tag-genres] Fallback: {n_fallback_ids:,} texts '
                  f'({len(fallback_tags):,} tags) from unrecognized sources.')
    else:
        fallback_tags = {}

    # Expand to (_id, tag, facet, recognized) triples
    triples_seen: dict = {}
    for (_id, tag) in tag_sources_recog:
        facets = tag_facets(tag) or ['unknown']
        for facet in facets:
            triples_seen[(_id, tag, facet)] = (_id, tag, facet, 1, now)
    for (_id, tag) in fallback_tags:
        facets = tag_facets(tag) or ['unknown']
        for facet in facets:
            if (_id, tag, facet) not in triples_seen:
                triples_seen[(_id, tag, facet)] = (_id, tag, facet, 0, now)
    triples = list(triples_seen.values())

    if not triples:
        if progress:
            print('[db-tag-genres] Normalization produced no tags.')
        return 0

    n_recog_triples = sum(1 for t in triples if t[3] == 1)
    n_fallback_triples = len(triples) - n_recog_triples
    if progress:
        print(f'[db-tag-genres] {len(triples):,} unique triples '
              f'({n_recog_triples:,} recognized, {n_fallback_triples:,} fallback).')
        print('[db-tag-genres] Truncating text_genre_tags...')

    # Ensure recognized column exists (for existing tables without it)
    try:
        ch_adapter.execute(
            "ALTER TABLE lltk.text_genre_tags ADD COLUMN IF NOT EXISTS "
            "recognized UInt8 DEFAULT 1"
        )
    except Exception:
        pass

    ch_adapter.execute('TRUNCATE TABLE lltk.text_genre_tags')

    cols = ['_id', 'tag', 'facet', 'recognized', 'tagged_at']
    total = 0
    for i in range(0, len(triples), batch_size):
        chunk = [list(t) for t in triples[i:i + batch_size]]
        ch_adapter.client.insert('lltk.text_genre_tags', chunk, column_names=cols)
        total += len(chunk)
        if progress:
            print(f'[db-tag-genres]   {total:,} / {len(triples):,}', end='\r')

    if progress:
        print(f'\n[db-tag-genres] Done — {total:,} rows written.')
    return total
