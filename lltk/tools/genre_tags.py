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
    return {k.lower().strip(): v for k, v in raw.items()}


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

def build_genre_tags(ch_adapter, batch_size: int = 50_000,
                     progress: bool = True) -> int:
    """Materialize lltk.text_genre_tags from annotations_latest.genre_raw.

    Reads every genre_raw annotation (winning value per _id per source),
    runs normalize_genre_raw(), expands to (_id, tag, facet) triples,
    then TRUNCATE + batch INSERT into text_genre_tags.

    Returns the number of rows written.
    """
    from datetime import datetime

    # Pull all winning genre_raw values — one row per (_id, source) from
    # annotations_latest so each source contributes its tags independently.
    # We use the raw annotations table with argMax rather than annotations_latest
    # (which dedups across sources) so every source's genre_raw feeds tags.
    rows_df = ch_adapter.query_df("""
        SELECT _id, argMax(value, annotated_at) AS genre_raw
        FROM lltk.annotations
        WHERE field = 'genre_raw' AND value != ''
        GROUP BY _id, source
    """)

    if not len(rows_df):
        if progress:
            print('[db-tag-genres] No genre_raw annotations found.')
        return 0

    if progress:
        print(f'[db-tag-genres] Normalizing {len(rows_df):,} genre_raw rows...')

    # Expand to (_id, tag, facet) triples
    now = datetime.now()
    triples = []
    for _id, raw in zip(rows_df['_id'], rows_df['genre_raw']):
        for tag in normalize_genre_raw(raw):
            facets = tag_facets(tag) or ['unknown']
            for facet in facets:
                triples.append((_id, tag, facet, now))

    if not triples:
        if progress:
            print('[db-tag-genres] Normalization produced no tags.')
        return 0

    # Deduplicate (_id, tag, facet) — multiple sources may produce same triple
    seen = {}
    for t in triples:
        key = (t[0], t[1], t[2])
        seen[key] = t
    triples = list(seen.values())

    if progress:
        print(f'[db-tag-genres] {len(triples):,} unique (_id, tag, facet) triples.')
        print('[db-tag-genres] Truncating text_genre_tags...')

    ch_adapter.execute('TRUNCATE TABLE lltk.text_genre_tags')

    cols = ['_id', 'tag', 'facet', 'tagged_at']
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
