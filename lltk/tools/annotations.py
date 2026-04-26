"""
Annotations SDK — cross-corpus canonical annotation store over ClickHouse.

Two tables:
  - lltk.annotations           append-only log of (id, field, value, source, ...)
  - lltk.annotation_sources    source → priority dim table

One VIEW:
  - lltk.annotations_latest    argMax-resolved winner per (_id, field)

Primary writers:
  - lltk itself                db-enrich-genres (later), curation UIs (later)
  - largeliterarymodels        LLM task outputs via write_task_to_lltk adapter
  - curation UIs               lltk annotate web app (later)

Readers: any code that wants the current best label for a text.

    from lltk.tools.annotations import write, resolve, register_source

    # One-time setup (auto-run on first import, idempotent):
    # seed_default_sources()

    # Write annotations (field_spec validation applied):
    write(
        source='llm:gemini-2.5-pro',
        rows=[
            {'_id': '_estc/T068056', 'field': 'genre', 'value': 'Fiction',
             'confidence': 0.95},
            {'_id': '_estc/T068056', 'field': 'year_estimated', 'value': '1689',
             'confidence': 0.8},
        ],
        run_id='gemini-pro:2026-04-19',  # optional — default `{source}:{today}`
    )

    # Read the current winner for a field across many ids
    df = resolve(ids=['_estc/T068056', '_estc/T012345'], fields=['genre'])
    # -> columns: _id, field, value, source, confidence, annotated_at
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from typing import Any, Iterable, Optional

import pandas as pd

from lltk.tools.vocabs import GENRE_VOCAB, LANG_ISO639_1, normalize_lang


# ── Field specs ─────────────────────────────────────────────────────────
#
# Each field spec declares:
#   type        'str' | 'int' | 'bool'  (how to encode → String on write,
#                                        decode ← String on read)
#   vocab       set of allowed values (for 'str'), or None
#   nullable    is empty value '' allowed? ('' = explicitly unknown,
#               distinct from "no one looked")
#   range       (lo, hi) inclusive for 'int', or None
#   normalize   optional callable(value) -> value (applied before validation)
#
# All values are stored as String in ClickHouse; type + encoding are
# convention-only. 'bool' is stored as '0' / '1'.

# Module-level registry of field specs — mutable via register_field_spec so
# client packages can add their own fields at import time.
_FIELD_SPECS: dict[str, dict[str, Any]] = {
    'genre': {
        'type': 'str',
        'vocab': GENRE_VOCAB,
        'nullable': False,
        'range': None,
        'normalize': None,
    },
    'genre_raw': {
        # Uncontrolled companion to genre — preserves the specific label
        # the source provided ("Epistolary fiction", "Novel of manners").
        'type': 'str',
        'vocab': None,
        'nullable': True,
        'range': None,
        'normalize': None,
    },
    'is_translated': {
        'type': 'bool',
        'vocab': None,
        'nullable': True,
        'range': None,
        'normalize': None,
    },
    'original_lang': {
        'type': 'str',
        'vocab': LANG_ISO639_1,
        'nullable': True,
        'range': None,
        # LLM may emit 'French' / 'fr' / 'fra'; normalize to ISO 639-1.
        # Unknown codes return None → value becomes '' (unknown, not invalid).
        'normalize': normalize_lang,
    },
    'year_estimated': {
        'type': 'int',
        'vocab': None,
        'nullable': True,
        # Matches the plausible range in lltk.texts.year after parsing.
        # Tighter than a full Int32 so OCR garbage / year-as-ID confusion
        # is caught.
        'range': (-500, 2100),
        'normalize': None,
    },
    'author_first_name': {
        'type': 'str',
        'vocab': None,
        'nullable': True,
        'range': None,
        'normalize': None,
    },
    'exclude': {
        # Any truthy value removes a text from a curated view. Treated
        # symmetrically with existing CuratedCorpus.exclude semantics.
        'type': 'bool',
        'vocab': None,
        'nullable': False,
        'range': None,
        'normalize': None,
    },
}


# ── Default source priorities ───────────────────────────────────────────
# Semantic: HIGHER priority wins in annotations_latest.
#
# Sources not in this registry (e.g. first-ever write from a new `llm:*`
# model) are auto-registered at DEFAULT_LLM_PRIORITY if they match the
# `llm:` prefix, else at DEFAULT_UNKNOWN_PRIORITY.

DEFAULT_SOURCES = {
    'human':                         (100, 'Manual human curation'),
    'bibliography:fiction_biblio':   (90, 'Fiction bibliographies (Mish, McBurney, Raven, etc.)'),
    'bibliography:end':              (90, 'Early Novels Database (Penn)'),
    'bibliography:ravengarside':     (90, 'Raven & Garside 1770-1799'),
    'authority_corpus':              (70, 'Generic authority-corpus label propagation'),
    'heuristic':                     (50, 'Rule-based (ESTC form, title keywords)'),
}
DEFAULT_LLM_PRIORITY = 10
DEFAULT_CORPUS_PRIORITY = 30        # corpus-native label (`corpus:<corpus_id>`)
DEFAULT_UNKNOWN_PRIORITY = 0


# ── Low-level helpers ───────────────────────────────────────────────────

_BOOL_TRUE = {'1', 'true', 't', 'yes', 'y', True, 1}
_BOOL_FALSE = {'0', 'false', 'f', 'no', 'n', False, 0, '', None}


def _encode_value(value, spec):
    """Convert a Python value to the String stored in lltk.annotations.

    Applies `normalize` hook first, then type-dispatches. Raises ValueError
    on invalid input. Returns '' for explicit-unknown on nullable fields.
    """
    # pd.isna handles None / float('nan') / pd.NA / pd.NaT; wrap in try
    # because it errors on unhashable arrays / lists — those are never valid
    # here so the fallback is fine.
    try:
        is_null = value is None or pd.isna(value)
    except (TypeError, ValueError):
        is_null = False
    if is_null:
        if spec.get('nullable', False):
            return ''
        raise ValueError(f'Null value for non-nullable field')

    if spec.get('normalize') is not None:
        value = spec['normalize'](value)
        if value is None:
            if spec.get('nullable', False):
                return ''
            raise ValueError(f'Normalize returned None for non-nullable field')

    t = spec['type']
    if t == 'bool':
        if value in _BOOL_TRUE:
            return '1'
        if value in _BOOL_FALSE:
            return '0'
        if isinstance(value, str) and value.lower() in _BOOL_TRUE:
            return '1'
        if isinstance(value, str) and value.lower() in _BOOL_FALSE:
            return '0'
        raise ValueError(f'Cannot coerce {value!r} to bool')

    if t == 'int':
        try:
            n = int(value)
        except (ValueError, TypeError):
            raise ValueError(f'Cannot coerce {value!r} to int')
        rng = spec.get('range')
        if rng is not None and not (rng[0] <= n <= rng[1]):
            raise ValueError(f'Value {n} outside allowed range {rng}')
        return str(n)

    # str
    s = str(value).strip()
    if not s and not spec.get('nullable', False):
        raise ValueError(f'Empty string for non-nullable field')
    vocab = spec.get('vocab')
    if vocab is not None and s and s not in vocab:
        raise ValueError(f'Value {s!r} not in controlled vocab')
    return s


def _decode_value(value, spec):
    """Convert stored String back to Python value per field type.

    Empty string → None (explicit unknown) when nullable.
    """
    if value == '' and spec.get('nullable', False):
        return None
    t = spec['type']
    if t == 'bool':
        return value == '1'
    if t == 'int':
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    return value


def _default_run_id(source):
    return f'{source}:{date.today().isoformat()}'


def _validate_id(_id):
    """Minimal id-shape check. Matches lltk.db.metadb_ch._validate_id
    semantics (starts with _{corpus}/). Avoids cross-importing to keep
    this module light."""
    if not isinstance(_id, str):
        raise TypeError(f'_id must be str, got {type(_id).__name__}')
    if not (_id.startswith('_') and '/' in _id):
        raise ValueError(f'Invalid _id {_id!r}: expected `_{{corpus}}/{{id}}`')


# ── Public API ──────────────────────────────────────────────────────────

def field_spec(field: str) -> Optional[dict]:
    """Return the field spec dict for `field`, or None if unregistered."""
    return _FIELD_SPECS.get(field)


def vocab(field: str) -> Optional[frozenset]:
    """Return the controlled vocab for `field`, or None if unbounded."""
    spec = _FIELD_SPECS.get(field)
    if spec is None:
        return None
    v = spec.get('vocab')
    return frozenset(v) if v is not None else None


def register_field_spec(field: str, spec: dict) -> None:
    """Register (or overwrite) a field spec. Idempotent per-import.

    A spec must have 'type' ∈ {'str', 'int', 'bool'}; other keys optional.
    """
    if not isinstance(spec, Mapping):
        raise TypeError(f'spec must be a mapping, got {type(spec).__name__}')
    if spec.get('type') not in ('str', 'int', 'bool'):
        raise ValueError(f"spec['type'] must be 'str'/'int'/'bool'")
    _FIELD_SPECS[field] = dict(spec)


def _db():
    """Return (adapter, database) for the singleton lltk.db connection."""
    import lltk
    return lltk.db.adapter, 'lltk'


def register_source(source: str, priority: int = None,
                    description: str = '') -> None:
    """Register a source with a priority. Idempotent (ReplacingMergeTree).

    If `priority` is None, picks DEFAULT_LLM_PRIORITY for `llm:*` sources
    else DEFAULT_UNKNOWN_PRIORITY. A later call with a new priority
    overwrites (winning row on FINAL reads).
    """
    if priority is None:
        if source in DEFAULT_SOURCES:
            priority, description = DEFAULT_SOURCES[source]
        elif source.startswith('llm:'):
            priority = DEFAULT_LLM_PRIORITY
        elif source.startswith('corpus:'):
            priority = DEFAULT_CORPUS_PRIORITY
        else:
            priority = DEFAULT_UNKNOWN_PRIORITY
    adapter, db = _db()
    adapter.client.insert(
        f'{db}.annotation_sources',
        [[source, int(priority), description, datetime.now()]],
        column_names=['source', 'priority', 'description', 'registered_at'],
    )


def seed_default_sources(overwrite: bool = False) -> list[str]:
    """Register all DEFAULT_SOURCES entries. Skips any that already have a
    registration unless `overwrite=True`."""
    adapter, db = _db()
    if not overwrite:
        existing = {
            r[0] for r in adapter.query(
                f"SELECT source FROM {db}.annotation_sources FINAL"
            )
        }
    else:
        existing = set()
    written = []
    for source, (priority, description) in DEFAULT_SOURCES.items():
        if source in existing:
            continue
        register_source(source, priority=priority, description=description)
        written.append(source)
    return written


def ensure_schema() -> None:
    """Create lltk.annotations + lltk.annotation_sources + the resolver view,
    and seed default source priorities. Idempotent."""
    from lltk.db.schema import (
        CLICKHOUSE_SCHEMA, ANNOTATIONS_LATEST_VIEW, ANNOTATIONS_BY_SOURCE_VIEW,
    )
    adapter, db = _db()
    adapter.execute(CLICKHOUSE_SCHEMA['annotations'].format(db=db))
    adapter.execute(CLICKHOUSE_SCHEMA['annotation_sources'].format(db=db))
    adapter.execute(ANNOTATIONS_LATEST_VIEW.format(db=db))
    adapter.execute(ANNOTATIONS_BY_SOURCE_VIEW.format(db=db))
    seed_default_sources(overwrite=False)


def write(source: str, rows: Iterable[Mapping],
          run_id: Optional[str] = None,
          validate: bool = True) -> int:
    """Append annotation rows. Returns number of rows written.

    Each row is a dict with at minimum `_id`, `field`, `value`. Optional:
    `confidence` (default 1.0), `meta` (dict or JSON string; default '{}').
    `source` and `run_id` come from the call, not per-row.

    With `validate=True` (default), each row's value is coerced/validated
    against its field_spec — unknown fields and vocab/range violations raise
    ValueError, aborting the whole batch before any insert.

    Unknown sources auto-register at DEFAULT_LLM_PRIORITY if the name starts
    with `llm:`, else DEFAULT_UNKNOWN_PRIORITY.
    """
    adapter, db = _db()
    run_id = run_id or _default_run_id(source)
    now = datetime.now()

    rows = list(rows)
    if not rows:
        return 0

    # Auto-register unknown source (so annotations_latest can resolve
    # priority on next read). Cheap — idempotent on the RMT.
    _ensure_source_registered(source)

    batch = []
    for r in rows:
        _id = r.get('_id')
        field = r.get('field')
        raw = r.get('value')
        if _id is None or field is None:
            raise ValueError(f'row missing _id or field: {r!r}')
        _validate_id(_id)

        if validate:
            spec = _FIELD_SPECS.get(field)
            if spec is None:
                raise ValueError(
                    f'unknown field {field!r}: register_field_spec() first'
                )
            value = _encode_value(raw, spec)
        else:
            value = '' if raw is None else str(raw)

        meta = r.get('meta', '{}')
        if isinstance(meta, Mapping):
            meta = json.dumps(meta, default=str)

        batch.append([
            _id, field, value, source, run_id, now,
            float(r.get('confidence', 1.0)), meta,
        ])

    adapter.client.insert(
        f'{db}.annotations',
        batch,
        column_names=[
            '_id', 'field', 'value', 'source', 'run_id',
            'annotated_at', 'confidence', 'meta',
        ],
    )
    return len(batch)


def _ensure_source_registered(source: str) -> None:
    """Register `source` iff not already present in annotation_sources."""
    adapter, db = _db()
    rows = adapter.query(
        f"SELECT 1 FROM {db}.annotation_sources FINAL "
        f"WHERE source = '{_sql_escape(source)}' LIMIT 1"
    )
    if not rows:
        register_source(source)


def _sql_escape(s):
    return s.replace("'", "''")


def resolve(ids: Optional[Iterable[str]] = None,
            fields: Optional[Iterable[str]] = None,
            decode: bool = True) -> pd.DataFrame:
    """Read current-winner annotations. Returns a DataFrame.

    Columns: _id, field, value, source, confidence, annotated_at.

    Filter by `ids` and/or `fields`. Both optional — omitting either
    pulls the whole resolved view (can be huge, use carefully).

    With `decode=True` (default), `value` is decoded to Python types per
    field_spec (int/bool/str, '' → None for nullable fields).
    """
    adapter, db = _db()
    wheres = []
    if ids is not None:
        ids = list(ids)
        if not ids:
            return pd.DataFrame(columns=[
                '_id', 'field', 'value', 'source', 'confidence', 'annotated_at',
            ])
        for i in ids:
            _validate_id(i)
        ids_sql = ', '.join(f"'{_sql_escape(i)}'" for i in ids)
        wheres.append(f"_id IN ({ids_sql})")
    if fields is not None:
        fields = list(fields)
        for f in fields:
            if not isinstance(f, str):
                raise TypeError(f'field names must be str, got {type(f)}')
        fields_sql = ', '.join(f"'{_sql_escape(f)}'" for f in fields)
        wheres.append(f"field IN ({fields_sql})")
    where_sql = ('WHERE ' + ' AND '.join(wheres)) if wheres else ''
    df = adapter.query_df(
        f"SELECT _id, field, value, source, confidence, annotated_at "
        f"FROM {db}.annotations_latest {where_sql}"
    )
    if decode and len(df):
        df = df.copy()
        df['value'] = [
            _decode_value(v, _FIELD_SPECS.get(f, {'type': 'str', 'nullable': True}))
            for v, f in zip(df['value'], df['field'])
        ]
    return df


def resolve_by_source(source: str,
                      ids: Optional[Iterable[str]] = None,
                      fields: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Return the latest annotation per (_id, field) from a single source.

    Use for idempotency checks ("which _ids has this source already annotated?")
    and for reading one source's values without priority shadowing.
    """
    adapter, db = _db()
    wheres = [f"source = '{_sql_escape(source)}'"]
    if ids is not None:
        ids = list(ids)
        if not ids:
            return pd.DataFrame(columns=['_id', 'field', 'value', 'confidence', 'annotated_at'])
        ids_sql = ', '.join(f"'{_sql_escape(i)}'" for i in ids)
        wheres.append(f"_id IN ({ids_sql})")
    if fields is not None:
        fields = list(fields)
        fields_sql = ', '.join(f"'{_sql_escape(f)}'" for f in fields)
        wheres.append(f"field IN ({fields_sql})")
    where_sql = ' AND '.join(wheres)
    return adapter.query_df(
        f"SELECT _id, field, value, confidence, annotated_at "
        f"FROM {db}.annotations_by_source WHERE {where_sql}"
    )


def disagreements(field: str, min_sources: int = 2) -> pd.DataFrame:
    """Return _ids where ≥ `min_sources` distinct sources disagree on
    `field`. Useful for prompt debugging + active-learning sample selection.

    Per-source dedup happens first (latest `annotated_at` wins within a
    source), so re-mirroring or multiple writes from the same source don't
    produce phantom same-source "disagreements".

    Columns: _id, n_sources, n_distinct_values, values (array of winner
    values, one per source).
    """
    adapter, db = _db()
    field_esc = _sql_escape(field)
    return adapter.query_df(f"""
        WITH latest_per_source AS (
            SELECT
                _id,
                source,
                argMax(value, annotated_at) AS value
            FROM {db}.annotations
            WHERE field = '{field_esc}'
            GROUP BY _id, source
        )
        SELECT
            _id,
            uniqExact(source) AS n_sources,
            uniqExact(value)  AS n_distinct_values,
            groupUniqArray(value) AS values
        FROM latest_per_source
        GROUP BY _id
        HAVING uniqExact(source) >= {int(min_sources)}
           AND uniqExact(value) > 1
        ORDER BY n_sources DESC, _id
    """)


# ── Mirror from lltk.texts ──────────────────────────────────────────────
# Reads genre + genre_enriched_source from lltk.texts FINAL and writes one
# annotation row per text, sourced by the provenance tag. Used to make
# existing enrichment state visible to the SDK until `db-enrich-genres`
# migrates to Path A (writing through lltk.annotations directly).

def mirror_genres_from_texts(run_id: str = 'mirror:genre',
                             genre_raw_run_id: str = 'mirror:genre_raw',
                             replace: bool = True,
                             batch_size: int = 50_000) -> int:
    """Populate `lltk.annotations` with genre + genre_raw rows from `lltk.texts`.

    genre source mapping:
      genre_enriched_source = 'bibliography:...'  → pass-through
      genre_enriched_source = '' / 'corpus'       → 'corpus:<corpus_id>'
      other                                        → pass-through

    genre_raw source: always 'corpus:<corpus_id>' (no enriched_source for raw).

    Rows with empty values are skipped. If `replace=True` (default), prior
    rows with the same run_ids are deleted first (idempotent).

    Returns total rows written (genre + genre_raw combined).
    """
    adapter, db = _db()

    if replace:
        for rid, field in [(run_id, 'genre'), (genre_raw_run_id, 'genre_raw')]:
            adapter.execute(
                f"ALTER TABLE {db}.annotations DELETE "
                f"WHERE run_id = '{_sql_escape(rid)}' AND field = '{field}' "
                f"SETTINGS mutations_sync=1"
            )

    now = datetime.now()
    rows_df = adapter.query_df(f"""
        SELECT _id, corpus, genre, genre_raw, genre_enriched_source
        FROM {db}.texts FINAL
        WHERE genre != '' OR genre_raw != ''
    """)
    if not len(rows_df):
        return 0

    def _source_of(ges, corpus):
        if ges and ges.startswith('bibliography:'):
            return ges
        if ges in ('', 'corpus'):
            return f'corpus:{corpus}'
        return ges

    genre_sources = [
        _source_of(ges, c)
        for ges, c in zip(rows_df['genre_enriched_source'], rows_df['corpus'])
    ]
    raw_sources = [f'corpus:{c}' for c in rows_df['corpus']]

    # Auto-register any new corpus:* sources
    unique_sources = set(genre_sources) | set(raw_sources)
    existing = {r[0] for r in adapter.query(f"SELECT source FROM {db}.annotation_sources FINAL")}
    for s in unique_sources - existing:
        register_source(s)

    cols = ['_id', 'field', 'value', 'source', 'run_id', 'annotated_at', 'confidence', 'meta']
    ids_list      = rows_df['_id'].tolist()
    genre_list    = rows_df['genre'].tolist()
    raw_list      = rows_df['genre_raw'].tolist()

    total = 0

    # genre pass
    genre_rows = [
        [ids_list[j], 'genre', genre_list[j], genre_sources[j], run_id, now, 1.0, '{}']
        for j in range(len(rows_df)) if genre_list[j]
    ]
    for i in range(0, len(genre_rows), batch_size):
        chunk = genre_rows[i:i + batch_size]
        adapter.client.insert(f'{db}.annotations', chunk, column_names=cols)
        total += len(chunk)

    # genre_raw pass
    raw_rows = [
        [ids_list[j], 'genre_raw', raw_list[j], raw_sources[j], genre_raw_run_id, now, 1.0, '{}']
        for j in range(len(rows_df)) if raw_list[j]
    ]
    for i in range(0, len(raw_rows), batch_size):
        chunk = raw_rows[i:i + batch_size]
        adapter.client.insert(f'{db}.annotations', chunk, column_names=cols)
        total += len(chunk)

    return total
