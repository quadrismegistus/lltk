"""
LLTK metadata database — ClickHouse backend.

This module was the original DuckDB-based MetaDB (3,500 lines). Now gutted
to a thin shim that provides the `metadb` singleton (MetaDBCH) and
re-exports constants for backwards compatibility.

Core constants live in lltk.tools.constants.
"""

import json
import os
import re

import numpy as np
import pandas as pd
from lltk.imports import PATH_LLTK_DATA, log

# Re-export constants for backwards compatibility
from lltk.tools.constants import (
    DB_BLACKLIST,
    CORPUS_SOURCE_RANKS,
    GENRE_AUTHORITY_CORPORA,
    GENRE_SOURCE_PRIORITY,
    CORE_COLS,
    STANDARD_COLS,
    normalize_title,
    normalize_author,
    _jaro_winkler,
    _chunk_text_to_passages,
)

from lltk.tools.vocabs import (
    GENRE_VOCAB,
    normalize_lang,
)

TEXT_COLS = [
    '_id', 'corpus', 'id', 'title', 'author', 'genre', 'genre_raw',
    'lang', 'title_norm', 'author_norm', 'path_freqs',
]


# ── Genre raw normalization ──────────────────────────────────────────

def normalize_genre_raw(val):
    """Normalize a genre_raw value: harmonize codes, synonyms, pipe-separated compounds."""
    if not val or not isinstance(val, str) or val in ('nan', 'None', ''):
        return None

    CODE_MAP = {
        'FIC': 'Fiction', 'fic': 'Fiction',
        'NEWS': 'News', 'MAG': 'Magazine',
        'ACAD': 'Academic', 'SPOK': 'Spoken',
        'bio': 'Biography',
        'Non-Fiction': 'Nonfiction', 'Non-fiction': 'Nonfiction',
    }
    stripped = val.strip()
    if stripped in CODE_MAP:
        return CODE_MAP[stripped]

    if '|' in stripped:
        parts = [p.strip() for p in re.split(r'\s*\|\s*', stripped) if p.strip()]
    else:
        parts = [stripped]

    EPISTOLARY = {'Epistolary fiction', 'Epistolary', 'Epistolary novel'}
    normalized = []
    for p in parts:
        if p in CODE_MAP:
            p = CODE_MAP[p]
        if p in EPISTOLARY:
            p = 'Novel, epistolary'
        if p and p[0].islower() and p not in CODE_MAP:
            p = p[0].upper() + p[1:]
        normalized.append(p)

    seen = set()
    deduped = []
    for p in normalized:
        if p not in seen:
            seen.add(p)
            deduped.append(p)

    FICTION_SUBGENRES = {
        'Novel', 'Novel, epistolary', 'Romance', 'Tale', 'Fable', 'Novella',
        'Picaresque', 'Gothic', 'Imaginary voyage', 'Silver Fork',
        'Bildungsroman', 'New Woman', 'Rogue fiction', 'Chapbook',
        'It Narrative', 'Utopia', 'Jestbook',
    }
    NOVEL_SUBTYPES = {
        'Novel, epistolary', 'Novel, sentimental', 'Novel, Gothic',
        'Novel, picaresque', 'Novel, satire', 'Novel, historical',
        'Novel, didactic', 'Novel, oriental', 'Novel, utopian',
        'Novel, utopia', 'Novel, erotic', 'Novel, philosophical',
        'Novel, anti-Jacobin', 'Novel, satirical',
        'Novel, It Narrative', 'Novel, scandalous memoir',
        'Novel, scandal chronicle', 'Novel, secret history',
        'Novel, miscellany', 'Novel, Romance',
    }
    if len(deduped) > 1:
        if any(p in FICTION_SUBGENRES for p in deduped):
            deduped = [p for p in deduped if p != 'Fiction']
        if any(p in NOVEL_SUBTYPES for p in deduped):
            deduped = [p for p in deduped if p != 'Novel']
        if 'Novel, epistolary' in deduped:
            deduped = [p for p in deduped if p != 'Letter']

    return ' | '.join(deduped) if deduped else None


# ── Year parsing ─────────────────────────────────────────────────────

def _parse_year(val):
    """Parse a year value to integer. Handles ranges, circa dates, etc."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s in ('nan', 'None', ''):
        return None
    for prefix in ('c.', 'c ', 'ca.', 'ca ', '[', ']', '?', '~'):
        s = s.replace(prefix, '')
    s = s.strip()
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        pass
    if '-' in s:
        parts = s.split('-')
        try:
            years = [int(float(p.strip())) for p in parts if p.strip()]
            years = [y for y in years if 100 < y < 2100]
            if years:
                return years[0]
        except (ValueError, OverflowError):
            pass
    m = re.search(r'\b(\d{4})\b', s)
    if m:
        try:
            return int(m.group(1))
        except (ValueError, OverflowError):
            pass
    return None


# ── Corpus DataFrame preparation ─────────────────────────────────────

def _resolve_freqs_paths(df, corpus):
    """Add path_freqs column with paths relative to PATH_CORPUS."""
    from lltk.imports import PATH_CORPUS
    corpus_root = os.path.expanduser(PATH_CORPUS)
    freqs_dir = getattr(corpus, 'path_freqs', None)
    if not freqs_dir or not os.path.isdir(freqs_dir):
        df['path_freqs'] = None
        return df
    has_custom = hasattr(corpus, 'freqs_path_for')
    ids = df.index if df.index.name == 'id' else df.get('id', pd.Series())
    paths = []
    if has_custom:
        for text_id in ids:
            try:
                abs_path = corpus.freqs_path_for(str(text_id))
                if abs_path and os.path.exists(abs_path):
                    paths.append(os.path.relpath(abs_path, corpus_root))
                else:
                    paths.append(None)
            except Exception:
                paths.append(None)
    else:
        ext = getattr(corpus, 'EXT_FREQS', getattr(corpus, 'ext_freqs', '.json'))
        for text_id in ids:
            abs_path = os.path.join(freqs_dir, str(text_id) + ext)
            if os.path.exists(abs_path):
                paths.append(os.path.relpath(abs_path, corpus_root))
            else:
                paths.append(None)
    df['path_freqs'] = paths
    n_found = df['path_freqs'].notna().sum()
    if log and n_found:
        log(f'  {n_found}/{len(df)} texts have freqs')
    return df


def prepare_corpus_df(df, corpus_id, corpus=None, default_lang=None):
    """Apply LLTK's standard metadata cleanup for a corpus DataFrame.

    Returns a DataFrame with canonical columns (CORE_COLS + 'meta') ready
    for insertion into ClickHouse.
    """
    df = df.copy()
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    if df.index.name is not None:
        df = df.reset_index()
    if 'id' not in df.columns:
        if log:
            log(f'No id column in {corpus_id} metadata')
        return None
    df = df[df['id'].notna() & (df['id'].astype(str) != '')]
    df = df.drop_duplicates(subset='id', keep='first')
    df['_id'] = '_' + corpus_id + '/' + df['id'].astype(str)
    df['corpus'] = corpus_id

    if 'genre' in df.columns and 'genre_raw' not in df.columns:
        df['genre_raw'] = df['genre']
    elif 'genre_raw' not in df.columns:
        df['genre_raw'] = None

    if 'genre' in df.columns:
        unknown = set(df['genre'].dropna().unique()) - GENRE_VOCAB - {''}
        if unknown and log:
            log(f'{corpus_id}: non-standard genre values: {unknown}')

    extra_cols = [c for c in df.columns if c not in CORE_COLS and c != 'meta']

    def row_to_json(row):
        d = {}
        for col in extra_cols:
            v = row[col]
            try:
                is_valid = pd.notna(v) and str(v) not in ('', 'nan', 'None', '[]')
            except ValueError:
                is_valid = len(v) > 0
            if is_valid:
                d[col] = str(v)
        return json.dumps(d) if d else None

    df['meta'] = df.apply(row_to_json, axis=1)

    if 'year' in df.columns:
        df['year'] = df['year'].apply(_parse_year)
    else:
        df['year'] = None

    if 'lang' in df.columns:
        df['lang'] = df['lang'].apply(normalize_lang)
    elif 'language' in df.columns:
        df['lang'] = df['language'].apply(normalize_lang)
    else:
        df['lang'] = normalize_lang(default_lang) if default_lang else None
    if default_lang and 'lang' in df.columns:
        norm_default = normalize_lang(default_lang)
        if norm_default:
            df['lang'] = df['lang'].fillna(norm_default)

    for col in TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({'nan': None, '': None, 'None': None})
        else:
            df[col] = None

    for col in ('genre', 'genre_raw'):
        if col not in df.columns:
            df[col] = None
    if 'is_translated' not in df.columns:
        df['is_translated'] = None
    else:
        col = df['is_translated']
        if col.dtype == object:
            col = col.map({
                'True': True, 'False': False, 'true': True, 'false': False,
                True: True, False: False,
            })
        df['is_translated'] = col.astype('boolean')

    if 'genre_raw' in df.columns:
        df['genre_raw'] = df['genre_raw'].apply(normalize_genre_raw)

    df['title_norm'] = df['title'].apply(normalize_title) if 'title' in df.columns else None
    df['author_norm'] = df['author'].apply(normalize_author) if 'author' in df.columns else None

    if corpus is not None:
        df = _resolve_freqs_paths(df, corpus)
    elif 'path_freqs' not in df.columns:
        df['path_freqs'] = None

    insert_cols = CORE_COLS + ['meta']
    return df[insert_cols].copy()


# ── Singleton ────────────────────────────────────────────────────────

from lltk.tools.metadb_ch import MetaDBCH
metadb = MetaDBCH()
