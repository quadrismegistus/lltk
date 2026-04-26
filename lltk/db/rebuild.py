"""
Fresh rebuild of LLTK metadata tables directly into ClickHouse.

Loads each corpus's metadata.csv (via `lltk.load(c).load_metadata()`),
applies the standard cleanup (id normalization, genre harmonization,
title/author normalization, meta JSON, lang resolution), and INSERTs
into ClickHouse's `lltk.texts` table.

No DuckDB staging — the corpus metadata files are the source of truth,
ClickHouse is the target.

Usage:

    from lltk.db.adapter import get_adapter
    from lltk.db.rebuild import rebuild_clickhouse

    ch = get_adapter('clickhouse://lltk:lltk@localhost:8123/lltk')
    rebuild_clickhouse(ch, corpora=['gildedage'])   # one corpus
    rebuild_clickhouse(ch)                            # all manifest corpora
"""

import os
import time
import json
import pandas as pd
import pyarrow as pa

from lltk.db.metadb import (
    prepare_corpus_df, _resolve_freqs_paths,
    CORE_COLS, DB_BLACKLIST,
)


# Schema of the `texts` table ClickHouse side. Must match clickhouse_schema.py.
# Built from CORE_COLS + meta. Types here drive pyarrow cast on the DataFrame.
_CH_TEXTS_COLS = CORE_COLS + ['meta']


def _df_to_arrow_texts(df: pd.DataFrame) -> pa.Table:
    """Cast a prepared corpus DataFrame to the Arrow schema ClickHouse expects.

    - boolean `is_translated` → nullable UInt8
    - Int32 `year` (nullable)
    - Nullable strings for everything else
    """
    # Work on a copy so we don't mutate caller's frame
    df = df.copy()

    # Ensure every expected column exists
    for c in _CH_TEXTS_COLS:
        if c not in df.columns:
            df[c] = None

    # Coerce types
    if 'year' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int32')

    if 'is_translated' in df.columns:
        # pandas nullable BooleanDtype → UInt8 (0/1) for ClickHouse compatibility
        col = df['is_translated']
        if hasattr(col.dtype, 'na_value'):
            # nullable BooleanArray
            col = col.astype('object').where(col.notna(), None)
        df['is_translated'] = col.map(
            {True: 1, False: 0, 'True': 1, 'False': 0, 'true': 1, 'false': 0}
        )

    # All remaining columns → string/None
    for c in _CH_TEXTS_COLS:
        if c in ('year', 'is_translated'):
            continue
        df[c] = df[c].where(df[c].notna(), None)
        df[c] = df[c].astype('object').map(
            lambda v: None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)
        )

    schema = pa.schema([
        ('_id',                   pa.string()),
        ('corpus',                pa.string()),
        ('id',                    pa.string()),
        ('title',                 pa.string()),
        ('author',                pa.string()),
        ('year',                  pa.int32()),
        ('genre',                 pa.string()),
        ('genre_raw',             pa.string()),
        ('lang',                  pa.string()),
        ('is_translated',         pa.uint8()),
        ('title_norm',            pa.string()),
        ('author_norm',           pa.string()),
        ('path_freqs',            pa.string()),
        ('meta',                  pa.string()),
    ])
    table = pa.Table.from_pandas(df[_CH_TEXTS_COLS], schema=schema, preserve_index=False)
    return table


def ingest_corpus_to_clickhouse(corpus_id: str, ch_adapter,
                                 force: bool = True) -> int:
    """Rebuild a single corpus's rows in ClickHouse lltk.texts.

    Mirrors MetaDB.ingest() but writes to ClickHouse. Returns row count,
    or None if the corpus was skipped.
    """
    if corpus_id in DB_BLACKLIST:
        print(f'Skipping {corpus_id} (DB_BLACKLIST)')
        return None

    from lltk.corpus.utils import load
    from lltk.corpus.synthetic import SyntheticCorpus
    try:
        corpus = load(corpus_id)
    except Exception as e:
        print(f'Could not load {corpus_id}: {e}')
        return None
    if isinstance(corpus, SyntheticCorpus):
        print(f'Skipping {corpus_id} (SyntheticCorpus)')
        return None

    try:
        df = corpus.load_metadata()
    except Exception as e:
        print(f'Could not load metadata for {corpus_id}: {e}')
        return None
    if df is None or not len(df):
        print(f'No metadata for {corpus_id}')
        return None

    manifest_lang = getattr(corpus, 'lang', None)
    prepared = prepare_corpus_df(df, corpus_id, corpus=corpus, default_lang=manifest_lang)
    if prepared is None or not len(prepared):
        return 0

    if force:
        ch_adapter.execute(
            f"ALTER TABLE lltk.texts DELETE WHERE corpus = '{corpus_id}'"
        )

    table = _df_to_arrow_texts(prepared)
    ch_adapter.client.insert_arrow('texts', table)

    ch_adapter.execute(f"""
        INSERT INTO lltk.corpus_info (corpus, ingested_at, n_texts)
        VALUES ('{corpus_id}', {time.time()}, {len(prepared)})
    """)
    return len(prepared)


def rebuild_clickhouse(ch_adapter, corpora=None, force=True):
    """Full db-rebuild equivalent: ingest every manifest corpus into
    ClickHouse lltk.texts. Loops in manifest order, logs per-corpus count.
    """
    if corpora is None:
        from lltk.corpus.utils import get_inducted_corpus_ids
        corpora = get_inducted_corpus_ids()

    if force:
        ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.texts")
        ch_adapter.execute("TRUNCATE TABLE IF EXISTS lltk.corpus_info")

    print(f'Rebuilding ClickHouse from {len(corpora)} corpora...')
    t0 = time.time()
    total = 0
    for i, cid in enumerate(corpora, 1):
        tc = time.time()
        try:
            n = ingest_corpus_to_clickhouse(cid, ch_adapter, force=False)
            if n:
                total += n
                print(f'  [{i}/{len(corpora)}] {cid}: {n:,} rows ({time.time()-tc:.1f}s)',
                      flush=True)
        except Exception as e:
            print(f'  [{i}/{len(corpora)}] {cid}: ERROR {type(e).__name__}: {str(e)[:150]}',
                  flush=True)

    print(f'\nTotal: {total:,} rows across {len(corpora)} corpora in {time.time()-t0:.0f}s')
    return total
