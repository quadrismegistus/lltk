"""
ClickHouse schema definitions for LLTK's core tables.

Mirrors the DuckDB schema (lltk/tools/metadb.py) but picks MergeTree engines
and ORDER BY keys optimized for ClickHouse's query patterns.

Notes on engine choices:
  - MergeTree: standard append-mostly table; no dedup.
  - ReplacingMergeTree: dedup by ORDER BY key, merged lazily in background.
    Use when you want UPSERT-like semantics (e.g. texts, match_groups).
  - ORDER BY determines the primary index (sparse marks) and data sort
    on disk — pick columns used in WHERE clauses.

Freqs data stays as per-corpus parquet files on disk; ClickHouse queries
them via the `file()` table function, no ingest needed.
"""

CLICKHOUSE_SCHEMA = {
    'texts': """
        CREATE TABLE IF NOT EXISTS {db}.texts (
            _id                     String,
            corpus                  LowCardinality(String),
            id                      String,
            title                   String,
            author                  String,
            year                    Nullable(Int32),
            genre                   LowCardinality(String),
            genre_raw               String,
            genre_corpus            String,
            genre_enriched_source   LowCardinality(String),
            title_norm              String,
            author_norm             String,
            path_freqs              Nullable(String),
            n_words                 Nullable(Int32),
            lang                    LowCardinality(String),
            lang_metadata           LowCardinality(String),
            lang_detected           LowCardinality(String),
            lang_coverage           Nullable(Float32),
            lang_confidence         Nullable(Float32),
            is_translated           Nullable(UInt8),
            original_lang           LowCardinality(String),
            meta                    String     -- JSON for corpus-specific fields
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (corpus, _id)
    """,

    'corpus_info': """
        CREATE TABLE IF NOT EXISTS {db}.corpus_info (
            corpus         LowCardinality(String),
            ingested_at    Float64,
            n_texts        Int32
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY corpus
    """,

    'matches': """
        CREATE TABLE IF NOT EXISTS {db}.matches (
            _id_a        String,
            _id_b        String,
            similarity   Float32,
            match_type   LowCardinality(String)
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (_id_a, _id_b)
    """,

    'match_groups': """
        CREATE TABLE IF NOT EXISTS {db}.match_groups (
            _id       String,
            group_id  Int32,
            rank      Int32
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY _id
    """,

    'wordcounts': """
        CREATE TABLE IF NOT EXISTS {db}.wordcounts (
            path_freqs  String,
            n_words     Int32
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY path_freqs
    """,

    'word_year_corpus': """
        CREATE TABLE IF NOT EXISTS {db}.word_year_corpus (
            word              String,
            year              Int32,
            corpus            LowCardinality(String),
            genre             LowCardinality(String),
            word_count        Int64,
            n_texts           Int32,
            word_count_dedup  Int64,
            n_texts_dedup     Int32
        )
        ENGINE = MergeTree()
        ORDER BY (word, year, corpus)
    """,

    'year_corpus_totals': """
        CREATE TABLE IF NOT EXISTS {db}.year_corpus_totals (
            year                Int32,
            corpus              LowCardinality(String),
            genre               LowCardinality(String),
            n_texts             Int32,
            total_words         Int64,
            n_texts_dedup       Int32,
            total_words_dedup   Int64
        )
        ENGINE = MergeTree()
        ORDER BY (year, corpus)
    """,

    'text_freqs': """
        CREATE TABLE IF NOT EXISTS {db}.text_freqs (
            _id     String,
            corpus  LowCardinality(String),
            freqs   Map(String, UInt32)
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY _id
    """,
}


def create_all_tables(adapter, database='lltk'):
    """Create every LLTK table on the ClickHouse server via `adapter`.

    Idempotent (CREATE TABLE IF NOT EXISTS). Only meaningful for
    ClickHouseAdapter; other engines raise.
    """
    if adapter.engine != 'clickhouse':
        raise ValueError(f'create_all_tables is ClickHouse-only; got engine={adapter.engine}')
    adapter.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    for name, ddl in CLICKHOUSE_SCHEMA.items():
        adapter.execute(ddl.format(db=database))
    return list(CLICKHOUSE_SCHEMA.keys())
