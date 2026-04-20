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

    'text_langs': """
        CREATE TABLE IF NOT EXISTS {db}.text_langs (
            _id              String,
            lang_detected    LowCardinality(String),
            lang_coverage    Float32,
            lang_confidence  Float32,
            detected_at      DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(detected_at)
        ORDER BY _id
    """,

    'text_genres': """
        CREATE TABLE IF NOT EXISTS {db}.text_genres (
            _id                     String,
            genre                   LowCardinality(String),
            genre_raw               String,
            genre_corpus            LowCardinality(String),
            genre_enriched_source   LowCardinality(String),
            enriched_at             DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(enriched_at)
        ORDER BY _id
    """,

    'text_genre_tags': """
        CREATE TABLE IF NOT EXISTS {db}.text_genre_tags (
            _id     String,
            tag     LowCardinality(String),
            facet   LowCardinality(String),
            tagged_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (tag, _id)
    """,

    'text_translations': """
        CREATE TABLE IF NOT EXISTS {db}.text_translations (
            _id              String,
            is_translated    UInt8,
            original_lang    LowCardinality(String),
            detected_at      DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(detected_at)
        ORDER BY _id
    """,

    # Flat (word, _id, count) long-format inversion of text_freqs.
    # ORDER BY (word, _id) means queries for a specific word read a contiguous
    # index range — sub-second even across billions of rows. LowCardinality
    # word column compresses heavily (most words repeat across texts).
    'text_words': """
        CREATE TABLE IF NOT EXISTS {db}.text_words (
            word    LowCardinality(String),
            _id     String,
            count   UInt32,
            corpus  LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY (word, _id)
        SETTINGS index_granularity = 8192
    """,

    # Small per-text stats table (populated from text_freqs). Makes
    # total-tokens JOINs cheap for detect_langs etc. without scanning the
    # full freqs Map column.
    'text_stats': """
        CREATE TABLE IF NOT EXISTS {db}.text_stats (
            _id            String,
            corpus         LowCardinality(String),
            n_words        UInt64,
            n_unique_words UInt32
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY _id
    """,

    # Cross-corpus annotation store. Writable by lltk itself and by client
    # packages (largeliterarymodels writing LLM outputs, curation UIs writing
    # human labels). Plain MergeTree preserves history — the resolver VIEW
    # (annotations_latest) picks the winning value per (_id, field) by source
    # priority. See `lltk.tools.annotations`.
    #
    # Schema contract:
    #   _id           canonical text address `_{corpus}/{id}`
    #   field         column name (genre, is_translated, year_estimated, ...)
    #   value         always String — caller encodes int/bool; field_spec
    #                 documents the expected shape per field
    #   source        who wrote it ('human', 'fiction_biblio',
    #                 'llm:gemini-2.5-pro', ...). Joined to annotation_sources
    #                 to get a priority score
    #   run_id        batch identifier; default `{source}:{YYYY-MM-DD}` at
    #                 write time if caller omits
    #   annotated_at  wall-clock DateTime, tie-breaks priority ties
    #   confidence    Float32 per-row confidence (optional; default 1.0)
    #   meta          JSON blob for free-form extensions (prompt fingerprint,
    #                 upstream tool version, etc.)
    'annotations': """
        CREATE TABLE IF NOT EXISTS {db}.annotations (
            _id           String,
            field         LowCardinality(String),
            value         String,
            source        LowCardinality(String),
            run_id        String,
            annotated_at  DateTime DEFAULT now(),
            confidence    Float32  DEFAULT 1.0,
            meta          String   DEFAULT '{{}}'
        )
        ENGINE = MergeTree()
        ORDER BY (_id, field, source, annotated_at)
    """,

    # Dim table of annotation sources and their priorities. Higher priority
    # wins at read time (see annotations_latest VIEW).
    #
    # Seeded priorities (see lltk.tools.annotations.DEFAULT_SOURCES):
    #   human                       100   manual curation
    #   bibliography:fiction_biblio   90   scholarly bibliographies
    #   bibliography:end              90
    #   bibliography:ravengarside     90
    #   heuristic                     50   rule-based (ESTC form, title kw)
    #   llm:*                         10   LLM outputs (auto-registered)
    #
    # Unregistered sources default to priority 0 (lowest) in the resolver VIEW.
    'annotation_sources': """
        CREATE TABLE IF NOT EXISTS {db}.annotation_sources (
            source         LowCardinality(String),
            priority       Int16,
            description    String,
            registered_at  DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(registered_at)
        ORDER BY source
    """,
}

# The `annotations_latest` VIEW runs `argMax(value, (priority, annotated_at))`
# per (_id, field) to resolve the current winning annotation. Defined as a
# separate DDL because CREATE VIEW can't be CREATE-IF-NOT-EXISTS-idempotent
# and depends on the two tables above existing first.
#
# priority semantic: HIGHER wins. Sources not in annotation_sources
# default to priority 0 via LEFT JOIN + coalesce.
ANNOTATIONS_LATEST_VIEW = """
    CREATE OR REPLACE VIEW {db}.annotations_latest AS
    SELECT
        _id,
        field,
        tupleElement(winner, 1) AS value,
        tupleElement(winner, 2) AS source,
        tupleElement(winner, 3) AS confidence,
        tupleElement(winner, 4) AS annotated_at
    FROM (
        SELECT
            _id,
            field,
            argMax(
                tuple(value, source, confidence, annotated_at),
                tuple(priority, annotated_at)
            ) AS winner
        FROM (
            SELECT
                a._id AS _id,
                a.field AS field,
                a.value AS value,
                a.source AS source,
                a.confidence AS confidence,
                a.annotated_at AS annotated_at,
                coalesce(s.priority, 0) AS priority
            FROM {db}.annotations a
            LEFT JOIN (
                SELECT source, priority FROM {db}.annotation_sources FINAL
            ) s
                ON a.source = s.source
        )
        GROUP BY _id, field
    )
"""


def create_all_tables(adapter, database='lltk'):
    """Create every LLTK table on the ClickHouse server via `adapter`.

    Idempotent (CREATE TABLE IF NOT EXISTS + CREATE OR REPLACE VIEW). Only
    meaningful for ClickHouseAdapter; other engines raise.
    """
    if adapter.engine != 'clickhouse':
        raise ValueError(f'create_all_tables is ClickHouse-only; got engine={adapter.engine}')
    adapter.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    for name, ddl in CLICKHOUSE_SCHEMA.items():
        adapter.execute(ddl.format(db=database))
    # Views depend on tables existing; run after the table loop.
    adapter.execute(ANNOTATIONS_LATEST_VIEW.format(db=database))
    return list(CLICKHOUSE_SCHEMA.keys()) + ['annotations_latest (view)']
