"""
MetaDBCH — ClickHouse-backed replacement for MetaDB.

Same public API as the old DuckDB-backed MetaDB class, but all storage and
queries go through ClickHouse. No DuckDB files touched anywhere.

Methods are migrated in phases (see below). Unported methods raise
NotImplementedError with a hint to the phase they'll land in.

Phase A (done):  query, get, corpora, corpus_info, drop, drop_matches,
                 rebuild, ingest, build_freqs_db, read_freqs, detect_langs
Phase B (next):  match, find_matches, get_group, match_stats, wordcounts,
                 enrich_genres, detect_translations, build_word_index_sql,
                 ngram, has_word_index
Phase C (later): info, validate, validate_genres, virtual texts/corpus,
                 backup
"""

import os
import json
import time
import pandas as pd

from lltk.tools.db_adapter import get_adapter
from lltk.tools.clickhouse_schema import create_all_tables


DEFAULT_CH_URL = 'clickhouse://lltk:lltk@localhost:8123/lltk'


# Mapping of legacy DuckDB-style table references → ClickHouse equivalents.
# Applied by the legacy conn shim for web-app backwards compatibility.
_LEGACY_TABLE_REWRITES = [
    ('match_db.matches',         'lltk.matches'),
    ('match_db.match_groups',    'lltk.match_groups'),
    ('wc_db.wordcounts',         'lltk.wordcounts'),
    ('wi_db.word_year_corpus',   'lltk.word_year_corpus'),
    ('wi_db.year_corpus_totals', 'lltk.year_corpus_totals'),
    ('freqs_db.text_freqs',      'lltk.text_freqs'),
]


def _rewrite_legacy_sql(sql):
    """Rewrite DuckDB-specific syntax in a SQL string for ClickHouse.

    - Table name qualifiers (match_db.* → lltk.*, etc.)
    - `FROM texts` (bare) → `FROM lltk.texts`
    - `json_extract_string(col, '$.x')` → `JSONExtractString(col, 'x')`
    - `to_timestamp(x)` → `toDateTime(x)`
    """
    import re
    for old, new in _LEGACY_TABLE_REWRITES:
        sql = sql.replace(old, new)

    # Bare `texts` / `corpus_info` references → lltk.*.
    # Only match when the identifier stands alone (whitespace/paren/start-of-line on either side),
    # and is NOT already prefixed with a database name.
    sql = re.sub(r'(?<![.\w])(texts|corpus_info)(?![.\w])',
                 lambda m: f'lltk.{m.group(1)}', sql)

    # Re-undo any double-prefixing that may have occurred
    sql = sql.replace('lltk.lltk.', 'lltk.')

    # json_extract_string → JSONExtractString, stripping '$.' if present
    def _json_extract(m):
        col = m.group(1).strip()
        path = m.group(2).strip().strip("'\"")
        if path.startswith('$.'):
            path = path[2:]
        return f"JSONExtractString({col}, '{path}')"
    sql = re.sub(
        r"json_extract_string\s*\(\s*([^,]+)\s*,\s*('\$\.[^']+'|\"\$\.[^\"]+\")\s*\)",
        _json_extract, sql,
    )

    # to_timestamp(x) → toDateTime(x)
    sql = re.sub(r'\bto_timestamp\s*\(', 'toDateTime(', sql)

    # random() → rand() — DuckDB has random(), ClickHouse needs rand()/rand64()
    sql = re.sub(r'\brandom\s*\(\s*\)', 'rand()', sql)

    return sql


class _LegacyResult:
    """Mimic DuckDB's result-object surface over ClickHouse query results."""

    def __init__(self, adapter, sql, params=None):
        self._adapter = adapter
        self._sql = sql
        self._params = params

    def _run(self):
        if self._params:
            # DuckDB-style positional '?' params aren't natively supported by
            # clickhouse-connect. Inline them (SQL injection risk is limited
            # to internal callers — LLTK doesn't accept user SQL here).
            sql = self._sql
            for p in self._params:
                if isinstance(p, str):
                    p = "'" + p.replace("'", "''") + "'"
                elif p is None:
                    p = 'NULL'
                else:
                    p = str(p)
                sql = sql.replace('?', p, 1)
        else:
            sql = self._sql
        return self._adapter.client.query(sql).result_rows

    def fetchone(self):
        rows = self._run()
        return rows[0] if rows else None

    def fetchall(self):
        return self._run()

    def fetchdf(self):
        sql = self._sql
        if self._params:
            for p in self._params:
                if isinstance(p, str):
                    p = "'" + p.replace("'", "''") + "'"
                elif p is None:
                    p = 'NULL'
                else:
                    p = str(p)
                sql = sql.replace('?', p, 1)
        return self._adapter.client.query_df(sql)


class _LegacyConnShim:
    """DuckDB-style conn shim over a ClickHouse adapter.

    Use for backward-compat with code that does:
        db.conn.execute(sql, params).fetchone()/fetchall()/fetchdf()

    New code should call `db.adapter.query(...)` / `db.query_df(...)` directly.
    """

    def __init__(self, adapter):
        self._adapter = adapter

    def execute(self, sql, params=None):
        return _LegacyResult(self._adapter, _rewrite_legacy_sql(sql), params)


class MetaDBCH:
    def __init__(self, url=None):
        self.url = url or os.environ.get('LLTK_CLICKHOUSE_URL', DEFAULT_CH_URL)
        self._adapter = None

    @property
    def adapter(self):
        if self._adapter is None:
            self._adapter = get_adapter(self.url)
        return self._adapter

    @property
    def client(self):
        return self.adapter.client

    # ── Reads ──

    def query(self, sql):
        """Run an arbitrary SELECT. Returns a DataFrame."""
        return self.adapter.query_df(sql)

    def get(self, *args):
        """Single-row lookup. Returns dict (core fields + unpacked meta JSON) or None.

            .get('_estc/T012345')
            .get('estc', 'T012345')
        """
        if len(args) == 1:
            _id = args[0]
        elif len(args) == 2:
            _id = f'_{args[0]}/{args[1]}'
        else:
            raise ValueError('get() takes 1 or 2 arguments')

        _id_esc = _id.replace("'", "''")
        rows = self.adapter.query(f"SELECT * FROM lltk.texts WHERE _id = '{_id_esc}' LIMIT 1")
        if not rows:
            return None

        # Column names from system.columns
        cols = [r[0] for r in self.adapter.query(
            "SELECT name FROM system.columns "
            "WHERE database='lltk' AND table='texts' ORDER BY position"
        )]
        row = dict(zip(cols, rows[0]))
        meta_json = row.pop('meta', None)
        if meta_json:
            try:
                row.update(json.loads(meta_json))
            except (json.JSONDecodeError, TypeError):
                pass
        return {k: v for k, v in row.items() if v not in (None, '', 0)} or None

    def corpora(self):
        """List ingested corpora with row counts."""
        return self.query(
            "SELECT corpus, COUNT(*) AS n FROM lltk.texts "
            "GROUP BY corpus ORDER BY corpus"
        )

    def corpus_info(self):
        """Ingest timestamps and row counts per corpus."""
        try:
            return self.query("""
                SELECT corpus, n_texts,
                       toDateTime(ingested_at) AS ingested_at
                FROM lltk.corpus_info
                ORDER BY corpus
            """)
        except Exception:
            return pd.DataFrame()

    # ── Writes ──

    def ensure_schema(self):
        """Create database + all tables if missing. Idempotent."""
        self.adapter.execute("CREATE DATABASE IF NOT EXISTS lltk")
        create_all_tables(self.adapter, database='lltk')

    def ingest(self, corpus_id, force=True):
        """Ingest one corpus's metadata into lltk.texts. Returns row count."""
        from lltk.tools.clickhouse_rebuild import ingest_corpus_to_clickhouse
        return ingest_corpus_to_clickhouse(corpus_id, self.adapter, force=force)

    def rebuild(self, corpus_ids=None, progress=True):
        """Full rebuild from corpus metadata CSVs into ClickHouse."""
        from lltk.tools.clickhouse_rebuild import rebuild_clickhouse
        return rebuild_clickhouse(
            self.adapter, corpora=corpus_ids, force=(corpus_ids is None),
        )

    def drop(self, corpus_id=None):
        """Drop data for one corpus, or all data if corpus_id is None.

        TRUNCATE for full drops (fast); ALTER TABLE DELETE for per-corpus.
        """
        if corpus_id:
            cid_esc = corpus_id.replace("'", "''")
            self.adapter.execute(
                f"ALTER TABLE lltk.texts DELETE WHERE corpus = '{cid_esc}' "
                f"SETTINGS mutations_sync=1"
            )
            self.adapter.execute(
                f"ALTER TABLE lltk.corpus_info DELETE WHERE corpus = '{cid_esc}' "
                f"SETTINGS mutations_sync=1"
            )
            # Also clean up downstream tables scoped to this corpus
            self.adapter.execute(
                f"ALTER TABLE lltk.text_freqs DELETE WHERE corpus = '{cid_esc}' "
                f"SETTINGS mutations_sync=1"
            )
            self.adapter.execute(
                f"ALTER TABLE lltk.matches DELETE "
                f"WHERE _id_a LIKE '_{cid_esc}/%' OR _id_b LIKE '_{cid_esc}/%' "
                f"SETTINGS mutations_sync=1"
            )
        else:
            for t in ('texts', 'corpus_info', 'text_freqs',
                      'matches', 'match_groups', 'wordcounts',
                      'word_year_corpus', 'year_corpus_totals'):
                try:
                    self.adapter.execute(f"TRUNCATE TABLE lltk.{t}")
                except Exception:
                    pass

    def drop_matches(self):
        """Clear all matches; keep texts intact."""
        for t in ('matches', 'match_groups'):
            try:
                self.adapter.execute(f"TRUNCATE TABLE lltk.{t}")
            except Exception:
                pass

    # ── Freqs (delegates to clickhouse_ingest) ──

    def build_freqs_db(self, corpora=None, num_proc=None, batch_size=2000,
                       truncate_first=False):
        from lltk.tools.clickhouse_ingest import ingest_freqs_from_jsons
        return ingest_freqs_from_jsons(
            self.adapter, corpora=corpora, batch_size=batch_size,
            num_proc=num_proc, truncate_first=truncate_first,
        )

    def read_freqs(self, ids=None, corpora=None, as_df=True):
        """Read rows from lltk.text_freqs. Returns DataFrame or list of tuples."""
        wheres = []
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            wheres.append(f"corpus IN ({cl})")
        if ids is not None:
            il = ', '.join(f"'{i}'" for i in list(ids))
            wheres.append(f"_id IN ({il})")
        where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ''
        sql = f"SELECT _id, corpus, freqs FROM lltk.text_freqs {where_sql}"
        return self.adapter.query_df(sql) if as_df else self.adapter.query(sql)

    # ── Phase B / C placeholders ──

    def _phase_b(self, name):
        raise NotImplementedError(
            f'{name}() not yet ported to ClickHouse. Tracking in Phase B.'
        )

    def match(self, corpora=None, fuzzy=False, containment=True, progress=True):
        from lltk.tools.clickhouse_match import match_clickhouse
        return match_clickhouse(self.adapter, corpora=corpora, fuzzy=fuzzy,
                                containment=containment, progress=progress)

    def find_matches(self, query):
        from lltk.tools.clickhouse_match import find_matches_ch
        return find_matches_ch(self.adapter, query)

    def get_group(self, _id):
        from lltk.tools.clickhouse_match import get_group_ch
        return get_group_ch(self.adapter, _id)

    def match_stats(self):
        from lltk.tools.clickhouse_match import match_stats_ch
        return match_stats_ch(self.adapter)
    def wordcounts(self, *a, **kw):       return self._phase_b('wordcounts')

    def enrich_genres(self, progress=True):
        from lltk.tools.clickhouse_enrich import enrich_genres_ch
        return enrich_genres_ch(self.adapter, progress=progress)

    def detect_translations(self):
        from lltk.tools.clickhouse_enrich import detect_translations_ch
        return detect_translations_ch(self.adapter)
    def build_word_index_sql(self, vocab_size=50_000, min_count=1,
                             corpora=None, **unused):
        from lltk.tools.clickhouse_wordindex import build_word_index_ch
        return build_word_index_ch(
            self.adapter, vocab_size=vocab_size, min_count=min_count,
            corpora=corpora,
        )

    def build_text_words(self, corpora=None, force=False):
        """Build lltk.text_words + lltk.text_stats from text_freqs.

        text_words is a flat (word, _id, count) inversion of freqs. Queries
        on a single word become sub-second index-range scans instead of
        full-column Map scans. text_stats stores per-text total_tokens.
        """
        from lltk.tools.clickhouse_text_words import (
            build_text_words, build_text_stats,
        )
        n_words = build_text_words(
            self.adapter, corpora=corpora, force=force,
        )
        build_text_stats(self.adapter, force=force)
        return n_words

    def ngram(self, words, genre=None, corpus=None, year_min=None,
              year_max=None, dedup=False, by_corpus=False):
        from lltk.tools.clickhouse_wordindex import ngram_ch
        return ngram_ch(self.adapter, words, genre=genre, corpus=corpus,
                        year_min=year_min, year_max=year_max,
                        dedup=dedup, by_corpus=by_corpus)

    def has_word_index(self):
        from lltk.tools.clickhouse_wordindex import has_word_index_ch
        return has_word_index_ch(self.adapter)

    def detect_langs(self, min_tokens=50, coverage_threshold=0.05,
                     confidence_threshold=2.0, batch_size=5000, progress=True,
                     **unused):
        """Run stopword-intersection language detection over lltk.text_freqs.
        Results land in lltk.text_langs (one row per _id, ReplacingMergeTree
        on _id so re-runs overwrite).
        """
        from lltk.tools.clickhouse_detect_langs import detect_langs_clickhouse
        return detect_langs_clickhouse(
            self.adapter,
            min_tokens=min_tokens,
            coverage_threshold=coverage_threshold,
            confidence_threshold=confidence_threshold,
            batch_size=batch_size,
            progress=progress,
        )

    # ── Legacy compatibility shim ────────────────────────────────────
    # The web app (and some abstraction-project code) expects a DuckDB-style
    # `.conn.execute(sql).fetchone()/fetchall()/fetchdf()` interface. The
    # shim routes those calls to ClickHouse after rewriting DuckDB-only
    # table references (match_db.x → lltk.x, texts → lltk.texts).

    @property
    def conn(self):
        return _LegacyConnShim(self.adapter)

    @property
    def match_conn(self):
        return _LegacyConnShim(self.adapter)

    def __repr__(self):
        try:
            n = self.adapter.query("SELECT COUNT(*) FROM lltk.texts")[0][0]
            nc = self.adapter.query("SELECT COUNT(DISTINCT corpus) FROM lltk.texts")[0][0]
            return f'MetaDBCH({n:,} texts, {nc} corpora @ {self.url})'
        except Exception:
            return f'MetaDBCH(@ {self.url})'

    def close(self):
        if self._adapter is not None:
            try:
                self._adapter.close()
            except Exception:
                pass
            self._adapter = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
