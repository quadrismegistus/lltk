"""
Centralized DuckDB metadata store for lltk.

CSV + load_metadata() remain the source of truth.
This DB is a read cache for fast single-row lookups and cross-corpus queries.

Usage:
    import lltk

    lltk.db.ingest('estc')           # ingest one corpus
    lltk.db.rebuild()                # drop and re-ingest everything

    lltk.db.get('_estc/T012345')     # single-row lookup by _id -> dict
    lltk.db.get('estc', 'T012345')   # single-row lookup by corpus + id -> dict
    lltk.db.query("SELECT * FROM texts WHERE year < 1700 AND genre = 'Fiction'")

    lltk.db.validate()               # report on standard column coverage
    lltk.db.validate_genres()        # distinct genre values per corpus
    lltk.db.corpus_info()            # ingest timestamps per corpus
"""

import json
import os
import time
import duckdb
import numpy as np
import pandas as pd
from lltk.imports import PATH_LLTK_DATA, log

PATH_METADB = os.path.join(PATH_LLTK_DATA, 'metadb.duckdb')

# Standard genre vocabulary — harmonized across corpora
GENRE_VOCAB = {
    'Fiction',
    'Poetry',
    'Drama',
    'Periodical',
    'Essay',
    'Treatise',
    'Letters',
    'Sermon',
    'Biography',
    'Nonfiction',
    'Legal',
    'Speech',
    'History',
    'Criticism',
    'Almanac',
    'Reference',
}

# Core columns stored as real columns; everything else goes in meta JSON
CORE_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'year', 'genre', 'genre_raw']
STANDARD_COLS = ['id', 'title', 'author', 'year', 'genre', 'genre_raw']
TEXT_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'genre', 'genre_raw']  # cols stored as TEXT


def _parse_year(val):
    """Parse a year value to integer. Handles ranges, circa dates, etc."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s in ('nan', 'None', ''):
        return None

    # Remove circa, brackets, etc.
    for prefix in ('c.', 'c ', 'ca.', 'ca ', '[', ']', '?', '~'):
        s = s.replace(prefix, '')
    s = s.strip()

    # Try direct int parse
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        pass

    # Try range: "1700-1710" -> 1705
    if '-' in s:
        parts = s.split('-')
        try:
            years = [int(float(p.strip())) for p in parts if p.strip()]
            # Filter to plausible years
            years = [y for y in years if 100 < y < 2100]
            if years:
                return years[0]  # take first year of range
        except (ValueError, OverflowError):
            pass

    # Take first 4-digit number
    import re
    m = re.search(r'\b(\d{4})\b', s)
    if m:
        try:
            return int(m.group(1))
        except (ValueError, OverflowError):
            pass

    return None


class MetaDB:
    def __init__(self, path=None):
        self.path = path or PATH_METADB
        self._conn = None
        self._col_cache = None

    @property
    def conn(self):
        if self._conn is None:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._conn = duckdb.connect(self.path)
            self._ensure_tables()
        return self._conn

    def _ensure_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS texts (
                _id       TEXT PRIMARY KEY,
                corpus    TEXT NOT NULL,
                id        TEXT NOT NULL,
                title     TEXT,
                author    TEXT,
                year      INTEGER,
                genre     TEXT,
                genre_raw TEXT,
                meta      TEXT
            )
        """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_corpus ON texts(corpus)
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS corpus_info (
                corpus      TEXT PRIMARY KEY,
                ingested_at DOUBLE NOT NULL,
                n_texts     INTEGER NOT NULL
            )
        """)

    def ingest(self, corpus_id, force=True):
        """Ingest a corpus's load_metadata() output into the DB."""
        from lltk.corpus.utils import load
        try:
            corpus = load(corpus_id)
        except Exception as e:
            if log: log(f'Could not load corpus {corpus_id}: {e}')
            return None

        try:
            df = corpus.load_metadata()
        except Exception as e:
            if log: log(f'Could not load metadata for {corpus_id}: {e}')
            return None

        if df is None or not len(df):
            if log: log(f'No metadata for {corpus_id}')
            return None

        return self.ingest_df(df, corpus_id, force=force)

    def ingest_df(self, df, corpus_id, force=True):
        """Ingest a DataFrame into the DB for a given corpus."""
        df = df.copy()

        # Dedupe column names (keep last)
        df = df.loc[:, ~df.columns.duplicated(keep='last')]

        # Ensure index is the id column
        if df.index.name is not None:
            df = df.reset_index()

        if 'id' not in df.columns:
            if log: log(f'No id column in {corpus_id} metadata')
            return None

        # Drop rows with no id
        df = df[df['id'].notna() & (df['id'].astype(str) != '')]

        # Drop duplicate ids (keep first)
        df = df.drop_duplicates(subset='id', keep='first')

        # Build _id and corpus
        df['_id'] = '_' + corpus_id + '/' + df['id'].astype(str)
        df['corpus'] = corpus_id

        # Handle genre_raw: preserve whatever the corpus calls 'genre' as genre_raw
        if 'genre' in df.columns and 'genre_raw' not in df.columns:
            df['genre_raw'] = df['genre']
        elif 'genre_raw' not in df.columns:
            df['genre_raw'] = None

        # Validate genre against GENRE_VOCAB
        if 'genre' in df.columns:
            unknown = set(df['genre'].dropna().unique()) - GENRE_VOCAB - {''}
            if unknown:
                if log: log(f'{corpus_id}: non-standard genre values: {unknown}')

        # Separate core cols from extra cols
        extra_cols = [c for c in df.columns if c not in CORE_COLS and c != 'meta']

        # Build meta JSON from extra columns
        def row_to_json(row):
            d = {}
            for col in extra_cols:
                v = row[col]
                if pd.notna(v) and str(v) not in ('', 'nan', 'None'):
                    d[col] = str(v)
            return json.dumps(d) if d else None

        df['meta'] = df.apply(row_to_json, axis=1)

        # Parse year to integer
        if 'year' in df.columns:
            df['year'] = df['year'].apply(_parse_year)
        else:
            df['year'] = None

        # Convert text core cols to string (except keep NaN as None)
        for col in TEXT_COLS:
            if col in df.columns:
                df[col] = df[col].astype(str).replace({'nan': None, '': None, 'None': None})
            else:
                df[col] = None

        # Ensure genre/genre_raw columns exist
        for col in ('genre', 'genre_raw'):
            if col not in df.columns:
                df[col] = None

        # Select only core cols + meta for insert
        insert_df = df[CORE_COLS + ['meta']].copy()

        # Remove old data for this corpus
        if force:
            self.conn.execute("DELETE FROM texts WHERE corpus = ?", [corpus_id])

        # Insert
        self.conn.execute(
            "INSERT INTO texts SELECT * FROM insert_df",
        )

        count = self.conn.execute(
            "SELECT COUNT(*) FROM texts WHERE corpus = ?", [corpus_id]
        ).fetchone()[0]

        # Update corpus_info
        self.conn.execute("""
            INSERT OR REPLACE INTO corpus_info (corpus, ingested_at, n_texts)
            VALUES (?, ?, ?)
        """, [corpus_id, time.time(), count])

        if log: log(f'Ingested {count} texts from {corpus_id}')
        self._col_cache = None
        return count

    def get(self, *args):
        """
        Single-row lookup. Returns dict (core fields + unpacked meta JSON) or None.

            .get('_estc/T012345')         # by _id
            .get('estc', 'T012345')       # by corpus + id
        """
        if len(args) == 1:
            _id = args[0]
        elif len(args) == 2:
            _id = f'_{args[0]}/{args[1]}'
        else:
            raise ValueError('get() takes 1 or 2 arguments: get(_id) or get(corpus, id)')

        try:
            result = self.conn.execute(
                "SELECT * FROM texts WHERE _id = ?", [_id]
            ).fetchone()
        except Exception:
            return None

        if result is None:
            return None

        # Get column names (cached)
        if self._col_cache is None:
            self._col_cache = [desc[0] for desc in self.conn.execute(
                "SELECT * FROM texts LIMIT 0"
            ).description]

        row = dict(zip(self._col_cache, result))

        # Unpack meta JSON into the dict
        meta_json = row.pop('meta', None)
        if meta_json:
            try:
                extra = json.loads(meta_json)
                row.update(extra)
            except (json.JSONDecodeError, TypeError):
                pass

        # Filter out None values
        return {k: v for k, v in row.items() if v is not None}

    def query(self, sql):
        """Run an arbitrary SQL query against the texts table. Returns DataFrame."""
        return self.conn.execute(sql).fetchdf()

    def corpora(self):
        """List all ingested corpora with row counts."""
        return self.query(
            "SELECT corpus, COUNT(*) as n FROM texts GROUP BY corpus ORDER BY corpus"
        )

    def corpus_info(self):
        """Show ingest timestamps and row counts per corpus."""
        try:
            df = self.query("""
                SELECT corpus, n_texts,
                       to_timestamp(ingested_at) as ingested_at
                FROM corpus_info
                ORDER BY corpus
            """)
            return df
        except Exception:
            return pd.DataFrame()

    def rebuild(self, corpus_ids=None, progress=True):
        """Drop and re-ingest corpora. If corpus_ids is None, re-ingest all available."""
        if corpus_ids is None:
            from lltk.corpus.utils import load_manifest
            manifest = load_manifest()
            corpus_ids = sorted(set(d.get('id', name) for name, d in manifest.items()))

        from lltk.tools.tools import get_tqdm
        iterr = corpus_ids
        if progress:
            iterr = get_tqdm(corpus_ids, desc='[MetaDB] Ingesting corpora')

        results = {}
        for cid in iterr:
            try:
                n = self.ingest(cid, force=True)
                if n is not None:
                    results[cid] = n
            except Exception as e:
                if log: log(f'Error ingesting {cid}: {e}')
                results[cid] = str(e)
        return results

    def drop(self, corpus_id=None):
        """Drop data for a corpus, or all data if corpus_id is None."""
        if corpus_id:
            self.conn.execute("DELETE FROM texts WHERE corpus = ?", [corpus_id])
            self.conn.execute("DELETE FROM corpus_info WHERE corpus = ?", [corpus_id])
        else:
            self.conn.execute("DROP TABLE IF EXISTS texts")
            self.conn.execute("DROP TABLE IF EXISTS corpus_info")
            self._conn = None  # force reconnect to recreate tables
        self._col_cache = None

    def validate(self, corpus_id=None):
        """
        Report on standard column coverage across ingested corpora.
        Returns a DataFrame showing % non-null for each standard column.
        """
        if corpus_id:
            corpora = [corpus_id]
        else:
            corpora = self.query(
                "SELECT DISTINCT corpus FROM texts ORDER BY corpus"
            )['corpus'].tolist()

        rows = []
        for cid in corpora:
            row = {'corpus': cid}
            n = self.conn.execute(
                "SELECT COUNT(*) FROM texts WHERE corpus = ?", [cid]
            ).fetchone()[0]
            row['n_texts'] = n

            for col in STANDARD_COLS:
                non_null = self.conn.execute(
                    f'SELECT COUNT(*) FROM texts WHERE corpus = ? AND "{col}" IS NOT NULL',
                    [cid]
                ).fetchone()[0]
                row[f'{col}_pct'] = round(non_null / n * 100, 1) if n else 0.0

            rows.append(row)

        report = pd.DataFrame(rows)
        if len(report):
            report = report.set_index('corpus')
        return report

    def validate_genres(self, corpus_id=None, limit=20):
        """
        Show distinct genre values per corpus.
        Flags values not in GENRE_VOCAB.
        Returns a dict of {corpus_id: [(genre_value, count, is_standard), ...]}.
        """
        if corpus_id:
            corpora = [corpus_id]
        else:
            corpora = self.query(
                "SELECT DISTINCT corpus FROM texts ORDER BY corpus"
            )['corpus'].tolist()

        result = {}
        for cid in corpora:
            genres = self.conn.execute(
                f"""SELECT genre, COUNT(*) as n FROM texts
                    WHERE corpus = ? AND genre IS NOT NULL
                    GROUP BY genre ORDER BY n DESC LIMIT {limit}""",
                [cid]
            ).fetchall()
            if genres:
                result[cid] = [
                    (g, n, g in GENRE_VOCAB) for g, n in genres
                ]
        return result

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __del__(self):
        self.close()

    def __repr__(self):
        try:
            n = self.conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
            nc = self.conn.execute("SELECT COUNT(DISTINCT corpus) FROM texts").fetchone()[0]
            return f'MetaDB({n} texts, {nc} corpora, {self.path})'
        except Exception:
            return f'MetaDB({self.path})'


# Module-level singleton
metadb = MetaDB()
