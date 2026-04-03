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
import re
import time
import duckdb
import networkx as nx
import numpy as np
import pandas as pd
from lltk.imports import PATH_LLTK_DATA, log

PATH_METADB = os.path.join(PATH_LLTK_DATA, 'metadb.duckdb')
PATH_MATCHDB = os.path.join(PATH_LLTK_DATA, 'metadb_matches.duckdb')

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
    'Spoken',
    'History',
    'Criticism',
    'Academic',
    'Almanac',
    'Reference',
}

# Corpora excluded from DB ingest (too large, not useful as standalone)
DB_BLACKLIST = {'hathi', 'bighist'}

# Core columns stored as real columns; everything else goes in meta JSON
CORE_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'year', 'genre', 'genre_raw', 'title_norm', 'author_norm']
STANDARD_COLS = ['id', 'title', 'author', 'year', 'genre', 'genre_raw']
TEXT_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'genre', 'genre_raw', 'title_norm', 'author_norm']  # cols stored as TEXT

# Corpus preference ranks for dedup (lower = preferred)
CORPUS_SOURCE_RANKS = {
    'chadwyck': 1, 'chadwyck_drama': 1, 'chadwyck_poetry': 1,
    'canon_fiction': 2, 'markmark': 3, 'chicago': 4,
    'gildedage': 5, 'ravengarside': 5,
    'eebo_tcp': 6, 'ecco_tcp': 6, 'evans_tcp': 6,
    'estc': 7, 'ecco': 8,
    'hathi_englit': 9, 'hathi_novels': 9,
    'hathi_bio': 10, 'hathi_essays': 10, 'hathi_letters': 10,
    'hathi_sermons': 10, 'hathi_stories': 10, 'hathi_tales': 10,
    'hathi_romances': 10, 'hathi_treatises': 10, 'hathi_almanacs': 10,
    'hathi_proclamations': 10,
    'blbooks': 11, 'internet_archive': 12,
}

_TITLE_NORM_PUNCS = re.compile(r'[;:.\(\[,!?]')
_TITLE_END_PHRASES = sorted([
    'edited by', 'written by', 'by the author', 'by mr', 'by mrs',
    'by miss', 'by dr', 'a novel', 'a romance', 'a tale', 'a poem',
    'a tragedy', 'a comedy', 'a farce', 'in two volumes', 'in three volumes',
    'in four volumes', 'the second edition', 'the third edition',
    'the fourth edition', 'a new edition', 'translated from',
    'translated by', 'with a preface', 'with an introduction',
], key=len, reverse=True)


def normalize_title(title):
    """Normalize a title for matching: lowercase, strip subtitle/edition info."""
    if not title or not isinstance(title, str) or title == 'nan':
        return None
    t = title.strip().lower()
    t = t.replace('\u2014', '--').replace('\u2013', '-')
    # Strip after first punctuation
    m = _TITLE_NORM_PUNCS.search(t)
    if m:
        t = t[:m.start()].strip()
    else:
        # Try title-end phrases
        tl = t.lower()
        for phrase in _TITLE_END_PHRASES:
            idx = tl.find(phrase)
            if idx > 3:
                t = t[:idx].strip()
                break
    t = ' '.join(t.split())  # collapse whitespace
    return t if len(t) > 1 else None


def _jaro_winkler(s1, s2):
    """Fast Jaro-Winkler similarity. Returns float 0-1."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    try:
        from rapidfuzz.distance import JaroWinkler
        return JaroWinkler.similarity(s1, s2)
    except ImportError:
        pass
    # Fallback: simple Jaro
    len1, len2 = len(s1), len(s2)
    search_range = max(len1, len2) // 2 - 1
    if search_range < 0:
        search_range = 0
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0
    for i in range(len1):
        lo = max(0, i - search_range)
        hi = min(i + search_range + 1, len2)
        for j in range(lo, hi):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    jaro = (matches / len1 + matches / len2 + (matches - transpositions / 2) / matches) / 3
    # Winkler prefix bonus
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


def normalize_author(author):
    """Normalize an author name: lowercase last name before first comma."""
    if not author or not isinstance(author, str) or author == 'nan':
        return None
    a = author.strip().lower()
    # Take text before first comma (last name)
    if ',' in a:
        a = a.split(',')[0].strip()
    # Remove trailing periods
    a = a.rstrip('.')
    a = ' '.join(a.split())
    return a if len(a) > 1 else None


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
    def __init__(self, path=None, match_path=None):
        self.path = path or PATH_METADB
        self.match_path = match_path or PATH_MATCHDB
        self._conn = None
        self._col_cache = None

    @property
    def conn(self):
        """Single connection to texts DB, with matches DB attached as match_db."""
        if self._conn is None:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._conn = duckdb.connect(self.path)
            self._ensure_text_tables()
            # Attach matches DB
            try:
                os.makedirs(os.path.dirname(self.match_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.match_path}' AS match_db")
            except Exception:
                pass  # already attached
            self._ensure_match_tables()
        return self._conn

    @property
    def match_conn(self):
        """Alias for conn — matches are in attached match_db."""
        return self.conn

    def _ensure_text_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS texts (
                _id         TEXT PRIMARY KEY,
                corpus      TEXT NOT NULL,
                id          TEXT NOT NULL,
                title       TEXT,
                author      TEXT,
                year        INTEGER,
                genre       TEXT,
                genre_raw   TEXT,
                title_norm  TEXT,
                author_norm TEXT,
                meta        TEXT
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_corpus ON texts(corpus)")
        # Add columns if upgrading from older schema
        for col in ('title_norm', 'author_norm'):
            try:
                self._conn.execute(f"ALTER TABLE texts ADD COLUMN {col} TEXT")
            except Exception:
                pass
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_title_norm ON texts(title_norm)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_author_norm ON texts(author_norm)")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS corpus_info (
                corpus      TEXT PRIMARY KEY,
                ingested_at DOUBLE NOT NULL,
                n_texts     INTEGER NOT NULL
            )
        """)

    def _ensure_match_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS match_db.matches (
                _id_a      TEXT NOT NULL,
                _id_b      TEXT NOT NULL,
                similarity FLOAT NOT NULL,
                match_type TEXT NOT NULL,
                PRIMARY KEY (_id_a, _id_b)
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_a ON match_db.matches(_id_a)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_matches_b ON match_db.matches(_id_b)")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS match_db.match_groups (
                _id      TEXT PRIMARY KEY,
                group_id INTEGER NOT NULL,
                rank     INTEGER NOT NULL
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_mg_gid ON match_db.match_groups(group_id)")

    def ingest(self, corpus_id, force=True):
        """Ingest a corpus's load_metadata() output into the DB."""
        if corpus_id in DB_BLACKLIST:
            if log: log(f'Skipping {corpus_id} (in DB_BLACKLIST)')
            return None
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

        # Compute normalized title and author for matching
        df['title_norm'] = df['title'].apply(normalize_title) if 'title' in df.columns else None
        df['author_norm'] = df['author'].apply(normalize_author) if 'author' in df.columns else None

        # Select only core cols + meta for insert
        insert_cols = CORE_COLS + ['meta']
        insert_df = df[insert_cols].copy()

        # Remove old data for this corpus
        if force:
            self.conn.execute("DELETE FROM texts WHERE corpus = ?", [corpus_id])

        # Insert with explicit column mapping
        cols_str = ', '.join(f'"{c}"' for c in insert_cols)
        self.conn.execute(
            f"INSERT INTO texts ({cols_str}) SELECT {cols_str} FROM insert_df",
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
            # Clean up matches involving this corpus
            try:
                self.match_conn.execute("""
                    DELETE FROM match_db.matches WHERE _id_a LIKE ? OR _id_b LIKE ?
                """, [f'_{corpus_id}/%', f'_{corpus_id}/%'])
            except Exception:
                pass
        else:
            self.conn.execute("DROP TABLE IF EXISTS texts")
            self.conn.execute("DROP TABLE IF EXISTS corpus_info")
            try:
                self.conn.execute("DROP TABLE IF EXISTS match_db.matches")
                self.conn.execute("DROP TABLE IF EXISTS match_db.match_groups")
            except Exception:
                pass
            self._conn = None
        self._col_cache = None

    def drop_matches(self):
        """Drop all matches and match groups (leaves texts intact)."""
        try:
            self.match_conn.execute("DELETE FROM match_db.matches")
            self.match_conn.execute("DELETE FROM match_db.match_groups")
        except Exception:
            pass

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

    # ── Dedup & Matching ─────────────────────────────────────────────

    def match(self, corpora=None, fuzzy=False, progress=True):
        """
        Find matching texts — both within and across corpora.
        Single SQL join on author_norm + title_norm. Handles dedup and
        cross-corpus matching in one pass.

        Args:
            corpora: list of corpus IDs to include (default: all).
            fuzzy: if True, also run fuzzy title matching within author blocks (slow).
        """
        from lltk.tools.tools import get_tqdm

        corpus_filter = ''
        corpus_where = ''
        if corpora:
            corpus_list = ', '.join(f"'{c}'" for c in corpora)
            corpus_filter = f'AND a.corpus IN ({corpus_list}) AND b.corpus IN ({corpus_list})'
            corpus_where = f'AND corpus IN ({corpus_list})'
            print(f'Matching {len(corpora)} corpora: {", ".join(corpora)}')

        # Exact: same title_norm + author_norm, any corpus (within or across)
        # Uses chain linking (each text → next in sorted order) instead of all-pairs
        # to avoid quadratic explosion on multi-volume works (N-1 edges, not N*(N-1)/2)
        print('Exact title + author matching...')
        self.match_conn.execute(f"""
            INSERT OR IGNORE INTO match_db.matches (_id_a, _id_b, similarity, match_type)
            SELECT a._id, b._id, 1.0, 'exact_norm'
            FROM (
                SELECT _id, title_norm, author_norm,
                       LEAD(_id) OVER (PARTITION BY title_norm, author_norm ORDER BY _id) as next_id
                FROM texts
                WHERE title_norm IS NOT NULL
                  AND author_norm IS NOT NULL
                  AND length(title_norm) > 5
            ) a
            JOIN texts b ON a.next_id = b._id
            WHERE a.next_id IS NOT NULL
              {corpus_filter}
        """)
        count_exact = self.match_conn.execute("SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'exact_norm'").fetchone()[0]
        print(f'  Exact: {count_exact} pairs')

        if fuzzy:
            print('Fuzzy title matching within author blocks...')
            # Cap at 200 texts per block — larger blocks are surname collisions
            # (different Smiths, Wards, etc.) not worth fuzzy matching
            authors = self.match_conn.execute(f"""
                SELECT author_norm, COUNT(*) as n
                FROM texts
                WHERE author_norm IS NOT NULL AND length(title_norm) > 3
                {corpus_where}
                GROUP BY author_norm
                HAVING n > 1 AND n <= 200
            """).fetchall()

            iterr = authors
            if progress:
                iterr = get_tqdm(authors, desc='[MetaDB] Fuzzy matching by author')

            batch = []
            for author_norm, _ in iterr:
                rows = self.match_conn.execute(f"""
                    SELECT _id, title_norm, corpus, year FROM texts
                    WHERE author_norm = ? AND title_norm IS NOT NULL AND length(title_norm) > 3
                    {corpus_where}
                """, [author_norm]).fetchall()

                for i in range(len(rows)):
                    for j in range(i + 1, len(rows)):
                        a_id, a_title, a_corp, a_year = rows[i]
                        b_id, b_title, b_corp, b_year = rows[j]
                        if a_corp == b_corp:
                            continue
                        if a_year and b_year and abs(a_year - b_year) > 20:
                            continue
                        # Compute similarity in Python (faster than per-pair SQL)
                        sim = _jaro_winkler(a_title, b_title)
                        if sim > 0.85:
                            pair = (a_id, b_id) if a_id < b_id else (b_id, a_id)
                            batch.append((*pair, sim, 'fuzzy_title'))

                if len(batch) >= 10000:
                    self._insert_matches_batch(batch)
                    batch = []

            if batch:
                self._insert_matches_batch(batch)

            count_fuzzy = self.match_conn.execute("SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'fuzzy_title'").fetchone()[0]
            print(f'  Fuzzy: {count_fuzzy} pairs')

        # Compute match groups
        print('Computing match groups...')
        self._compute_match_groups()
        total = self.match_conn.execute("SELECT COUNT(*) FROM match_db.matches").fetchone()[0]
        groups = self.match_conn.execute("SELECT COUNT(DISTINCT group_id) FROM match_db.match_groups").fetchone()[0]
        n_texts = self.match_conn.execute("SELECT COUNT(*) FROM match_db.match_groups").fetchone()[0]
        print(f'Done: {total} match pairs, {n_texts} texts in {groups} groups')

    def _insert_matches_batch(self, batch):
        """Insert a batch of match tuples, ignoring duplicates."""
        if not batch:
            return
        df = pd.DataFrame(batch, columns=['_id_a', '_id_b', 'similarity', 'match_type'])
        df = df.drop_duplicates(subset=['_id_a', '_id_b'])
        self.match_conn.execute("""
            INSERT OR IGNORE INTO match_db.matches (_id_a, _id_b, similarity, match_type)
            SELECT * FROM df
        """)

    def _compute_match_groups(self):
        """Build connected components from matches, store in match_groups."""
        pairs = self.match_conn.execute("SELECT _id_a, _id_b FROM match_db.matches").fetchall()
        if not pairs:
            self.match_conn.execute("DELETE FROM match_db.match_groups")
            return

        G = nx.Graph()
        G.add_edges_from(pairs)

        rows = []
        for gid, component in enumerate(nx.connected_components(G)):
            ranked = sorted(component,
                key=lambda x: CORPUS_SOURCE_RANKS.get(
                    x.split('/')[0].lstrip('_'), 1000))
            for rank, _id in enumerate(ranked):
                rows.append((_id, gid, rank))

        df = pd.DataFrame(rows, columns=['_id', 'group_id', 'rank'])
        self.match_conn.execute("DELETE FROM match_db.match_groups")
        self.match_conn.execute("INSERT INTO match_db.match_groups SELECT * FROM df")

    def find_matches(self, query):
        """Search for matches by title substring. Returns DataFrame with match groups."""
        return self.match_conn.execute("""
            SELECT t._id, t.corpus, t.title, t.author, t.year, t.genre,
                   t.title_norm, mg.group_id, mg.rank
            FROM texts t
            JOIN match_db.match_groups mg ON t._id = mg._id
            WHERE mg.group_id IN (
                SELECT mg2.group_id FROM match_db.match_groups mg2
                JOIN texts t2 ON mg2._id = t2._id
                WHERE t2.title ILIKE ?
            )
            ORDER BY mg.group_id, mg.rank
        """, [f'%{query}%']).fetchdf()

    def get_group(self, _id):
        """Get all texts in the same match group as the given _id."""
        return self.match_conn.execute("""
            SELECT t._id, t.corpus, t.title, t.author, t.year, t.genre,
                   mg.group_id, mg.rank
            FROM texts t
            JOIN match_db.match_groups mg ON t._id = mg._id
            WHERE mg.group_id = (
                SELECT group_id FROM match_db.match_groups WHERE _id = ?
            )
            ORDER BY mg.rank
        """, [_id]).fetchdf()

    def match_stats(self):
        """Summary statistics for matches."""
        total = self.match_conn.execute("SELECT COUNT(*) FROM match_db.matches").fetchone()[0]
        groups = self.match_conn.execute("SELECT COUNT(DISTINCT group_id) FROM match_db.match_groups").fetchone()[0]
        by_type = self.match_conn.execute("""
            SELECT match_type, COUNT(*) as n FROM match_db.matches GROUP BY match_type
        """).fetchdf()
        # Group size distribution
        sizes = self.match_conn.execute("""
            SELECT group_size, COUNT(*) as n_groups FROM (
                SELECT group_id, COUNT(*) as group_size FROM match_db.match_groups GROUP BY group_id
            ) GROUP BY group_size ORDER BY group_size
        """).fetchdf()
        return {
            'total_matches': total,
            'total_groups': groups,
            'by_type': by_type,
            'group_sizes': sizes,
        }

    # ── Query API ──────────────────────────────────────────────────

    def _build_where(self, where=None, genre=None, year_min=None, year_max=None, corpora=None, sources=None):
        """Build a WHERE clause from keyword filters. Returns (sql_fragment, params).
        Uses string interpolation (not ?) for values since the clause gets reused in subqueries."""
        clauses = []

        if where:
            clauses.append(f'({where})')

        if genre:
            clauses.append(f"t.genre = '{genre}'")

        if year_min is not None:
            clauses.append(f't.year >= {int(year_min)}')

        if year_max is not None:
            clauses.append(f't.year <= {int(year_max)}')

        if corpora:
            corpus_list = ', '.join(f"'{c}'" for c in corpora)
            clauses.append(f't.corpus IN ({corpus_list})')

        if sources:
            source_clauses = []
            for corpus_id, filters in sources.items():
                parts = [f"t.corpus = '{corpus_id}'"]
                for k, v in filters.items():
                    parts.append(f"t.{k} = '{v}'")
                source_clauses.append('(' + ' AND '.join(parts) + ')')
            clauses.append('(' + ' OR '.join(source_clauses) + ')')

        sql = ' AND '.join(clauses) if clauses else '1=1'
        return sql

    def _dedup_sql(self, where_sql, dedup_by='rank', texts_table='texts'):
        """Return SQL fragment that keeps only one representative per match group."""
        if dedup_by == 'oldest':
            return f"""
                AND (
                    mg._id IS NULL
                    OR t._id = (
                        SELECT mg2._id FROM match_db.match_groups mg2
                        JOIN {texts_table} t2 ON mg2._id = t2._id
                        WHERE mg2.group_id = mg.group_id
                          AND {where_sql.replace('t.', 't2.')}
                        ORDER BY t2.year NULLS LAST, mg2.rank
                        LIMIT 1
                    )
                )
            """
        else:  # rank
            return f"""
                AND (
                    mg._id IS NULL
                    OR mg.rank = (
                        SELECT MIN(mg2.rank) FROM match_db.match_groups mg2
                        JOIN {texts_table} t2 ON mg2._id = t2._id
                        WHERE mg2.group_id = mg.group_id
                          AND {where_sql.replace('t.', 't2.')}
                    )
                )
            """

    def texts(self, where=None, *, genre=None, year_min=None, year_max=None,
              corpora=None, sources=None, dedup=True, dedup_by='rank', progress=False):
        """
        Query texts and return real text objects.

        Args:
            where: raw SQL WHERE clause fragment
            genre, year_min, year_max, corpora: convenience filters
            sources: dict of {corpus_id: {filter_key: value}} for SyntheticCorpus
            dedup: if True, keep one representative per match group
            dedup_by: 'rank' (CORPUS_SOURCE_RANKS) or 'oldest' (earliest year)
            progress: show progress bar

        Yields:
            BaseText objects with their original corpus for file access
        """
        from lltk.corpus.corpus import Corpus
        from lltk.tools.tools import get_tqdm

        where_sql = self._build_where(
            where=where, genre=genre, year_min=year_min, year_max=year_max,
            corpora=corpora, sources=sources
        )

        if dedup:
            # Query through match_conn (has match_groups + attached texts_db)
            dedup_sql = self._dedup_sql(where_sql, dedup_by, texts_table='texts')
            sql = f"""
                SELECT t.corpus, t.id FROM texts t
                LEFT JOIN match_db.match_groups mg ON t._id = mg._id
                WHERE {where_sql} {dedup_sql}
                ORDER BY t.year, t.corpus, t.id
            """
            rows = self.conn.execute(sql).fetchall()
        else:
            sql = f"""
                SELECT t.corpus, t.id FROM texts t
                WHERE {where_sql}
                ORDER BY t.year, t.corpus, t.id
            """
            rows = self.conn.execute(sql).fetchall()

        iterr = rows
        if progress:
            iterr = get_tqdm(rows, desc='[MetaDB] Loading texts')

        for corpus_id, text_id in iterr:
            try:
                corpus_obj = Corpus(corpus_id)
                t = corpus_obj.text(text_id)
                yield t
            except Exception:
                continue

    def texts_df(self, where=None, *, genre=None, year_min=None, year_max=None,
                 corpora=None, sources=None, dedup=True, dedup_by='rank'):
        """Like texts() but returns a DataFrame instead of text objects."""
        where_sql = self._build_where(
            where=where, genre=genre, year_min=year_min, year_max=year_max,
            corpora=corpora, sources=sources
        )
        if dedup:
            dedup_sql = self._dedup_sql(where_sql, dedup_by, texts_table='texts')
            sql = f"""
                SELECT t.* FROM texts t
                LEFT JOIN match_db.match_groups mg ON t._id = mg._id
                WHERE {where_sql} {dedup_sql}
                ORDER BY t.year, t.corpus, t.id
            """
            return self.match_conn.execute(sql).fetchdf()
        else:
            sql = f"""
                SELECT t.* FROM texts t
                WHERE {where_sql}
                ORDER BY t.year, t.corpus, t.id
            """
            return self.conn.execute(sql).fetchdf()

    def corpus(self, where=None, id='_query', **kwargs):
        """Return a SyntheticCorpus from a MetaDB query."""
        from lltk.corpus.synthetic import SyntheticCorpus
        return SyntheticCorpus(id=id, _query_kwargs={'where': where, **kwargs})

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
