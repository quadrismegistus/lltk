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
PATH_WORDCOUNTDB = os.path.join(PATH_LLTK_DATA, 'metadb_wordcounts.duckdb')
PATH_WORDINDEXDB = os.path.join(PATH_LLTK_DATA, 'metadb_wordindex.duckdb')

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
CORE_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'year', 'genre', 'genre_raw', 'is_translated', 'title_norm', 'author_norm', 'path_freqs']
STANDARD_COLS = ['id', 'title', 'author', 'year', 'genre', 'genre_raw']
TEXT_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'genre', 'genre_raw', 'title_norm', 'author_norm', 'path_freqs']  # cols stored as TEXT (not is_translated — that's BOOLEAN)

# Genre authority corpora — metadata-only corpora whose genre labels propagate
# to digitized corpora via match groups. Higher priority = trusted more.
GENRE_AUTHORITY_CORPORA = {
    'fiction_biblio': 10,
    'end': 10,
    'ravengarside': 10,
}

# Genre source priority (higher = more trusted). Used to resolve conflicts.
GENRE_SOURCE_PRIORITY = {
    'bibliography': 50,   # fiction_biblio, end, ravengarside
    'form': 30,           # ESTC 655$a cataloger-assigned
    'topic': 20,          # ESTC 650$a cataloger-assigned
    'title': 5,           # ESTC title keyword heuristic
    'corpus': 10,         # corpus's own genre (non-ESTC corpora)
}

# Corpus preference ranks for dedup (lower = preferred)
CORPUS_SOURCE_RANKS = {
    'chadwyck': 1, 'chadwyck_drama': 1, 'chadwyck_poetry': 1,
    'earlyprint': 2,
    'eebo_tcp': 3, 'ecco_tcp': 3, 'evans_tcp': 3,
    'markmark': 3, 'chicago': 3, 'clmet': 3,
    'gildedage': 4, 'coca': 4, 'coha': 4, 'sellers': 4, 'new_yorker': 4, 'spectator': 4, 
    'tedjdh': 5, 'long_arc_prestige': 5,
    'hathi_englit': 5, 'hathi_novels': 5, 'hathi_romances': 5, 'hathi_treatises': 5, 'hathi_almanacs': 5, 'hathi_essays': 5, 'hathi_letters': 5, 'hathi_sermons': 5, 'hathi_stories': 5, 'hathi_tales': 5, 'hathi_proclamations': 5, 'hathi_bio': 5,
    'ecco': 6, 'bpo': 6, 'litlab': 6, 'pmla': 6, 'sotu': 6, 'gale_amfic': 6,
    'internet_archive': 7,
    'blbooks': 8,
    'canon_fiction': 9, 'dialogues':9, 'fanfic':9,
    'ravengarside': 9, 'estc': 10, 'semantic_cohort': 10,
    'dta': 11, 'dialnarr': 11, 'txtlab': 11, 'hathi':11, 'oldbailey':11, 'epistolary':11,
    'test_fixture': 100, 'test_fixture_linked': 100,
    'arc_fiction': 101, 'arc_poetry': 101, 'arc_periodical': 101, 'tmp':101,
}

_TITLE_NORM_PUNCS = re.compile(r'[;:.\(\[,!?]')  # period safe now — abbreviation periods already stripped
_TITLE_END_PHRASES = sorted([
    'edited by', 'written by', 'by the author', 'by mr', 'by mrs',
    'by miss', 'by dr', 'a novel', 'a romance', 'a tale', 'a poem',
    'a tragedy', 'a comedy', 'a farce', 'in two volumes', 'in three volumes',
    'in four volumes', 'the second edition', 'the third edition',
    'the fourth edition', 'a new edition', 'translated from',
    'translated by', 'with a preface', 'with an introduction',
], key=len, reverse=True)

# Lazy-loaded spelling modernizer for title normalization
_spelling_modernizer = None

def _get_spelling_modernizer():
    """Load MorphAdorner spelling modernizer (cached after first call)."""
    global _spelling_modernizer
    if _spelling_modernizer is not None:
        return _spelling_modernizer
    try:
        from lltk.imports import PATH_LLTK_HOME, PATH_TO_ENGLISH_SPELLING_MODERNIZER
        path = PATH_TO_ENGLISH_SPELLING_MODERNIZER
        if not os.path.isabs(path):
            path = os.path.join(PATH_LLTK_HOME, path)
        if os.path.exists(path):
            import gzip
            d = {}
            with gzip.open(path, 'rt') as f:
                for ln in f:
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        old, new = ln.split('\t')
                        d[old] = new
                    except ValueError:
                        continue
            _spelling_modernizer = d
        else:
            _spelling_modernizer = {}
    except Exception:
        _spelling_modernizer = {}
    return _spelling_modernizer


def normalize_title(title):
    """Normalize a title for matching: modernize spelling, lowercase, strip subtitle/edition info."""
    if not title or not isinstance(title, str) or title == 'nan':
        return None
    t = title.strip().lower()

    # Unescape HTML entities (e.g. &hyphen; → -, &amp; → &)
    import html
    t = html.unescape(t)

    # Normalize all dash/hyphen variants to ASCII hyphen
    t = re.sub(r'[\u2010\u2011\u2012\u2013\u2014\u2015\u2212\uFE58\uFE63\uFF0D]', '-', t)
    t = t.replace('--', ' ')

    # Strip brackets: [Love-Letters...] → Love-Letters...
    t = re.sub(r'[\[\]]', '', t).strip()

    # Strip periods after known abbreviations only (Mr. Mrs. Dr. St. Q. K. E. etc.)
    # Keeps subtitle-separating periods intact: "The life of X. Being a history"
    _ABBREV_PREFIXES = {
        'mr', 'mrs', 'ms', 'dr', 'st', 'sr', 'jr', 'esq', 'rev', 'gen', 'col', 'capt', 'maj', 'sgt',
        'vol', 'pt', 'no', 'ed', 'edn',
    }
    # Also strip period after any single letter (Q., E., K., A., etc.)
    t = re.sub(r'\b([a-z])\.\s', r'\1 ', t)
    for abbr in _ABBREV_PREFIXES:
        t = re.sub(r'\b' + abbr + r'\.\s', abbr + ' ', t)
        t = re.sub(r'\b' + abbr + r'\.$', abbr, t)

    # Modernize early modern spelling (u/v, vv/w, i/j, etc.)
    mod = _get_spelling_modernizer()
    if mod:
        t = ' '.join(mod.get(w, w) for w in t.split())

    # Split on first subtitle punctuation: ; : . ( [ , ! ?
    # Abbreviation periods already stripped above, so remaining periods are subtitle separators
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

    # Strip trailing period and whitespace
    t = t.rstrip('. ')
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


def normalize_genre_raw(val):
    """Normalize a genre_raw value: harmonize codes, synonyms, and pipe-separated compounds.

    Applied at ingest time so all downstream consumers (matching, enrichment,
    arc corpora) see clean values.
    """
    if not val or not isinstance(val, str) or val in ('nan', 'None', ''):
        return None

    # ── Step 1: Code-to-label mappings (COCA/COHA, lowercase variants) ──
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

    # ── Step 2: Split pipe-separated terms, normalize each ──
    # Two separators in use: " | " (ESTC) and "|" (litlab)
    if '|' in stripped:
        parts = [p.strip() for p in re.split(r'\s*\|\s*', stripped) if p.strip()]
    else:
        parts = [stripped]

    # Normalize individual terms
    EPISTOLARY = {'Epistolary fiction', 'Epistolary', 'Epistolary novel'}
    normalized = []
    for p in parts:
        if p in CODE_MAP:
            p = CODE_MAP[p]
        if p in EPISTOLARY:
            p = 'Novel, epistolary'
        # Capitalize first letter
        if p and p[0].islower() and p not in CODE_MAP:
            p = p[0].upper() + p[1:]
        normalized.append(p)

    # Deduplicate after normalization
    seen = set()
    deduped = []
    for p in normalized:
        if p not in seen:
            seen.add(p)
            deduped.append(p)

    # ── Step 3: Remove generic terms when more specific ones present ──
    FICTION_SUBGENRES = {
        'Novel', 'Novel, epistolary', 'Romance', 'Tale', 'Fable', 'Novella',
        'Picaresque', 'Gothic', 'Imaginary voyage', 'Silver Fork',
        'Bildungsroman', 'New Woman', 'Rogue fiction', 'Chapbook',
        'It Narrative', 'Utopia', 'Jestbook',
    }
    # Novel subtypes that make plain "Novel" redundant
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
        has_specific_fiction = any(p in FICTION_SUBGENRES for p in deduped)
        if has_specific_fiction:
            deduped = [p for p in deduped if p != 'Fiction']
        # Novel is redundant when a more specific Novel subtype is present
        has_novel_subtype = any(p in NOVEL_SUBTYPES for p in deduped)
        if has_novel_subtype:
            deduped = [p for p in deduped if p != 'Novel']
        # Letter is redundant with Novel, epistolary
        if 'Novel, epistolary' in deduped:
            deduped = [p for p in deduped if p != 'Letter']

    if not deduped:
        return None

    return ' | '.join(deduped)


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
    def __init__(self, path=None, match_path=None, wordcount_path=None, wordindex_path=None):
        self.path = path or PATH_METADB
        self.match_path = match_path or PATH_MATCHDB
        self.wordcount_path = wordcount_path or PATH_WORDCOUNTDB
        self.wordindex_path = wordindex_path or PATH_WORDINDEXDB
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
            # Attach wordcounts DB
            try:
                os.makedirs(os.path.dirname(self.wordcount_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.wordcount_path}' AS wc_db")
            except Exception:
                pass  # already attached
            self._ensure_wordcount_tables()
            # Attach word index DB
            try:
                os.makedirs(os.path.dirname(self.wordindex_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.wordindex_path}' AS wi_db")
            except Exception:
                pass  # already attached
            self._ensure_wordindex_tables()
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
                is_translated BOOLEAN,
                title_norm  TEXT,
                author_norm TEXT,
                meta        TEXT
            )
        """)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_corpus ON texts(corpus)")
        # Add columns if upgrading from older schema
        for col, dtype in [('title_norm', 'TEXT'), ('author_norm', 'TEXT'), ('is_translated', 'BOOLEAN'), ('path_freqs', 'TEXT'),
                           ('genre_enriched_source', 'TEXT'), ('genre_corpus', 'TEXT')]:
            try:
                self._conn.execute(f"ALTER TABLE texts ADD COLUMN {col} {dtype}")
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

    def _ensure_wordcount_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS wc_db.wordcounts (
                path_freqs  TEXT PRIMARY KEY,
                n_words     INTEGER NOT NULL
            )
        """)
        # Add n_words column to texts table if not present
        try:
            self._conn.execute("ALTER TABLE texts ADD COLUMN n_words INTEGER")
        except Exception:
            pass

    def _ensure_wordindex_tables(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS wi_db.word_index (
                _id    TEXT NOT NULL,
                word   TEXT NOT NULL,
                count  INTEGER NOT NULL
            )
        """)

    def _ensure_wordindex_indexes(self):
        """Create indexes on word_index (call after bulk load)."""
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_wi_word ON wi_db.word_index(word)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_wi_id ON wi_db.word_index(_id)")

    def ingest(self, corpus_id, force=True):
        """Ingest a corpus's load_metadata() output into the DB."""
        if corpus_id in DB_BLACKLIST:
            if log: log(f'Skipping {corpus_id} (in DB_BLACKLIST)')
            return None
        from lltk.corpus.utils import load
        from lltk.corpus.synthetic import SyntheticCorpus
        try:
            corpus = load(corpus_id)
        except Exception as e:
            if log: log(f'Could not load corpus {corpus_id}: {e}')
            return None
        if isinstance(corpus, SyntheticCorpus):
            if log: log(f'Skipping {corpus_id} (SyntheticCorpus)')
            return None

        try:
            df = corpus.load_metadata()
        except Exception as e:
            if log: log(f'Could not load metadata for {corpus_id}: {e}')
            return None

        if df is None or not len(df):
            if log: log(f'No metadata for {corpus_id}')
            return None

        # Resolve freqs paths (relative to PATH_CORPUS)
        df = self._resolve_freqs_paths(df, corpus)

        return self.ingest_df(df, corpus_id, force=force)

    def _resolve_freqs_paths(self, df, corpus):
        """Add path_freqs column with paths relative to PATH_CORPUS."""
        from lltk.imports import PATH_CORPUS
        corpus_root = os.path.expanduser(PATH_CORPUS)

        # Get the corpus's freqs directory
        freqs_dir = getattr(corpus, 'path_freqs', None)
        if not freqs_dir or not os.path.isdir(freqs_dir):
            df['path_freqs'] = None
            return df

        # Check for corpus-specific freqs_path_for method (e.g. Hathi)
        has_custom = hasattr(corpus, 'freqs_path_for')

        if has_custom:
            # Use corpus's own resolution (handles ID normalization, nested dirs)
            ids = df.index if df.index.name == 'id' else df.get('id', pd.Series())
            paths = []
            for text_id in ids:
                try:
                    abs_path = corpus.freqs_path_for(str(text_id))
                    if abs_path and os.path.exists(abs_path):
                        paths.append(os.path.relpath(abs_path, corpus_root))
                    else:
                        paths.append(None)
                except Exception:
                    paths.append(None)
            df['path_freqs'] = paths
        else:
            # Standard pattern: {freqs_dir}/{text_id}.json
            ext = getattr(corpus, 'EXT_FREQS', getattr(corpus, 'ext_freqs', '.json'))
            ids = df.index if df.index.name == 'id' else df.get('id', pd.Series())
            paths = []
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
                try:
                    is_valid = pd.notna(v) and str(v) not in ('', 'nan', 'None', '[]')
                except ValueError:
                    # v is array-like (e.g. numpy array) — convert to string if non-empty
                    is_valid = len(v) > 0
                if is_valid:
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

        # Ensure genre/genre_raw/is_translated columns exist
        for col in ('genre', 'genre_raw'):
            if col not in df.columns:
                df[col] = None
        if 'is_translated' not in df.columns:
            df['is_translated'] = None
        else:
            # Handle string booleans from CSVs and Python bools in object columns
            col = df['is_translated']
            if col.dtype == object:
                col = col.map({
                    'True': True, 'False': False, 'true': True, 'false': False,
                    True: True, False: False,
                })
            df['is_translated'] = col.astype('boolean')

        # Normalize genre_raw
        if 'genre_raw' in df.columns:
            df['genre_raw'] = df['genre_raw'].apply(normalize_genre_raw)

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

        # Backfill n_words from wordcount cache
        self.conn.execute("""
            UPDATE texts SET n_words = wc.n_words
            FROM wc_db.wordcounts wc
            WHERE texts.path_freqs = wc.path_freqs
              AND texts.n_words IS NULL
              AND texts.corpus = ?
        """, [corpus_id])

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
            if progress:
                iterr.set_description(f'[MetaDB] Ingesting {cid}')
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

    def match(self, corpora=None, fuzzy=False, containment=True, progress=True):
        """
        Find matching texts — both within and across corpora.
        Single SQL join on author_norm + title_norm. Handles dedup and
        cross-corpus matching in one pass.

        Args:
            corpora: list of corpus IDs to include (default: all).
            containment: if True (default), match when short title is substring of long title.
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

        # Tier 0: ID-based matching from corpus LINKS declarations
        # e.g. earlyprint.id_tcp = eebo_tcp.id, ecco.ESTCID = estc.id_estc
        print('ID-based linking from corpus LINKS...')
        self._match_by_links(corpora)
        id_link_total = self.match_conn.execute(
            "SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'id_link'"
        ).fetchone()[0]
        print(f'  ID links: {id_link_total} pairs')

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
        print(f'  Exact (by author): {count_exact} pairs')

        # Exact: same title_norm + year, authorless texts only
        # Stricter title length (>10) to compensate for weaker constraint
        print('Exact title + year matching (authorless)...')
        self.match_conn.execute(f"""
            INSERT OR IGNORE INTO match_db.matches (_id_a, _id_b, similarity, match_type)
            SELECT a._id, b._id, 1.0, 'exact_norm_year'
            FROM (
                SELECT _id, title_norm, year,
                       LEAD(_id) OVER (PARTITION BY title_norm, year ORDER BY _id) as next_id
                FROM texts
                WHERE title_norm IS NOT NULL
                  AND author_norm IS NULL
                  AND year IS NOT NULL
                  AND length(title_norm) > 10
            ) a
            JOIN texts b ON a.next_id = b._id
            WHERE a.next_id IS NOT NULL
              {corpus_filter}
        """)
        count_exact_year = self.match_conn.execute("SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'exact_norm_year'").fetchone()[0]
        print(f'  Exact (by year, authorless): {count_exact_year} pairs')

        if containment:
            print('Containment title matching...')
            batch = []
            existing_pairs = set()
            for row in self.match_conn.execute("SELECT _id_a, _id_b FROM match_db.matches").fetchall():
                existing_pairs.add((row[0], row[1]))

            def _check_containment(rows, match_type='containment', min_short=8, min_sim=0.3):
                """Check all cross-corpus pairs in a block for title containment.

                min_sim is the minimum ratio of len(short)/len(long) to accept.
                Filters out generic short fragments like 'the life' matching long titles.
                """
                for i in range(len(rows)):
                    for j in range(i + 1, len(rows)):
                        a_id, a_title, a_corp = rows[i][0], rows[i][1], rows[i][2]
                        b_id, b_title, b_corp = rows[j][0], rows[j][1], rows[j][2]
                        if a_corp == b_corp:
                            continue
                        pair = (a_id, b_id) if a_id < b_id else (b_id, a_id)
                        if pair in existing_pairs:
                            continue
                        short, long = (a_title, b_title) if len(a_title) <= len(b_title) else (b_title, a_title)
                        if len(short) < min_short:
                            continue
                        if short in long:
                            sim = len(short) / len(long)
                            if sim < min_sim:
                                continue
                            batch.append((*pair, sim, match_type))
                            existing_pairs.add(pair)

            # (a) Within author blocks
            authors = self.match_conn.execute(f"""
                SELECT author_norm, COUNT(*) as n
                FROM texts
                WHERE author_norm IS NOT NULL AND title_norm IS NOT NULL
                  AND length(title_norm) > 3
                  {corpus_where}
                GROUP BY author_norm
                HAVING n > 1 AND n <= 500
            """).fetchall()

            iterr = authors
            if progress:
                iterr = get_tqdm(authors, desc='[MetaDB] Containment by author')

            for author_norm, _ in iterr:
                rows = self.match_conn.execute(f"""
                    SELECT _id, title_norm, corpus FROM texts
                    WHERE author_norm = ? AND title_norm IS NOT NULL
                      AND length(title_norm) > 3
                      {corpus_where}
                """, [author_norm]).fetchall()
                _check_containment(rows)
                if len(batch) >= 10000:
                    self._insert_matches_batch(batch)
                    batch = []

            if batch:
                self._insert_matches_batch(batch)
                batch = []

            count_by_author = self.match_conn.execute(
                "SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'containment'"
            ).fetchone()[0]
            print(f'  By author: {count_by_author} pairs')

            # (b) Authorless texts: match by year + title containment
            # Stricter: min 15 chars for short title to avoid false positives
            years = self.match_conn.execute(f"""
                SELECT year, COUNT(*) as n
                FROM texts
                WHERE author_norm IS NULL AND title_norm IS NOT NULL
                  AND year IS NOT NULL AND length(title_norm) > 5
                  {corpus_where}
                GROUP BY year
                HAVING n > 1 AND n <= 500
            """).fetchall()

            iterr = years
            if progress:
                iterr = get_tqdm(years, desc='[MetaDB] Containment by year (authorless)')

            for year, _ in iterr:
                rows = self.match_conn.execute(f"""
                    SELECT _id, title_norm, corpus FROM texts
                    WHERE author_norm IS NULL AND title_norm IS NOT NULL
                      AND year = ? AND length(title_norm) > 5
                      {corpus_where}
                """, [year]).fetchall()
                _check_containment(rows, match_type='containment_year', min_short=15)
                if len(batch) >= 10000:
                    self._insert_matches_batch(batch)
                    batch = []

            if batch:
                self._insert_matches_batch(batch)

            count_by_year = self.match_conn.execute(
                "SELECT COUNT(*) FROM match_db.matches WHERE match_type = 'containment_year'"
            ).fetchone()[0]
            print(f'  By year (authorless): {count_by_year} pairs')

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

    def _match_by_links(self, corpora=None):
        """Tier 0: match texts across corpora using LINKS declarations (shared IDs).

        For each corpus with LINKS, joins on the declared ID columns via the
        meta JSON blob. Only matches across different corpora.
        """
        from lltk.corpus.utils import load_manifest, load_corpus

        manifest = load_manifest()
        corpus_ids = set(corpora or [d.get('id', name) for name, d in manifest.items()])
        count_before = self.match_conn.execute(
            "SELECT COUNT(*) FROM match_db.matches"
        ).fetchone()[0]

        for corpus_id in corpus_ids:
            if corpus_id in DB_BLACKLIST:
                continue
            try:
                corpus = load_corpus(corpus_id)
            except Exception:
                continue
            # Combine LINKS and MATCH_LINKS for matching
            links = dict(getattr(corpus, 'LINKS', None) or {})
            match_links = getattr(corpus, 'MATCH_LINKS', None) or {}
            links.update(match_links)
            if not links:
                continue

            # Top-level columns in texts table (not in meta JSON)
            top_level_cols = {'_id', 'corpus', 'id', 'title', 'author', 'year',
                              'genre', 'genre_raw', 'is_translated',
                              'title_norm', 'author_norm', 'path_freqs'}

            for target_corpus_id, (my_col, their_col) in links.items():
                if corpora and target_corpus_id not in corpus_ids:
                    continue

                # Use top-level column directly if it exists, otherwise extract from meta JSON
                my_expr = f"a.{my_col}" if my_col in top_level_cols else f"json_extract_string(a.meta, '$.{my_col}')"
                their_expr = f"b.{their_col}" if their_col in top_level_cols else f"json_extract_string(b.meta, '$.{their_col}')"

                try:
                    self.match_conn.execute(f"""
                        INSERT OR IGNORE INTO match_db.matches (_id_a, _id_b, similarity, match_type)
                        SELECT CASE WHEN a._id < b._id THEN a._id ELSE b._id END,
                               CASE WHEN a._id < b._id THEN b._id ELSE a._id END,
                               1.0, 'id_link'
                        FROM texts a
                        JOIN texts b ON (
                            COALESCE({my_expr}, '') != ''
                            AND COALESCE({my_expr}, '') = COALESCE({their_expr}, '')
                        )
                        WHERE a.corpus = '{corpus_id}'
                          AND b.corpus = '{target_corpus_id}'
                          AND a._id != b._id
                    """)
                except Exception as e:
                    if log: log(f'ID link {corpus_id}.{my_col} → {target_corpus_id}.{their_col}: {e}')

        count_after = self.match_conn.execute(
            "SELECT COUNT(*) FROM match_db.matches"
        ).fetchone()[0]
        return count_after - count_before

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

    # ── Genre enrichment ─────────────────────────────────────────────

    def enrich_genres(self, progress=True):
        """
        Propagate genre labels from authority corpora across match groups.

        For each text in the DB:
        1. Start with its own genre + genre_source (from corpus load_metadata)
        2. If any match group member is in a genre authority corpus
           (fiction_biblio, end, ravengarside), inherit that genre
        3. Bibliography sources outrank ESTC heuristics

        Updates genre and genre_enriched_source columns on the texts table.
        The original corpus genre is preserved in genre_corpus.
        Must be run after db-match (needs match_groups).
        """
        from lltk.tools.tools import get_tqdm

        # Ensure columns exist
        for col, dtype in [('genre_enriched_source', 'TEXT'), ('genre_corpus', 'TEXT')]:
            try:
                self.conn.execute(f"ALTER TABLE texts ADD COLUMN {col} {dtype}")
            except Exception:
                pass

        # Step 0: Save original corpus genre before we overwrite
        self.conn.execute("UPDATE texts SET genre_corpus = genre")

        # Step 1: Reset enrichment source
        self.conn.execute("""
            UPDATE texts SET
                genre_enriched_source = CASE
                    WHEN genre IS NOT NULL THEN 'corpus'
                    ELSE NULL
                END
        """)
        if log: log('Reset genre_enriched to corpus baseline')

        # Step 1b: For ESTC-linked corpora, carry forward the genre_source from meta JSON
        # (form/topic/title) so we know which ESTC tier the genre came from
        self.conn.execute("""
            UPDATE texts SET
                genre_enriched_source = json_extract_string(meta, '$.genre_source')
            WHERE json_extract_string(meta, '$.genre_source') IS NOT NULL
              AND json_extract_string(meta, '$.genre_source') != ''
        """)
        if log: log('Set genre_enriched_source from ESTC genre_source where available')

        # Step 1c: For ESTC-linked corpora that inherited genre, use the estc_genre_source
        self.conn.execute("""
            UPDATE texts SET
                genre_enriched_source = json_extract_string(meta, '$.estc_genre_source')
            WHERE json_extract_string(meta, '$.estc_genre_source') IS NOT NULL
              AND json_extract_string(meta, '$.estc_genre_source') != ''
              AND genre_enriched_source = 'corpus'
        """)
        if log: log('Set genre_enriched_source from estc_genre_source for linked corpora')

        # Step 2: Propagate from genre authority corpora via match groups
        authority_corpora = list(GENRE_AUTHORITY_CORPORA.keys())
        authority_list = ', '.join(f"'{c}'" for c in authority_corpora)

        # Find all match groups that contain at least one authority corpus text
        # For each group, pick the authority text's genre (highest-priority authority wins)
        authority_groups = self.conn.execute(f"""
            SELECT mg.group_id,
                   t.genre as authority_genre,
                   t.genre_raw as authority_genre_raw,
                   t.corpus as authority_corpus,
                   t._id as authority_id
            FROM match_db.match_groups mg
            JOIN texts t ON mg._id = t._id
            WHERE t.corpus IN ({authority_list})
              AND t.genre IS NOT NULL
        """).fetchdf()

        if not len(authority_groups):
            if log: log('No authority corpus texts found in match groups')
            return

        # Deduplicate: one genre per group (highest priority authority wins)
        authority_groups['priority'] = authority_groups['authority_corpus'].map(GENRE_AUTHORITY_CORPORA)
        authority_groups = authority_groups.sort_values('priority', ascending=False)
        group_genre = authority_groups.drop_duplicates('group_id', keep='first')

        if log: log(f'Found {len(group_genre)} match groups with authority corpus members')

        # Step 3: For each group, update all member texts
        updated = 0
        iterr = group_genre.iterrows()
        if progress:
            iterr = get_tqdm(iterr, total=len(group_genre), desc='Enriching genres')
        for _, row in iterr:
            gid = int(row['group_id'])
            genre = row['authority_genre']
            genre_raw = row.get('authority_genre_raw') or ''
            source = f"bibliography:{row['authority_corpus']}"

            # Update texts in this group whose current genre_enriched_source
            # has lower priority than bibliography
            n = self.conn.execute("""
                UPDATE texts SET
                    genre = ?,
                    genre_raw = CASE
                        WHEN ? != '' THEN ?
                        WHEN genre_raw IS NOT NULL AND genre_raw NOT IN (
                            'Fiction','Novel','Novel, epistolary','Romance','Tale',
                            'Fable','Novella','Picaresque','Gothic','Imaginary voyage',
                            'Satire','Dialogue','Allegory','Epistolary fiction',
                            'Novel, sentimental','Novel, Gothic','Novel, satire',
                            'Novel, historical','Novel, didactic','Novel, oriental'
                        ) AND ? = 'Fiction' THEN NULL
                        ELSE genre_raw
                    END,
                    genre_enriched_source = ?
                WHERE _id IN (
                    SELECT _id FROM match_db.match_groups WHERE group_id = ?
                )
                AND (
                    genre IS NULL
                    OR genre != ?
                    OR genre_enriched_source IN ('corpus', 'form', 'title', 'topic')
                )
            """, [genre, genre_raw, genre_raw, genre, source, gid, genre]).fetchone()
            # DuckDB UPDATE doesn't return rowcount easily; count separately
            updated += 1

        # Step 3b: Also update texts NOT in any match group but IN authority corpora
        # (authority corpus texts that didn't match anything still get bibliography source)
        self.conn.execute(f"""
            UPDATE texts SET
                genre_enriched_source = 'bibliography:' || corpus
            WHERE corpus IN ({authority_list})
              AND genre IS NOT NULL
        """)

        # Report
        stats = self.conn.execute("""
            SELECT genre_enriched_source, COUNT(*) as n
            FROM texts
            WHERE genre_enriched_source IS NOT NULL
            GROUP BY genre_enriched_source
            ORDER BY n DESC
        """).fetchdf()

        if log:
            log(f'Genre enrichment complete:')
            for _, r in stats.iterrows():
                log(f'  {r["genre_enriched_source"]}: {r["n"]}')

        # Count texts whose genre changed from original corpus genre
        changed = self.conn.execute("""
            SELECT COUNT(*) FROM texts
            WHERE genre != genre_corpus OR (genre IS NOT NULL AND genre_corpus IS NULL)
        """).fetchone()[0]
        if log: log(f'Texts with genre changed by enrichment: {changed}')

        return stats

    # ── Word counts ────────────────────────────────────────────────

    def wordcounts(self, num_proc=None, progress=True):
        """Compute word counts from freqs files, cached in metadb_wordcounts.duckdb.

        Reads freqs JSON files, sums word counts, and stores in a persistent
        cache keyed by path_freqs. Incremental: only processes paths not
        already cached. Results are written back to n_words on the texts table.
        """
        from lltk.tools.tools import get_tqdm
        from concurrent.futures import ThreadPoolExecutor
        if num_proc is None:
            num_proc = max(1, os.cpu_count() - 2)

        # Find paths that need counting (not in cache)
        todo = self.conn.execute("""
            SELECT DISTINCT t.path_freqs
            FROM texts t
            WHERE t.path_freqs IS NOT NULL
              AND t.path_freqs NOT IN (SELECT path_freqs FROM wc_db.wordcounts)
        """).fetchdf()

        if not len(todo):
            if log: log('All word counts cached')
        else:
            paths = todo['path_freqs'].tolist()
            if log: log(f'Counting words for {len(paths)} freqs files...')

            from lltk.imports import PATH_CORPUS
            corpus_root = os.path.expanduser(PATH_CORPUS)

            def _count_one(rel_path):
                abs_path = os.path.join(corpus_root, rel_path)
                try:
                    if abs_path.endswith('.gz'):
                        import gzip
                        with gzip.open(abs_path, 'rt') as f:
                            d = json.load(f)
                    else:
                        with open(abs_path) as f:
                            d = json.load(f)
                    return (rel_path, int(sum(d.values())))
                except Exception:
                    return None

            results = []
            with ThreadPoolExecutor(max_workers=num_proc) as pool:
                iterr = pool.map(_count_one, paths)
                if progress:
                    iterr = get_tqdm(iterr, total=len(paths), desc='Counting words')
                for result in iterr:
                    if result:
                        results.append(result)
                    # Batch insert every 10K
                    if len(results) >= 10000:
                        self._insert_wordcounts_batch(results)
                        results = []

            if results:
                self._insert_wordcounts_batch(results)

            if log: log(f'Cached {len(todo)} word counts')

        # Backfill n_words on texts table from cache
        updated = self.conn.execute("""
            UPDATE texts SET n_words = wc.n_words
            FROM wc_db.wordcounts wc
            WHERE texts.path_freqs = wc.path_freqs
              AND texts.n_words IS NULL
        """)

        total_with_wc = self.conn.execute(
            "SELECT COUNT(*) FROM texts WHERE n_words IS NOT NULL"
        ).fetchone()[0]
        total_with_freqs = self.conn.execute(
            "SELECT COUNT(*) FROM texts WHERE path_freqs IS NOT NULL"
        ).fetchone()[0]
        if log: log(f'Word counts: {total_with_wc}/{total_with_freqs} texts with n_words')

    def _insert_wordcounts_batch(self, results):
        """Insert a batch of (path_freqs, n_words) into the wordcount cache."""
        import pandas as pd
        df = pd.DataFrame(results, columns=['path_freqs', 'n_words'])
        self.conn.execute("""
            INSERT OR IGNORE INTO wc_db.wordcounts (path_freqs, n_words)
            SELECT path_freqs, n_words FROM df
        """)

    # ── Word Index ────────────────────────────────────────────────

    def build_word_index(self, num_proc=None, progress=True, min_count=1,
                         vocab_size=100_000, corpora=None):
        """Build per-word frequency index from freqs files.

        Two-pass build:
          Pass 1: scan all freqs files to build vocabulary (top N words by document frequency)
          Pass 2: re-scan and insert only words in the vocabulary

        This keeps the index manageable (~2-4 GB) while covering all useful words.
        Incremental: skips texts already indexed on pass 2.

            lltk.db.build_word_index()                      # all texts, top 100K words
            lltk.db.build_word_index(vocab_size=50_000)     # smaller vocab
            lltk.db.build_word_index(corpora=['ecco_tcp'])  # specific corpora
        """
        from lltk.tools.tools import get_tqdm
        from collections import Counter
        from concurrent.futures import ThreadPoolExecutor
        if num_proc is None:
            num_proc = max(1, os.cpu_count() - 2)

        from lltk.imports import PATH_CORPUS
        corpus_root = os.path.expanduser(PATH_CORPUS)

        corpus_filter = ''
        if corpora:
            corpus_list = ', '.join(f"'{c}'" for c in corpora)
            corpus_filter = f'AND t.corpus IN ({corpus_list})'

        # All texts with freqs
        all_texts = self.conn.execute(f"""
            SELECT t._id, t.path_freqs
            FROM texts t
            WHERE t.path_freqs IS NOT NULL {corpus_filter}
        """).fetchdf()

        if not len(all_texts):
            if log: log('No texts with freqs found')
            return

        all_pairs = list(zip(all_texts['_id'], all_texts['path_freqs']))

        def _read_freqs_file(rel_path):
            """Read a freqs JSON file, return dict or None."""
            abs_path = os.path.join(corpus_root, rel_path)
            try:
                if abs_path.endswith('.gz'):
                    import gzip
                    with gzip.open(abs_path, 'rt') as f:
                        return json.load(f)
                else:
                    with open(abs_path) as f:
                        return json.load(f)
            except Exception:
                return None

        # ── Pass 1: Build vocabulary ────────────────────────────────
        if log: log(f'Pass 1: Building vocabulary from {len(all_pairs):,} texts...')

        doc_freq = Counter()  # word → number of texts containing it

        def _count_vocab(row):
            _id, rel_path = row
            d = _read_freqs_file(rel_path)
            if d is None:
                return set()
            return set(w for w, c in d.items() if c >= min_count)

        with ThreadPoolExecutor(max_workers=num_proc) as pool:
            iterr = pool.map(_count_vocab, all_pairs)
            if progress:
                iterr = get_tqdm(iterr, total=len(all_pairs), desc='Pass 1: vocabulary')
            for word_set in iterr:
                doc_freq.update(word_set)

        # Keep top N by document frequency
        vocab = set(w for w, _ in doc_freq.most_common(vocab_size))
        if log:
            log(f'Vocabulary: {len(doc_freq):,} unique words → top {len(vocab):,} kept')
            # Show doc freq at the cutoff
            if len(doc_freq) > vocab_size:
                cutoff_df = doc_freq.most_common(vocab_size)[-1][1]
                log(f'  Min doc frequency in vocab: {cutoff_df}')

        # ── Pass 2: Index words in vocabulary ───────────────────────
        # Find texts not yet indexed
        todo = self.conn.execute(f"""
            SELECT t._id, t.path_freqs
            FROM texts t
            WHERE t.path_freqs IS NOT NULL {corpus_filter}
              AND t._id NOT IN (SELECT DISTINCT _id FROM wi_db.word_index)
        """).fetchdf()

        if not len(todo):
            if log: log('All texts already indexed')
            self._ensure_wordindex_indexes()
            return

        if log: log(f'Pass 2: Indexing {len(todo):,} texts...')
        todo_pairs = list(zip(todo['_id'], todo['path_freqs']))

        def _read_filtered(row):
            _id, rel_path = row
            d = _read_freqs_file(rel_path)
            if d is None:
                return []
            return [(_id, w, c) for w, c in d.items() if w in vocab and c >= min_count]

        rows_to_insert = []
        batch_size = 500_000
        n_inserted = 0

        with ThreadPoolExecutor(max_workers=num_proc) as pool:
            iterr = pool.map(_read_filtered, todo_pairs)
            if progress:
                iterr = get_tqdm(iterr, total=len(todo_pairs), desc='Pass 2: indexing')
            for result in iterr:
                rows_to_insert.extend(result)
                if len(rows_to_insert) >= batch_size:
                    self._insert_wordindex_batch(rows_to_insert)
                    n_inserted += len(rows_to_insert)
                    rows_to_insert = []

        if rows_to_insert:
            self._insert_wordindex_batch(rows_to_insert)
            n_inserted += len(rows_to_insert)

        if log: log(f'Inserted {n_inserted:,} word-count rows')

        # Build indexes
        if log: log('Building indexes...')
        self._ensure_wordindex_indexes()

        n_total = self.conn.execute("SELECT COUNT(DISTINCT _id) FROM wi_db.word_index").fetchone()[0]
        n_rows = self.conn.execute("SELECT COUNT(*) FROM wi_db.word_index").fetchone()[0]
        if log: log(f'Word index: {n_total:,} texts, {n_rows:,} rows')

    def _insert_wordindex_batch(self, rows):
        """Insert a batch of (_id, word, count) into word_index."""
        df = pd.DataFrame(rows, columns=['_id', 'word', 'count'])
        self.conn.execute("""
            INSERT INTO wi_db.word_index (_id, word, count)
            SELECT _id, word, count FROM df
        """)

    def drop_word_index(self):
        """Drop all word_index data."""
        self.conn.execute("DELETE FROM wi_db.word_index")
        if log: log('Dropped word_index')

    def has_word_index(self):
        """Check if word_index has data."""
        try:
            n = self.conn.execute("SELECT COUNT(*) FROM wi_db.word_index").fetchone()[0]
            return n > 0
        except Exception:
            return False

    # ── Ngram queries ──────────────────────────────────────────────

    def ngram(self, words, genre=None, corpus=None, year_min=1500, year_max=2020,
              normalize='per_million', by='decade'):
        """Query word frequency over time.

            lltk.db.ngram('virtue')
            lltk.db.ngram(['virtue', 'honor'], genre='Fiction')
            lltk.db.ngram('virtue', normalize='raw', by='year')
        """
        if isinstance(words, str):
            words = [w.strip() for w in words.split(',')]

        word_list = ', '.join(f"'{w}'" for w in words)

        if by == 'decade':
            time_expr = 'CAST(t.year / 10 AS INTEGER) * 10'
        elif by == 'year':
            time_expr = 't.year'
        else:
            time_expr = f'CAST(t.year / {int(by)} AS INTEGER) * {int(by)}'

        clauses = [f't.year BETWEEN {int(year_min)} AND {int(year_max)}']
        if genre:
            clauses.append(f"t.genre = '{genre}'")
        if corpus:
            clauses.append(f"t.corpus = '{corpus}'")
        where = ' AND '.join(clauses)

        if normalize == 'per_million':
            value_expr = 'SUM(wi.count) * 1000000.0 / NULLIF(SUM(t.n_words), 0)'
        else:
            value_expr = 'SUM(wi.count)'

        sql = f"""
            SELECT {time_expr} as period,
                   wi.word,
                   {value_expr} as value,
                   SUM(wi.count) as raw_count,
                   COUNT(DISTINCT wi._id) as n_texts
            FROM wi_db.word_index wi
            JOIN texts t ON wi._id = t._id
            WHERE wi.word IN ({word_list})
              AND {where}
            GROUP BY period, wi.word
            ORDER BY period, wi.word
        """
        return self.conn.execute(sql).fetchdf()

    def ngram_examples(self, word, genre=None, corpus=None,
                       year_min=None, year_max=None, limit=20):
        """Find texts that use a word most frequently.

            lltk.db.ngram_examples('virtue', genre='Fiction', year_min=1750, year_max=1759)
        """
        clauses = [f"wi.word = '{word}'"]
        if genre:
            clauses.append(f"t.genre = '{genre}'")
        if corpus:
            clauses.append(f"t.corpus = '{corpus}'")
        if year_min is not None:
            clauses.append(f't.year >= {int(year_min)}')
        if year_max is not None:
            clauses.append(f't.year <= {int(year_max)}')
        where = ' AND '.join(clauses)

        sql = f"""
            SELECT t._id, t.corpus, t.title, t.author, t.year, t.genre,
                   wi.count,
                   wi.count * 1000000.0 / NULLIF(t.n_words, 0) as per_million
            FROM wi_db.word_index wi
            JOIN texts t ON wi._id = t._id
            WHERE {where}
            ORDER BY per_million DESC
            LIMIT {int(limit)}
        """
        return self.conn.execute(sql).fetchdf()

    def ngram_collocates(self, word, genre=None, corpus=None,
                         year_min=None, year_max=None, limit=50):
        """Find words that co-occur with a given word (document-level).

            lltk.db.ngram_collocates('virtue', genre='Fiction', year_min=1750, year_max=1759)
        """
        clauses = [f"w1.word = '{word}'", f"w2.word != '{word}'"]
        if genre:
            clauses.append(f"t.genre = '{genre}'")
        if corpus:
            clauses.append(f"t.corpus = '{corpus}'")
        if year_min is not None:
            clauses.append(f't.year >= {int(year_min)}')
        if year_max is not None:
            clauses.append(f't.year <= {int(year_max)}')
        where = ' AND '.join(clauses)

        sql = f"""
            SELECT w2.word, COUNT(DISTINCT w1._id) as n_texts, SUM(w2.count) as total_count
            FROM wi_db.word_index w1
            JOIN wi_db.word_index w2 ON w1._id = w2._id
            JOIN texts t ON w1._id = t._id
            WHERE {where}
            GROUP BY w2.word
            ORDER BY n_texts DESC
            LIMIT {int(limit)}
        """
        return self.conn.execute(sql).fetchdf()

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
            _RANGE_SUFFIXES = {'_min': '>=', '_max': '<='}
            for corpus_id, filters in sources.items():
                parts = [f"t.corpus = '{corpus_id}'"]
                for k, v in filters.items():
                    # Support range filters: year_min, year_max, etc.
                    handled = False
                    for suffix, op in _RANGE_SUFFIXES.items():
                        if k.endswith(suffix):
                            col = k[:-len(suffix)]
                            parts.append(f"t.{col} {op} {int(v)}")
                            handled = True
                            break
                    if not handled:
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
