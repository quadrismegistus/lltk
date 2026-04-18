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
import orjson
import pandas as pd
from lltk.imports import PATH_LLTK_DATA, log

PATH_METADB = os.path.join(PATH_LLTK_DATA, 'metadb.duckdb')
PATH_MATCHDB = os.path.join(PATH_LLTK_DATA, 'metadb_matches.duckdb')
PATH_WORDCOUNTDB = os.path.join(PATH_LLTK_DATA, 'metadb_wordcounts.duckdb')
PATH_WORDINDEXDB = os.path.join(PATH_LLTK_DATA, 'metadb_wordindex.duckdb')
PATH_PASSAGESDB = os.path.join(PATH_LLTK_DATA, 'metadb_passages.sqlite')

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

# Normalize language codes to ISO 639-1 two-letter codes
LANG_NORMALIZE = {
    # ISO 639-2/B (bibliographic)
    'eng': 'en', 'fre': 'fr', 'ger': 'de', 'spa': 'es', 'ita': 'it',
    'por': 'pt', 'rus': 'ru', 'lat': 'la', 'grc': 'el', 'dut': 'nl',
    'swe': 'sv', 'dan': 'da', 'nor': 'no', 'pol': 'pl', 'hun': 'hu',
    'cze': 'cs', 'rum': 'ro', 'fin': 'fi',
    # ISO 639-2/T (terminological)
    'fra': 'fr', 'deu': 'de', 'nld': 'nl', 'ces': 'cs', 'ron': 'ro',
    # Full names (lowercase)
    'english': 'en', 'french': 'fr', 'german': 'de', 'spanish': 'es',
    'italian': 'it', 'portuguese': 'pt', 'russian': 'ru', 'latin': 'la',
    'greek': 'el', 'dutch': 'nl', 'swedish': 'sv', 'danish': 'da',
    'norwegian': 'no', 'polish': 'pl', 'hungarian': 'hu',
    'czech': 'cs', 'romanian': 'ro', 'finnish': 'fi',
    # Already ISO 639-1 — pass through
    'en': 'en', 'fr': 'fr', 'de': 'de', 'es': 'es', 'it': 'it',
    'pt': 'pt', 'ru': 'ru', 'la': 'la', 'el': 'el', 'nl': 'nl',
    'sv': 'sv', 'da': 'da', 'no': 'no', 'pl': 'pl', 'hu': 'hu',
    'cs': 'cs', 'ro': 'ro', 'fi': 'fi',
}


def normalize_lang(lang):
    """Normalize a language code/name to ISO 639-1 two-letter code."""
    if not lang or not isinstance(lang, str) or lang.strip() == '' or lang == 'nan':
        return None
    return LANG_NORMALIZE.get(lang.strip().lower())


# Corpora excluded from DB ingest (too large, not useful as standalone)
DB_BLACKLIST = {'hathi', 'bighist'}

# Core columns stored as real columns; everything else goes in meta JSON
CORE_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'year', 'genre', 'genre_raw', 'lang', 'is_translated', 'title_norm', 'author_norm', 'path_freqs']
STANDARD_COLS = ['id', 'title', 'author', 'year', 'genre', 'genre_raw']
TEXT_COLS = ['_id', 'corpus', 'id', 'title', 'author', 'genre', 'genre_raw', 'lang', 'title_norm', 'author_norm', 'path_freqs']  # cols stored as TEXT (not is_translated — that's BOOLEAN)

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


# ── Picklable workers for ProcessPoolExecutor (word index build) ──

def _wi_read_freqs(args):
    """Read a freqs JSON, return dict or None. Picklable for multiprocessing."""
    corpus_root, rel_path = args
    abs_path = os.path.join(corpus_root, rel_path)
    try:
        if abs_path.endswith('.gz'):
            import gzip
            with gzip.open(abs_path, 'rb') as f:
                return orjson.loads(f.read())
        else:
            with open(abs_path, 'rb') as f:
                return orjson.loads(f.read())
    except Exception:
        return None

def _wi_is_word(w):
    """Filter: keep only alphabetic words (allows hyphens/apostrophes internally)."""
    return w and w[0].isalpha() and all(c.isalpha() or c in "-'" for c in w)

def _wi_get_word_set(args):
    """Return set of unique lowercased words in a text's freqs. For pass 1 vocabulary."""
    corpus_root, _id, rel_path, min_count = args
    d = _wi_read_freqs((corpus_root, rel_path))
    if d is None:
        return set()
    return set(w.lower() for w, c in d.items() if c >= min_count and _wi_is_word(w))

def _wi_agg_batch_from_db(args):
    """Read a batch of (_id, year, corpus, genre, is_preferred) from ClickHouse
    text_freqs, aggregate (word, year, corpus, genre) → (wc, nt, wc_d, nt_d)
    locally, and (optionally) also aggregate a per-chunk vocab Counter. Writes
    two parquet shards.

    args: (ch_url, batch, min_count, shard_dir, shard_idx, vocab_set_or_None)
      ch_url: clickhouse:// URL string for connecting from the worker
      batch: list[(_id, year, corpus, genre, is_preferred)]
      vocab_set: frozenset of words to keep for Pass 2; None for Pass 1 (vocab build)

    Returns (n_texts_processed, word_shard_path, vocab_shard_path).
    """
    import re
    from collections import defaultdict, Counter
    import pyarrow as pa
    import pyarrow.parquet as pq
    from lltk.tools.db_adapter import get_adapter

    ch_url, batch, min_count, shard_dir, shard_idx, vocab_set = args

    ch = get_adapter(ch_url)
    ids = [b[0] for b in batch]
    id_list = ', '.join(f"'{i}'" for i in ids)  # _ids are corpus-prefixed strings
    rows = ch.query(
        f"SELECT _id, freqs FROM lltk.text_freqs WHERE _id IN ({id_list})"
    )
    ch.close()

    # Python lookup: _id → (year, corpus, genre, is_preferred)
    meta = {b[0]: (b[1], b[2], b[3], b[4]) for b in batch}

    word_re = re.compile(r"^[a-zA-Z][a-zA-Z'\-]*$")

    if vocab_set is None:
        # Pass 1: collect word → total count (filter by regex)
        vocab_counter = Counter()
        for _id, freqs in rows:
            for w, c in freqs.items():
                if c < min_count:
                    continue
                wl = w.lower()
                if word_re.match(wl):
                    vocab_counter[wl] += c
        vocab_path = os.path.join(shard_dir, f'vocab_{shard_idx:06d}.parquet')
        if vocab_counter:
            pq.write_table(pa.table({
                'word': list(vocab_counter.keys()),
                'cnt': list(vocab_counter.values()),
            }), vocab_path, compression='zstd')
        else:
            vocab_path = None
        return (len(batch), None, vocab_path)
    else:
        # Pass 2: aggregate (word, year, corpus, genre) → [wc, nt, wc_d, nt_d]
        agg = defaultdict(lambda: [0, 0, 0, 0])
        for _id, freqs in rows:
            if _id not in meta:
                continue
            year, corpus, genre, is_pref = meta[_id]
            for w, c in freqs.items():
                if c < min_count:
                    continue
                wl = w.lower()
                if wl not in vocab_set:
                    continue
                key = (wl, year, corpus, genre)
                cell = agg[key]
                cell[0] += c
                cell[1] += 1
                if is_pref:
                    cell[2] += c
                    cell[3] += 1
        word_path = os.path.join(shard_dir, f'wyc_{shard_idx:06d}.parquet')
        if agg:
            words, years, corpora, genres, wcs, nts, wcds, ntds = [], [], [], [], [], [], [], []
            for (w, y, c, g), (wc, nt, wcd, ntd) in agg.items():
                words.append(w); years.append(y); corpora.append(c); genres.append(g)
                wcs.append(wc); nts.append(nt); wcds.append(wcd); ntds.append(ntd)
            pq.write_table(pa.table({
                'word': words, 'year': years, 'corpus': corpora, 'genre': genres,
                'word_count': wcs, 'n_texts': nts,
                'word_count_dedup': wcds, 'n_texts_dedup': ntds,
            }), word_path, compression='zstd')
        else:
            word_path = None
        return (len(batch), word_path, None)

def _chunk_text_to_passages(args):
    """Chunk a single text file into ~n-word passages using sentence splitting."""
    _id, corpus_id, txt_path, lang, n = args
    try:
        from lltk.text.text import _open_file, _lang_to_punkt
        import nltk
        with _open_file(txt_path) as f:
            txt = f.read()
        if not txt or not txt.strip():
            return (_id, corpus_id, [])

        punkt_lang = _lang_to_punkt(lang)
        sents = nltk.sent_tokenize(txt, language=punkt_lang)

        passages = []
        chunk_sents = []
        chunk_word_count = 0
        seq = 0

        for sent in sents:
            sent_n = len(sent.split())
            chunk_sents.append(sent)
            chunk_word_count += sent_n

            if chunk_word_count >= n:
                passages.append((_id, seq, ' '.join(chunk_sents), chunk_word_count, lang))
                seq += 1
                chunk_sents = []
                chunk_word_count = 0

        if chunk_sents:
            passages.append((_id, seq, ' '.join(chunk_sents), chunk_word_count, lang))

        return (_id, corpus_id, passages)
    except Exception:
        return (_id, corpus_id, [])


def _wi_pass1_batch(args):
    """Pass 1 worker: aggregate doc frequencies for a batch of texts locally.

    Returns (n_texts_processed, Counter) where Counter[word] = # of texts in batch
    containing word (each incremented once per text, regardless of token count).
    """
    from collections import Counter
    corpus_root, batch, min_count = args
    local = Counter()
    n = 0
    for rel_path in batch:
        d = _wi_read_freqs((corpus_root, rel_path))
        n += 1
        if d is None:
            continue
        seen = set()
        for w, c in d.items():
            if c >= min_count and _wi_is_word(w):
                wl = w.lower()
                if wl not in seen:
                    local[wl] += 1
                    seen.add(wl)
    return (n, local)

_wi_vocab = None  # set by parent before ProcessPoolExecutor fork

def _wi_init(vocab):
    """ProcessPoolExecutor initializer: set vocab in each worker once."""
    global _wi_vocab
    _wi_vocab = vocab

def _wi_read_filtered(args):
    """Read freqs, filter to vocabulary, lowercase and merge counts."""
    corpus_root, _id, rel_path, min_count = args
    d = _wi_read_freqs((corpus_root, rel_path))
    if d is None:
        return []
    merged = {}
    for w, c in d.items():
        if c >= min_count and _wi_is_word(w):
            wl = w.lower()
            if wl in _wi_vocab:
                merged[wl] = merged.get(wl, 0) + c
    return [(_id, w, c) for w, c in merged.items()]

def _wi_aggregate_batch(args):
    """Aggregate a batch of texts locally and write parquet shards.

    Returns (n_texts_processed, word_shard_path or None, totals_shard_path or None).
    """
    from collections import defaultdict
    corpus_root, batch, min_count, shard_dir, shard_idx = args

    agg = defaultdict(lambda: [0, 0, 0, 0])     # (word, year, corpus, genre) → [wc, nt, wc_d, nt_d]
    totals = defaultdict(lambda: [0, 0, 0, 0])  # (year, corpus, genre) → [nt, tw, nt_d, tw_d]

    for _id, rel_path, year, corpus, genre, n_words, is_pref in batch:
        d = _wi_read_freqs((corpus_root, rel_path))
        if d is None:
            continue

        merged = {}
        for w, c in d.items():
            if c >= min_count and _wi_is_word(w):
                wl = w.lower()
                if wl in _wi_vocab:
                    merged[wl] = merged.get(wl, 0) + c

        grp = (year, corpus, genre)
        tot = totals[grp]
        tot[0] += 1
        tot[1] += n_words
        if is_pref:
            tot[2] += 1
            tot[3] += n_words

        for word, count in merged.items():
            key = (word, year, corpus, genre)
            wc = agg[key]
            wc[0] += count
            wc[1] += 1
            if is_pref:
                wc[2] += count
                wc[3] += 1

    import pandas as pd
    word_path = None
    tot_path = None
    if agg:
        word_path = os.path.join(shard_dir, f'words_{shard_idx:06d}.parquet')
        word_rows = [(w, y, c, g, wc, nt, wcd, ntd)
                     for (w, y, c, g), (wc, nt, wcd, ntd) in agg.items()]
        pd.DataFrame(word_rows, columns=[
            'word', 'year', 'corpus', 'genre',
            'word_count', 'n_texts', 'word_count_dedup', 'n_texts_dedup'
        ]).to_parquet(word_path, index=False)
    if totals:
        tot_path = os.path.join(shard_dir, f'totals_{shard_idx:06d}.parquet')
        tot_rows = [(y, c, g, nt, tw, ntd, twd)
                    for (y, c, g), (nt, tw, ntd, twd) in totals.items()]
        pd.DataFrame(tot_rows, columns=[
            'year', 'corpus', 'genre', 'n_texts', 'total_words',
            'n_texts_dedup', 'total_words_dedup'
        ]).to_parquet(tot_path, index=False)

    return (len(batch), word_path, tot_path)


class MetaDB:
    def __init__(self, path=None, match_path=None, wordcount_path=None, wordindex_path=None,
                 read_only=False):
        self.path = path or PATH_METADB
        self.match_path = match_path or PATH_MATCHDB
        self.wordcount_path = wordcount_path or PATH_WORDCOUNTDB
        self.wordindex_path = wordindex_path or PATH_WORDINDEXDB
        self.read_only = read_only
        self._conn = None
        self._col_cache = None

    @property
    def conn(self):
        """Single connection to texts DB, with matches DB attached as match_db."""
        if self._conn is None:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self._conn = duckdb.connect(self.path, read_only=self.read_only)
            if not self.read_only:
                self._ensure_text_tables()
            # Attach matches DB
            ro = ' (READ_ONLY)' if self.read_only else ''
            try:
                os.makedirs(os.path.dirname(self.match_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.match_path}' AS match_db{ro}")
            except Exception:
                pass  # already attached or doesn't exist
            if not self.read_only:
                self._ensure_match_tables()
            # Attach wordcounts DB
            try:
                os.makedirs(os.path.dirname(self.wordcount_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.wordcount_path}' AS wc_db{ro}")
            except Exception:
                pass
            if not self.read_only:
                self._ensure_wordcount_tables()
            # Attach word index DB
            try:
                os.makedirs(os.path.dirname(self.wordindex_path), exist_ok=True)
                self._conn.execute(f"ATTACH '{self.wordindex_path}' AS wi_db{ro}")
            except Exception:
                pass
            if not self.read_only:
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
                           ('genre_enriched_source', 'TEXT'), ('genre_corpus', 'TEXT'), ('lang', 'TEXT'),
                           ('original_lang', 'TEXT'),
                           ('lang_metadata', 'TEXT'), ('lang_detected', 'TEXT'),
                           ('lang_coverage', 'DOUBLE'), ('lang_confidence', 'DOUBLE')]:
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
        """Ensure word aggregate tables exist (created by build_word_index)."""
        pass

    def _ensure_wordindex_indexes(self):
        """No-op — aggregate tables don't need indexes."""
        pass

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

        # Pass manifest lang as fallback for corpora without a lang column
        manifest_lang = getattr(corpus, 'lang', None)

        return self.ingest_df(df, corpus_id, force=force, default_lang=manifest_lang)

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

    def prepare_corpus_df(self, df, corpus_id, default_lang=None):
        """Apply LLTK's standard metadata cleanup for a corpus DataFrame.

        Engine-agnostic — returns a DataFrame with the canonical column set
        (CORE_COLS + 'meta' JSON) ready to insert into either DuckDB or
        ClickHouse. See ingest_df() for the DuckDB-specific INSERT path.
        """
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

        # Normalize language codes to ISO 639-1
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

        # Convert text core cols to string
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

        insert_cols = CORE_COLS + ['meta']
        return df[insert_cols].copy()

    def ingest_df(self, df, corpus_id, force=True, default_lang=None):
        """Ingest a DataFrame into the DuckDB texts table for a given corpus."""
        insert_df = self.prepare_corpus_df(df, corpus_id, default_lang=default_lang)
        if insert_df is None:
            return None

        # Remove old data for this corpus
        if force:
            self.conn.execute("DELETE FROM texts WHERE corpus = ?", [corpus_id])

        insert_cols = CORE_COLS + ['meta']

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

    # ── Translation detection ─────────────────────────────────────

    def detect_translations(self):
        """Detect translations via cross-language match groups.

        For each match group containing texts in multiple languages:
        1. Find the earliest year per language in the group
        2. The language with the earliest text is the original language
        3. Texts in other languages get is_translated=True, original_lang set

        Title keywords ("traduit", "translated", "übersetzt") confirm direction
        but are not required — year priority is the primary signal.

        Writes to: is_translated (BOOLEAN), original_lang (TEXT) on texts table.
        Only touches texts in cross-language match groups — leaves other
        is_translated values (e.g. from ESTC MARC data) untouched.
        """
        _ = self.conn  # ensure connection + schema migrations

        # Find cross-language match groups
        cross = self.conn.execute("""
            WITH group_langs AS (
                SELECT mg.group_id,
                       COUNT(DISTINCT t.lang) as n_langs
                FROM match_db.match_groups mg
                JOIN texts t ON mg._id = t._id
                WHERE t.lang IS NOT NULL
                GROUP BY mg.group_id
                HAVING COUNT(DISTINCT t.lang) >= 2
            )
            SELECT mg.group_id, t._id, t.lang, t.year, t.title, t.corpus
            FROM match_db.match_groups mg
            JOIN texts t ON mg._id = t._id
            WHERE mg.group_id IN (SELECT group_id FROM group_langs)
              AND t.lang IS NOT NULL
            ORDER BY mg.group_id, t.year NULLS LAST
        """).fetchdf()

        if cross.empty:
            if log: log('No cross-language match groups found.')
            return {}

        n_groups = cross['group_id'].nunique()
        if log: log(f'Found {n_groups} cross-language match groups ({len(cross)} texts)')

        # Translation title signals (confirms but doesn't determine)
        import re as _re
        _translation_re = _re.compile(
            r'traduit|traduction|übersetzt|übersetzung|translated|translation',
            _re.IGNORECASE,
        )

        updates = []  # (_id, is_translated, original_lang)

        for gid, group in cross.groupby('group_id'):
            # Earliest year per language
            lang_min_year = (
                group.dropna(subset=['year'])
                .groupby('lang')['year']
                .min()
            )
            if lang_min_year.empty:
                continue

            # Original language = language with earliest text
            orig_lang = lang_min_year.idxmin()
            orig_year = lang_min_year.min()

            # Tie-breaking: if two languages share the same earliest year,
            # prefer the one with more texts in the group
            tied = lang_min_year[lang_min_year == orig_year]
            if len(tied) > 1:
                lang_counts = group[group['lang'].isin(tied.index)].groupby('lang').size()
                orig_lang = lang_counts.idxmax()

            for _, row in group.iterrows():
                if row['lang'] == orig_lang:
                    # Original language text — not a translation (from this signal)
                    # But don't clear is_translated if already set from MARC data
                    updates.append((row['_id'], False, None))
                else:
                    # Translation
                    updates.append((row['_id'], True, orig_lang))

        # Apply updates
        if updates:
            import tempfile
            update_df = pd.DataFrame(updates, columns=['_id', '_is_trans', '_orig_lang'])

            # Only set is_translated=True for translations; don't overwrite
            # existing is_translated=True on originals (may come from MARC)
            self.conn.execute("""
                UPDATE texts SET
                    is_translated = CASE
                        WHEN u._is_trans THEN TRUE
                        ELSE texts.is_translated
                    END,
                    original_lang = u._orig_lang
                FROM update_df u
                WHERE texts._id = u._id
            """)

        # Stats
        n_translated = sum(1 for _, t, _ in updates if t)
        n_originals = sum(1 for _, t, _ in updates if not t)
        lang_pairs = cross.groupby('group_id')['lang'].apply(
            lambda x: ' → '.join(sorted(x.unique()))
        ).value_counts().head(10)

        stats = {
            'n_groups': n_groups,
            'n_texts': len(updates),
            'n_translated': n_translated,
            'n_originals': n_originals,
        }
        if log:
            log(f'Marked {n_translated} texts as translations, '
                f'{n_originals} as originals across {n_groups} groups')
            log(f'Top language pairs:\n{lang_pairs.to_string()}')
        return stats

    # ── Per-text language detection ───────────────────────────────

    def _apply_detected_langs(self, apply=False, apply_conservative=False):
        """Apply the selected lang_detected → lang rule. Returns stats dict."""
        if apply and apply_conservative:
            raise ValueError("Pass only one of apply or apply_conservative, not both")
        conn = self.conn
        if apply:
            applied = conn.execute("""
                UPDATE texts SET lang = lang_detected
                WHERE lang_detected IS NOT NULL
                  AND lang_detected != 'unknown'
                  AND (lang IS NULL OR lang != lang_detected)
                RETURNING _id, lang_detected
            """).fetchdf()
            rule = 'symmetric'
        elif apply_conservative:
            applied = conn.execute("""
                UPDATE texts SET lang = lang_detected
                WHERE lang_metadata = 'en'
                  AND lang_detected IS NOT NULL
                  AND lang_detected NOT IN ('en', 'unknown')
                RETURNING _id, lang_detected
            """).fetchdf()
            rule = 'conservative (default-en → non-en)'
        else:
            return {'n_applied': 0}
        if log:
            log(f'Applied lang_detected to lang for {len(applied)} texts ({rule})')
            if len(applied):
                breakdown = applied.groupby('lang_detected').size().sort_values(ascending=False)
                log(f'Breakdown by new lang:\n{breakdown.to_string()}')
        return {'n_applied': len(applied)}

    def detect_langs(self, batch_size=5000, min_tokens=50,
                     coverage_threshold=0.05, confidence_threshold=2.0,
                     apply=False, apply_conservative=False, only_apply=False,
                     num_proc=None, progress=True):
        """Detect per-text language via stopword intersection against ClickHouse text_freqs.

        For each text with freqs, computes per-language hit counts against
        function-word lists (NLTK stopwords + curated Latin/Greek). Assigns
        to the language with the highest coverage if it dominates the
        runner-up by the configured factor.

        Writes four columns on the texts table:
          - lang_metadata   : snapshot of current `lang` value (taken once)
          - lang_detected   : argmax language from freqs detection
          - lang_coverage   : share of tokens captured by top language's stopwords
          - lang_confidence : ratio of top language hits to runner-up hits

        By default `lang` is NOT modified. Pass apply=True to overwrite
        `lang` with `lang_detected` for high-confidence detections
        (lang_detected is a real language, not 'unknown' or NULL).

        Args:
            batch_size: texts per fetch chunk (memory trade-off)
            min_tokens: skip texts with fewer total tokens than this
            coverage_threshold: min share of tokens hitting top lang's stopwords
            confidence_threshold: min ratio of top hits to second-place hits
            apply: if True, also update the `lang` column with detected values
            progress: show a tqdm progress bar

        Returns a dict with distribution stats.
        """
        from lltk.tools.lang_detect import function_words_table
        from lltk.tools.tools import get_tqdm
        from collections import defaultdict

        conn = self.conn  # ensure schema migrations

        # Fast path: skip detection, just run the selected apply-update using
        # whatever lang_detected values are already stored.
        if only_apply:
            if not (apply or apply_conservative):
                raise ValueError("--only-apply requires --apply or --apply-conservative")
            return self._apply_detected_langs(
                apply=apply, apply_conservative=apply_conservative,
            )

        # Stream freqs from ClickHouse text_freqs in Arrow batches.
        from lltk.tools.db_adapter import get_adapter
        ch_url = os.environ.get(
            'LLTK_CLICKHOUSE_URL',
            'clickhouse://lltk:lltk@localhost:8123/lltk',
        )
        ch = get_adapter(ch_url)

        n_freqs = ch.query('SELECT COUNT(*) FROM lltk.text_freqs')[0][0]
        if n_freqs == 0:
            if log: log('lltk.text_freqs is empty — run `lltk db-freqs` first')
            return {}

        # Snapshot lang → lang_metadata (one-shot; only fills NULLs so re-runs
        # don't clobber a prior snapshot)
        conn.execute("UPDATE texts SET lang_metadata = lang WHERE lang_metadata IS NULL AND lang IS NOT NULL")

        # Invert function-word list: word → tuple of langs (most words belong to 1 lang)
        word_to_langs = {}
        for w, lg in function_words_table():
            word_to_langs.setdefault(w, []).append(lg)
        word_to_langs = {w: tuple(lgs) for w, lgs in word_to_langs.items()}
        langs = sorted({lg for lgs in word_to_langs.values() for lg in lgs})
        if log: log(f'Detecting against {len(langs)} languages: {", ".join(langs)} ({len(word_to_langs)} distinct words)')

        results = []
        pbar = get_tqdm(total=n_freqs, desc='Detecting languages', disable=not progress)
        try:
            # ClickHouse returns freqs as a ClickHouse Map column; Arrow
            # maps it to ListArray[Struct{'keys','values'}]. Streaming via
            # query_arrow_stream keeps memory bounded.
            with ch.client.query_arrow_stream(
                "SELECT _id, freqs FROM lltk.text_freqs"
            ) as stream:
                for arrow_batch in stream:
                    ids = arrow_batch.column('_id').to_pylist()
                    freqs_col = arrow_batch.column('freqs').to_pylist()
                    for _id, entries in zip(ids, freqs_col):
                        # ClickHouse Map → Python dict via arrow
                        if not entries:
                            results.append((_id, None, None, None))
                            continue
                        items = entries.items() if isinstance(entries, dict) else entries
                        total_tokens = 0
                        lang_hits = {}
                        for w, n in items:
                            total_tokens += n
                            lgs = word_to_langs.get(w)
                            if lgs:
                                for lg in lgs:
                                    lang_hits[lg] = lang_hits.get(lg, 0) + n
                        if total_tokens < min_tokens:
                            results.append((_id, None, None, None))
                            continue
                        if not lang_hits:
                            results.append((_id, 'unknown', 0.0, 0.0))
                            continue
                        top_lang = None
                        top_hits = 0
                        second_hits = 0
                        for lg, h in lang_hits.items():
                            if h > top_hits:
                                second_hits = top_hits
                                top_hits = h
                                top_lang = lg
                            elif h > second_hits:
                                second_hits = h
                        coverage = top_hits / total_tokens
                        confidence = top_hits / second_hits if second_hits > 0 else 999.0
                        if coverage < coverage_threshold or confidence < confidence_threshold:
                            results.append((_id, 'unknown', coverage, confidence))
                        else:
                            results.append((_id, top_lang, coverage, confidence))
                    pbar.update(len(ids))
            pbar.close()
        finally:
            pass

        if not results:
            if log: log('No texts processed')
            return {}

        # Bulk-write results back
        result_df = pd.DataFrame(results, columns=['_id', 'lang_detected', 'lang_coverage', 'lang_confidence'])
        conn.register('_ld_tmp', result_df)
        conn.execute("""
            UPDATE texts SET
                lang_detected = u.lang_detected,
                lang_coverage = u.lang_coverage,
                lang_confidence = u.lang_confidence
            FROM _ld_tmp u WHERE texts._id = u._id
        """)
        conn.unregister('_ld_tmp')

        # Optionally apply detected lang → lang column
        if apply or apply_conservative:
            self._apply_detected_langs(apply=apply, apply_conservative=apply_conservative)

        # Stats
        dist = conn.execute("""
            SELECT lang_detected, COUNT(*) n
            FROM texts WHERE lang_detected IS NOT NULL
            GROUP BY 1 ORDER BY n DESC
        """).fetchdf()
        disagree = conn.execute("""
            SELECT lang_metadata, lang_detected, COUNT(*) n
            FROM texts
            WHERE lang_metadata IS NOT NULL
              AND lang_detected IS NOT NULL
              AND lang_detected != 'unknown'
              AND lang_metadata != lang_detected
            GROUP BY 1, 2
            ORDER BY n DESC
            LIMIT 20
        """).fetchdf()

        if log:
            log('Detection distribution:')
            log(dist.to_string(index=False))
            log('\nTop metadata ↔ detected disagreements:')
            log(disagree.to_string(index=False) if len(disagree) else '  (none)')

        return {
            'n_detected': len(results),
            'distribution': dist.set_index('lang_detected')['n'].to_dict(),
            'disagreements': disagree.to_dict(orient='records'),
        }

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
                        with gzip.open(abs_path, 'rb') as f:
                            d = orjson.loads(f.read())
                    else:
                        with open(abs_path, 'rb') as f:
                            d = orjson.loads(f.read())
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

    # ── Freqs DB ──────────────────────────────────────────────────

    def build_freqs_db(self, corpora=None, num_proc=None, batch_size=500,
                       truncate_first=False):
        """Ingest per-text freqs JSONs into ClickHouse `lltk.text_freqs`.

        Reads freqs JSON files in parallel Python workers, batches into
        pyarrow Map-typed tables, streams to ClickHouse via insert_arrow.
        No intermediate parquet, no central DuckDB — JSONs are the source
        of truth, ClickHouse is the runtime query engine.

        The `texts` table (in ClickHouse) provides the (_id, corpus,
        path_freqs) todo list. Ensure it's populated first via
        `lltk.tools.db_migrate.migrate_tables`.

        Args:
            corpora: list of corpus ids (default: all with path_freqs)
            num_proc: parallel JSON readers (default cpu_count - 2)
            batch_size: texts per insert batch (default 500)
            truncate_first: remove existing rows for these corpora first
        """
        from lltk.tools.db_adapter import get_adapter
        from lltk.tools.clickhouse_ingest import ingest_freqs_from_jsons
        ch_url = os.environ.get(
            'LLTK_CLICKHOUSE_URL',
            'clickhouse://lltk:lltk@localhost:8123/lltk',
        )
        ch = get_adapter(ch_url)
        return ingest_freqs_from_jsons(
            ch,
            corpora=corpora,
            batch_size=batch_size,
            num_proc=num_proc,
            truncate_first=truncate_first,
        )

    def read_freqs(self, ids=None, corpora=None, as_df=True):
        """Read freqs rows from ClickHouse `lltk.text_freqs`.

        Args:
            ids: iterable of _id strings (exact match). None = all.
            corpora: list of corpus ids to filter. None = all.
            as_df: return pandas DataFrame (True) or list of tuples (False)

        Returns rows of (_id, corpus, freqs). `freqs` comes back as a Python
        dict[str, int].
        """
        from lltk.tools.db_adapter import get_adapter
        ch_url = os.environ.get(
            'LLTK_CLICKHOUSE_URL',
            'clickhouse://lltk:lltk@localhost:8123/lltk',
        )
        ch = get_adapter(ch_url)

        wheres = []
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            wheres.append(f"corpus IN ({cl})")
        if ids is not None:
            ids = list(ids)
            il = ', '.join(f"'{i}'" for i in ids)
            wheres.append(f"_id IN ({il})")
        where_sql = f"WHERE {' AND '.join(wheres)}" if wheres else ''
        sql = f"SELECT _id, corpus, freqs FROM lltk.text_freqs {where_sql}"

        if as_df:
            return ch.query_df(sql)
        return ch.query(sql)


    # ── Passages DB ────────────────────────────────────────────────

    def build_passages_db(self, n=500, num_proc=None, corpora=None, force=False):
        """Build SQLite passages DB with FTS5 from corpus txt files.

        Chunks each text into ~n-word passages using sentence-aware splitting
        (language-specific punkt tokenizer). No DuckDB dependency — reads
        directly from corpus txt files.

        Args:
            n: target words per passage (default 500)
            num_proc: parallel workers (default cpu_count - 2)
            corpora: list of corpus IDs to process (default: all with txt)
            force: rebuild from scratch
        """
        import sqlite3
        import time

        if num_proc is None:
            num_proc = max(1, os.cpu_count() - 2)

        db_path = PATH_PASSAGESDB
        if force and os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS passages (
                _id     TEXT NOT NULL,
                seq     INTEGER NOT NULL,
                text    TEXT NOT NULL,
                n_words INTEGER NOT NULL,
                lang    TEXT,
                PRIMARY KEY (_id, seq)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS passages_meta (
                _id         TEXT PRIMARY KEY,
                corpus      TEXT NOT NULL,
                n_passages  INTEGER NOT NULL
            )
        """)
        conn.commit()

        # Gather texts to process
        from lltk.corpus.utils import load as load_corpus
        from lltk.corpus.synthetic import SyntheticCorpus

        if corpora is None:
            from lltk.corpus.utils import get_inducted_corpus_ids
            corpora = get_inducted_corpus_ids()

        # Find already-processed _ids
        done_ids = set(
            r[0] for r in conn.execute("SELECT _id FROM passages_meta").fetchall()
        )
        if done_ids and not force:
            if log: log(f'{len(done_ids)} texts already in passages DB')

        all_tasks = []  # (corpus_id, text_id, txt_path, lang)
        for cid in corpora:
            if cid in DB_BLACKLIST:
                continue
            try:
                corpus = load_corpus(cid)
            except Exception:
                continue
            if isinstance(corpus, SyntheticCorpus):
                continue
            if not hasattr(corpus, 'path_txt') or not os.path.isdir(getattr(corpus, 'path_txt', '')):
                continue

            manifest_lang = getattr(corpus, 'lang', None)
            try:
                meta = corpus.load_metadata()
            except Exception:
                continue
            if meta is None or not len(meta):
                continue

            for text_id in meta.index:
                _id = f'_{cid}/{text_id}'
                if _id in done_ids:
                    continue
                t = corpus.text(text_id)
                txt_path = getattr(t, 'path_txt', None)
                if not txt_path or not os.path.exists(txt_path):
                    continue
                # Get lang from text meta
                lang = None
                if t._meta:
                    for key in ('lang', 'language', 'language_1', 'estc_lang'):
                        val = t._meta.get(key)
                        if val and str(val).strip() and str(val) != 'nan':
                            lang = normalize_lang(str(val).strip())
                            if lang:
                                break
                if not lang and manifest_lang:
                    lang = normalize_lang(manifest_lang)

                all_tasks.append((_id, cid, txt_path, lang, n))

        if not all_tasks:
            if log: log('No new texts to process.')
            return

        if log: log(f'{len(all_tasks)} texts to chunk into passages (n={n}, workers={num_proc})')

        t0 = time.time()
        from lltk.tools.tools import pmap

        batch_size = 200
        total_passages = 0
        for i in range(0, len(all_tasks), batch_size):
            batch = all_tasks[i:i + batch_size]
            results = pmap(
                _chunk_text_to_passages,
                batch,
                num_proc=num_proc,
                desc=f'[passages] {i}/{len(all_tasks)}',
                use_threads=True,
            )
            # Insert batch
            for _id, corpus_id, passages_list in results:
                if not passages_list:
                    continue
                conn.executemany(
                    "INSERT OR IGNORE INTO passages (_id, seq, text, n_words, lang) VALUES (?, ?, ?, ?, ?)",
                    passages_list,
                )
                conn.execute(
                    "INSERT OR IGNORE INTO passages_meta (_id, corpus, n_passages) VALUES (?, ?, ?)",
                    (_id, corpus_id, len(passages_list)),
                )
                total_passages += len(passages_list)
            conn.commit()

        elapsed = time.time() - t0
        if log: log(f'Built {total_passages:,} passages from {len(all_tasks)} texts in {elapsed:.0f}s')

        # Build FTS5 index
        if log: log('Building FTS5 index...')
        t1 = time.time()
        conn.execute("DROP TABLE IF EXISTS passages_fts")
        conn.execute("""
            CREATE VIRTUAL TABLE passages_fts USING fts5(
                _id, text,
                content='passages', content_rowid='rowid'
            )
        """)
        conn.execute("""
            INSERT INTO passages_fts (rowid, _id, text)
            SELECT rowid, _id, text FROM passages
        """)
        conn.commit()
        if log: log(f'FTS5 index built in {time.time() - t1:.0f}s')

        total = conn.execute("SELECT COUNT(*) FROM passages").fetchone()[0]
        if log: log(f'passages DB: {total:,} passages total')
        conn.close()

    # ── Word Index ────────────────────────────────────────────────

    def build_word_index_sql(self, vocab_size=50_000, min_count=1, corpora=None,
                             memory_limit='24GB', threads=None):
        """Build word_year_corpus + year_corpus_totals tables from ClickHouse
        lltk.text_freqs. Requires `build_freqs_db()` to have populated it.

            lltk.db.build_word_index_sql()                     # all texts, top 50K words
            lltk.db.build_word_index_sql(vocab_size=100_000)
            lltk.db.build_word_index_sql(corpora=['ecco_tcp'])
        """
        import time
        wi_conn = duckdb.connect(self.wordindex_path)
        wi_conn.execute(f"SET memory_limit='{memory_limit}'")
        wi_conn.execute("SET preserve_insertion_order=false")
        if threads:
            wi_conn.execute(f"SET threads={threads}")
        wi_conn.execute(f"ATTACH '{self.path}' AS main_db (READ_ONLY)")
        wi_conn.execute(f"ATTACH '{self.match_path}' AS match_db (READ_ONLY)")

        corpus_filter_sql = ''
        if corpora:
            cl = ', '.join(f"'{c}'" for c in corpora)
            corpus_filter_sql = f"AND t.corpus IN ({cl})"

        # Texts with both metadata (year/corpus) AND freqs in DB. Adds is_preferred for dedup.
        # Valid-word filter: first char alpha, rest alpha or in [-']. Done in SQL via regex.
        word_regex = r"^[a-zA-Z][a-zA-Z''\-]*$"

        print('Building texts view (joining metadata + match_groups)...')
        # Filter to texts with freqs via path_freqs — avoids a cross-DB
        # round-trip to ClickHouse. Any text with a freqs JSON on disk
        # will have been ingested by `lltk db-freqs`.
        wi_conn.execute(f"""
            CREATE OR REPLACE TEMP VIEW _texts AS
            SELECT t._id, t.year, t.corpus, t.genre, COALESCE(t.n_words, 0) AS n_words,
                   CASE WHEN mg.rank IS NULL OR mg.rank = 0 THEN TRUE ELSE FALSE END AS is_preferred
            FROM main_db.texts t
            LEFT JOIN match_db.match_groups mg ON t._id = mg._id
            WHERE t.year IS NOT NULL
              AND t.path_freqs IS NOT NULL
              {corpus_filter_sql}
        """)
        n_texts = wi_conn.execute("SELECT COUNT(*) FROM _texts").fetchone()[0]
        n_pref = wi_conn.execute("SELECT COUNT(*) FROM _texts WHERE is_preferred").fetchone()[0]
        print(f'{n_texts:,} texts ({n_pref:,} preferred)')
        if not n_texts:
            wi_conn.close()
            return

        # Load all text metadata into Python for chunking (1.6M rows ≈ 120MB)
        all_texts = wi_conn.execute("""
            SELECT _id, year, corpus, genre, is_preferred FROM _texts
        """).fetchall()

        import tempfile, shutil
        from concurrent.futures import ProcessPoolExecutor, as_completed

        num_proc = max(1, os.cpu_count() - 2)
        chunk_size = 2_000   # ~50M word/count pairs, ~few hundred MB per worker

        batches = [all_texts[i:i+chunk_size] for i in range(0, len(all_texts), chunk_size)]
        n_chunks = len(batches)
        print(f'Partitioned into {n_chunks} chunks of up to {chunk_size:,} texts')

        shard_dir = tempfile.mkdtemp(prefix='wi_sql_shards_',
                                     dir=os.path.dirname(self.wordindex_path))
        print(f'Writing parquet shards to {shard_dir}')

        try:
            # ── Pass 1: vocabulary (top N by total occurrences) ──
            vocab_full_path = os.path.join(os.path.dirname(self.wordindex_path),
                                           f'wordindex_vocab_{vocab_size}.tsv')
            if os.path.exists(vocab_full_path):
                print(f'Loading vocab from {vocab_full_path}')
                vocab = []
                with open(vocab_full_path) as fp:
                    for line in fp:
                        parts = line.rstrip('\n').split('\t')
                        if len(parts) == 2:
                            vocab.append(parts[0])
                        if len(vocab) >= vocab_size:
                            break
                vocab_set = frozenset(vocab)
            else:
                from lltk.tools.tools import get_tqdm
                print(f'Pass 1: vocabulary build ({num_proc} workers, {n_chunks} chunks)...')
                t0 = time.time()
                ch_url = os.environ.get(
                    'LLTK_CLICKHOUSE_URL',
                    'clickhouse://lltk:lltk@localhost:8123/lltk',
                )
                args_list = [
                    (ch_url, b, min_count, shard_dir, i, None)
                    for i, b in enumerate(batches)
                ]
                with ProcessPoolExecutor(max_workers=num_proc) as pool:
                    futures = [pool.submit(_wi_agg_batch_from_db, a) for a in args_list]
                    pbar = get_tqdm(total=len(all_texts), desc='Pass 1: vocab')
                    for fut in as_completed(futures):
                        n, _, _ = fut.result()
                        pbar.update(n)
                    pbar.close()

                # Merge vocab shards via DuckDB and pick top-N
                vocab_glob = os.path.join(shard_dir, 'vocab_*.parquet')
                print(f'  merging vocab shards...')
                top = wi_conn.execute(f"""
                    SELECT word, SUM(cnt)::BIGINT AS cnt
                    FROM read_parquet('{vocab_glob}')
                    GROUP BY word
                    ORDER BY cnt DESC
                    LIMIT {vocab_size}
                """).fetchall()
                with open(vocab_full_path, 'w') as fp:
                    for w, c in top:
                        fp.write(f'{w}\t{c}\n')
                vocab_set = frozenset(w for w, _ in top)
                print(f'  vocab built in {time.time()-t0:.1f}s; saved {len(top):,} words')

                # Clean vocab shards (big); keep only Pass 2 wyc shards
                import glob as _glob
                for vp in _glob.glob(vocab_glob):
                    os.remove(vp)

            # ── Pass 2: word_year_corpus ──
            wi_conn.execute("DROP TABLE IF EXISTS word_year_corpus")
            wi_conn.execute("DROP TABLE IF EXISTS year_corpus_totals")

            from lltk.tools.tools import get_tqdm
            print(f'Pass 2: word_year_corpus ({num_proc} workers, {n_chunks} chunks)...')
            t0 = time.time()
            ch_url = os.environ.get(
                'LLTK_CLICKHOUSE_URL',
                'clickhouse://lltk:lltk@localhost:8123/lltk',
            )
            args_list = [
                (ch_url, b, min_count, shard_dir, i, vocab_set)
                for i, b in enumerate(batches)
            ]
            with ProcessPoolExecutor(max_workers=num_proc) as pool:
                futures = [pool.submit(_wi_agg_batch_from_db, a) for a in args_list]
                pbar = get_tqdm(total=len(all_texts), desc='Pass 2: agg')
                for fut in as_completed(futures):
                    n, _, _ = fut.result()
                    pbar.update(n)
                pbar.close()

            # Batched merge: SUM-reduce shards into a staging table in groups of N,
            # then final SUM-reduce into word_year_corpus. Keeps peak memory bounded.
            import glob as _glob
            shard_paths = sorted(_glob.glob(os.path.join(shard_dir, 'wyc_*.parquet')))
            if not shard_paths:
                raise RuntimeError(f'No Pass 2 shards found in {shard_dir}')
            print(f'  merging {len(shard_paths)} shards (batched)...')
            tm = time.time()

            wi_conn.execute("DROP TABLE IF EXISTS _wyc_stage")
            wi_conn.execute("""
                CREATE TABLE _wyc_stage (
                    word TEXT, year INTEGER, corpus TEXT, genre TEXT,
                    word_count BIGINT, n_texts INTEGER,
                    word_count_dedup BIGINT, n_texts_dedup INTEGER
                )
            """)

            merge_batch = 50
            for i in range(0, len(shard_paths), merge_batch):
                chunk = shard_paths[i:i+merge_batch]
                file_list = ', '.join(f"'{p}'" for p in chunk)
                wi_conn.execute(f"""
                    INSERT INTO _wyc_stage
                    SELECT word, year, corpus, genre,
                           SUM(word_count)::BIGINT,
                           SUM(n_texts)::INTEGER,
                           SUM(word_count_dedup)::BIGINT,
                           SUM(n_texts_dedup)::INTEGER
                    FROM read_parquet([{file_list}])
                    GROUP BY word, year, corpus, genre
                """)
                print(f'    merged {min(i+merge_batch, len(shard_paths))}/{len(shard_paths)} shards '
                      f'({time.time()-tm:.0f}s)')

            print(f'  final GROUP BY to word_year_corpus...')
            wi_conn.execute("""
                CREATE TABLE word_year_corpus AS
                SELECT word, year, corpus, genre,
                       SUM(word_count)::BIGINT AS word_count,
                       SUM(n_texts)::INTEGER AS n_texts,
                       SUM(word_count_dedup)::BIGINT AS word_count_dedup,
                       SUM(n_texts_dedup)::INTEGER AS n_texts_dedup
                FROM _wyc_stage
                GROUP BY word, year, corpus, genre
            """)
            wi_conn.execute("DROP TABLE IF EXISTS _wyc_stage")
            print(f'  merge: {time.time()-tm:.1f}s; pass 2 total {time.time()-t0:.1f}s')

            # Clean up shards only on success. Failures (OOM, Ctrl+C) keep the
            # shards so we can retry just the merge instead of redoing Pass 2.
            shutil.rmtree(shard_dir, ignore_errors=True)
            shard_dir_cleaned = True
        except Exception:
            print(f'\nFAILED — parquet shards preserved at {shard_dir}')
            print(f'To retry just the merge, call lltk.db.merge_wi_shards("{shard_dir}")')
            raise
        finally:
            pass  # no unconditional cleanup — preserved on failure for retry

        print('Aggregating year_corpus_totals...')
        t0 = time.time()
        wi_conn.execute("""
            CREATE TABLE year_corpus_totals AS
            SELECT t.year, t.corpus, t.genre,
                   COUNT(*)::INTEGER AS n_texts,
                   SUM(t.n_words)::BIGINT AS total_words,
                   SUM(CASE WHEN t.is_preferred THEN 1 ELSE 0 END)::INTEGER AS n_texts_dedup,
                   SUM(CASE WHEN t.is_preferred THEN t.n_words ELSE 0 END)::BIGINT AS total_words_dedup
            FROM _texts t
            GROUP BY t.year, t.corpus, t.genre
        """)
        print(f'  year_corpus_totals built in {time.time()-t0:.1f}s')

    def merge_wi_shards(self, shard_dir, memory_limit='24GB', merge_batch=50):
        """Retry just the batched merge of preserved Pass 2 shards.

        Use after build_word_index_sql fails in the merge step — avoids redoing
        the 30-60 min Pass 2 worker phase.
        """
        import time, glob as _glob
        shard_paths = sorted(_glob.glob(os.path.join(shard_dir, 'wyc_*.parquet')))
        if not shard_paths:
            raise RuntimeError(f'No wyc_*.parquet shards in {shard_dir}')
        print(f'Merging {len(shard_paths)} shards from {shard_dir} (batch={merge_batch})')
        wi_conn = duckdb.connect(self.wordindex_path)
        wi_conn.execute(f"SET memory_limit='{memory_limit}'")
        wi_conn.execute("SET preserve_insertion_order=false")
        wi_conn.execute("DROP TABLE IF EXISTS word_year_corpus")
        wi_conn.execute("DROP TABLE IF EXISTS _wyc_stage")
        wi_conn.execute("""
            CREATE TABLE _wyc_stage (
                word TEXT, year INTEGER, corpus TEXT, genre TEXT,
                word_count BIGINT, n_texts INTEGER,
                word_count_dedup BIGINT, n_texts_dedup INTEGER
            )
        """)
        t0 = time.time()
        for i in range(0, len(shard_paths), merge_batch):
            chunk = shard_paths[i:i+merge_batch]
            file_list = ', '.join(f"'{p}'" for p in chunk)
            wi_conn.execute(f"""
                INSERT INTO _wyc_stage
                SELECT word, year, corpus, genre,
                       SUM(word_count)::BIGINT, SUM(n_texts)::INTEGER,
                       SUM(word_count_dedup)::BIGINT, SUM(n_texts_dedup)::INTEGER
                FROM read_parquet([{file_list}])
                GROUP BY word, year, corpus, genre
            """)
            print(f'  merged {min(i+merge_batch, len(shard_paths))}/{len(shard_paths)} '
                  f'({time.time()-t0:.0f}s)')
        print('Final GROUP BY to word_year_corpus...')
        wi_conn.execute("""
            CREATE TABLE word_year_corpus AS
            SELECT word, year, corpus, genre,
                   SUM(word_count)::BIGINT AS word_count,
                   SUM(n_texts)::INTEGER AS n_texts,
                   SUM(word_count_dedup)::BIGINT AS word_count_dedup,
                   SUM(n_texts_dedup)::INTEGER AS n_texts_dedup
            FROM _wyc_stage
            GROUP BY word, year, corpus, genre
        """)
        wi_conn.execute("DROP TABLE IF EXISTS _wyc_stage")
        n = wi_conn.execute("SELECT COUNT(*) FROM word_year_corpus").fetchone()[0]
        print(f'word_year_corpus: {n:,} rows ({time.time()-t0:.1f}s total)')
        wi_conn.close()

        n_agg = wi_conn.execute("SELECT COUNT(*) FROM word_year_corpus").fetchone()[0]
        n_tot = wi_conn.execute("SELECT COUNT(*) FROM year_corpus_totals").fetchone()[0]
        print(f'word_year_corpus: {n_agg:,} rows')
        print(f'year_corpus_totals: {n_tot:,} rows')

        wi_conn.close()

    def build_word_index(self, num_proc=None, progress=True, min_count=1,
                         vocab_size=50_000, corpora=None):
        """Build word frequency aggregate tables from freqs files.

        Two-pass build:
          Pass 1: scan all texts to build vocabulary (top N words by doc frequency)
          Pass 2: group texts by (year, corpus, genre), read freqs, aggregate directly

        Produces two small tables (no per-text detail table):
          word_year_corpus: word × year × corpus × genre → word_count, n_texts
          year_corpus_totals: year × corpus × genre → n_texts, total_words

            lltk.db.build_word_index()                      # all texts, top 50K words
            lltk.db.build_word_index(vocab_size=25_000)     # smaller vocab
            lltk.db.build_word_index(corpora=['ecco_tcp'])  # specific corpora
        """
        from lltk.tools.tools import get_tqdm
        from collections import Counter, defaultdict
        from concurrent.futures import ProcessPoolExecutor
        import concurrent.futures
        import random

        if num_proc is None:
            num_proc = max(1, os.cpu_count() - 2)

        from lltk.imports import PATH_CORPUS
        corpus_root = os.path.expanduser(PATH_CORPUS)

        wi_conn = duckdb.connect(self.wordindex_path)
        wi_conn.execute("SET memory_limit='16GB'")
        wi_conn.execute("SET preserve_insertion_order=false")
        wi_conn.execute(f"ATTACH '{self.path}' AS main_db (READ_ONLY)")

        corpus_filter = ''
        if corpora:
            corpus_list = ', '.join(f"'{c}'" for c in corpora)
            corpus_filter = f'AND t.corpus IN ({corpus_list})'

        # All texts with freqs + metadata
        # Also load match group rank for dedup support
        wi_conn.execute(f"ATTACH '{self.match_path}' AS match_db (READ_ONLY)")
        all_texts = wi_conn.execute(f"""
            SELECT t._id, t.path_freqs, t.year, t.corpus, t.genre, t.n_words,
                   CASE WHEN mg.rank IS NULL OR mg.rank = 0 THEN TRUE ELSE FALSE END as is_preferred
            FROM main_db.texts t
            LEFT JOIN match_db.match_groups mg ON t._id = mg._id
            WHERE t.path_freqs IS NOT NULL AND t.year IS NOT NULL {corpus_filter}
        """).fetchdf()

        if not len(all_texts):
            if log: log('No texts with freqs found')
            return

        n_preferred = all_texts['is_preferred'].sum()
        if log: log(f'{len(all_texts):,} texts ({n_preferred:,} preferred / deduped)')

        all_pairs = list(zip(all_texts['_id'], all_texts['path_freqs']))
        random.shuffle(all_pairs)

        def _bounded_map(pool, func, items, desc='', max_pending=None):
            if max_pending is None:
                max_pending = num_proc * 4
            pending = set()
            items_iter = iter(items)
            total = len(items)
            pbar = get_tqdm(total=total, desc=desc) if progress else None
            exhausted = False
            while pending or not exhausted:
                while len(pending) < max_pending and not exhausted:
                    try:
                        item = next(items_iter)
                        pending.add(pool.submit(func, item))
                    except StopIteration:
                        exhausted = True
                if pending:
                    done, pending = concurrent.futures.wait(
                        pending, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for f in done:
                        yield f.result()
                        if pbar:
                            pbar.update(1)
            if pbar:
                pbar.close()

        # ── Pass 1: Build or load vocabulary ────────────────────────
        vocab_full_path = os.path.join(os.path.dirname(self.wordindex_path), f'wordindex_vocab_{vocab_size}.tsv')

        if os.path.exists(vocab_full_path):
            all_vocab = []
            with open(vocab_full_path) as f:
                for line in f:
                    parts = line.strip().split('\t')
                    if len(parts) == 2:
                        all_vocab.append((parts[0], int(parts[1])))
            vocab = frozenset(w for w, _ in all_vocab[:vocab_size])
            if log: log(f'Loaded vocabulary from {vocab_full_path} ({len(all_vocab):,} total, using top {len(vocab):,})')
        else:
            if log: log(f'Pass 1: Building vocabulary from {len(all_pairs):,} texts ({num_proc} processes)...')

            doc_freq = Counter()
            pass1_batch_size = 500
            pass1_batches = [
                [rp for _, rp in all_pairs[i:i+pass1_batch_size]]
                for i in range(0, len(all_pairs), pass1_batch_size)
            ]
            pass1_args = [(corpus_root, b, min_count) for b in pass1_batches]

            from lltk.tools.tools import get_tqdm
            with ProcessPoolExecutor(max_workers=num_proc) as pool:
                pbar = get_tqdm(total=len(all_pairs), desc='Pass 1: vocabulary') if progress else None
                pending = set()
                items_iter = iter(pass1_args)
                max_pending = num_proc * 4
                exhausted = False
                while pending or not exhausted:
                    while len(pending) < max_pending and not exhausted:
                        try:
                            pending.add(pool.submit(_wi_pass1_batch, next(items_iter)))
                        except StopIteration:
                            exhausted = True
                    if pending:
                        done, pending = concurrent.futures.wait(
                            pending, return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        for f in done:
                            n, local = f.result()
                            doc_freq.update(local)
                            if pbar:
                                pbar.update(n)
                if pbar:
                    pbar.close()

            ranked = doc_freq.most_common(vocab_size)
            vocab = frozenset(w for w, _ in ranked)
            if log:
                log(f'Vocabulary: {len(doc_freq):,} unique words → top {len(vocab):,} kept')
                if ranked:
                    log(f'  Min doc frequency in vocab: {ranked[-1][1]}')

            with open(vocab_full_path, 'w') as f:
                for w, df in ranked:
                    f.write(f'{w}\t{df}\n')
            if log: log(f'Saved vocabulary to {vocab_full_path}')
            del doc_freq

        # ── Pass 2: Batched per-worker aggregation → parquet shards → DuckDB merge ───
        if log: log(f'Pass 2: Aggregating {len(all_texts):,} texts ({num_proc} processes)...')

        vocab_set = frozenset(vocab)

        # Create tables (with dedup columns)
        wi_conn.execute("DROP TABLE IF EXISTS word_year_corpus")
        wi_conn.execute("""
            CREATE TABLE word_year_corpus (
                word TEXT NOT NULL, year INTEGER NOT NULL,
                corpus TEXT NOT NULL, genre TEXT,
                word_count BIGINT NOT NULL, n_texts INTEGER NOT NULL,
                word_count_dedup BIGINT NOT NULL, n_texts_dedup INTEGER NOT NULL
            )
        """)
        wi_conn.execute("DROP TABLE IF EXISTS year_corpus_totals")
        wi_conn.execute("""
            CREATE TABLE year_corpus_totals (
                year INTEGER NOT NULL, corpus TEXT NOT NULL,
                genre TEXT, n_texts INTEGER NOT NULL,
                total_words BIGINT NOT NULL,
                n_texts_dedup INTEGER NOT NULL,
                total_words_dedup BIGINT NOT NULL
            )
        """)

        # Assemble self-contained batches (everything each worker needs)
        text_records = [
            (row['_id'], row['path_freqs'],
             int(row['year']), row['corpus'], row['genre'],
             int(row['n_words']) if pd.notna(row['n_words']) else 0,
             bool(row['is_preferred']))
            for _, row in all_texts.iterrows()
        ]
        random.shuffle(text_records)

        batch_size = 500
        batches = [text_records[i:i+batch_size]
                   for i in range(0, len(text_records), batch_size)]

        import tempfile, shutil
        shard_dir = tempfile.mkdtemp(prefix='wi_shards_',
                                     dir=os.path.dirname(self.wordindex_path))
        if log: log(f'Writing parquet shards to {shard_dir}')

        try:
            pass2_args = [(corpus_root, b, min_count, shard_dir, i)
                          for i, b in enumerate(batches)]

            with ProcessPoolExecutor(max_workers=num_proc,
                                     initializer=_wi_init,
                                     initargs=(vocab_set,)) as pool:
                # Process results as they arrive; batched progress (texts, not batches)
                from lltk.tools.tools import get_tqdm
                pbar = get_tqdm(total=len(text_records), desc='Pass 2: aggregating') if progress else None
                pending = set()
                items_iter = iter(pass2_args)
                max_pending = num_proc * 4
                exhausted = False
                while pending or not exhausted:
                    while len(pending) < max_pending and not exhausted:
                        try:
                            pending.add(pool.submit(_wi_aggregate_batch, next(items_iter)))
                        except StopIteration:
                            exhausted = True
                    if pending:
                        done, pending = concurrent.futures.wait(
                            pending, return_when=concurrent.futures.FIRST_COMPLETED
                        )
                        for f in done:
                            n, _, _ = f.result()
                            if pbar:
                                pbar.update(n)
                if pbar:
                    pbar.close()

            # ── Final merge via DuckDB (parallel GROUP BY across shards) ──
            words_glob = os.path.join(shard_dir, 'words_*.parquet')
            totals_glob = os.path.join(shard_dir, 'totals_*.parquet')

            if log: log('Merging word shards via DuckDB GROUP BY...')
            wi_conn.execute(f"""
                INSERT INTO word_year_corpus
                SELECT word, year, corpus, genre,
                       SUM(word_count)::BIGINT, SUM(n_texts)::INTEGER,
                       SUM(word_count_dedup)::BIGINT, SUM(n_texts_dedup)::INTEGER
                FROM read_parquet('{words_glob}')
                GROUP BY word, year, corpus, genre
            """)

            if log: log('Merging totals shards via DuckDB GROUP BY...')
            wi_conn.execute(f"""
                INSERT INTO year_corpus_totals
                SELECT year, corpus, genre,
                       SUM(n_texts)::INTEGER, SUM(total_words)::BIGINT,
                       SUM(n_texts_dedup)::INTEGER, SUM(total_words_dedup)::BIGINT
                FROM read_parquet('{totals_glob}')
                GROUP BY year, corpus, genre
            """)
        finally:
            shutil.rmtree(shard_dir, ignore_errors=True)

        n_agg = wi_conn.execute("SELECT COUNT(*) FROM word_year_corpus").fetchone()[0]
        n_tot = wi_conn.execute("SELECT COUNT(*) FROM year_corpus_totals").fetchone()[0]
        if log: log(f'word_year_corpus: {n_agg:,} rows')
        if log: log(f'year_corpus_totals: {n_tot:,} rows')

        wi_conn.close()

    def build_word_aggregates(self, progress=True):
        """Build aggregate tables from existing word_index by streaming per-word.

        Avoids the impossible GROUP BY on 5B rows by iterating through words
        and aggregating in Python.

            lltk.db.build_word_aggregates()
        """
        from lltk.tools.tools import get_tqdm
        from collections import defaultdict
        import duckdb

        wi_conn = duckdb.connect(self.wordindex_path)
        wi_conn.execute("SET memory_limit='16GB'")
        wi_conn.execute(f"ATTACH '{self.path}' AS main_db (READ_ONLY)")

        # Load text metadata lookup
        if log: log('Loading text metadata...')
        meta_df = wi_conn.execute("""
            SELECT t._id, t.year, t.corpus, t.genre, t.n_words
            FROM main_db.texts t
            WHERE t.year IS NOT NULL AND t.n_words IS NOT NULL
        """).fetchdf()
        text_meta = {}
        for _, row in meta_df.iterrows():
            text_meta[row['_id']] = (int(row['year']), row['corpus'], row['genre'], int(row['n_words']))
        if log: log(f'Loaded metadata for {len(text_meta):,} texts')
        del meta_df

        # Single sequential scan — stream all rows, accumulate in Python
        agg = defaultdict(lambda: [0, 0])  # (word, year, corpus, genre) → [word_count, n_texts]
        totals = defaultdict(lambda: [0, 0])  # (year, corpus, genre) → [n_texts, total_words]
        seen_ids = set()

        n_total = wi_conn.execute("SELECT COUNT(*) FROM word_index").fetchone()[0]
        if log: log(f'Scanning {n_total:,} rows...')

        # Fetch in chunks to avoid materializing 5B rows in memory
        chunk_size = 10_000_000
        pbar = get_tqdm(total=n_total, desc='Aggregating') if progress else None
        offset = 0
        while True:
            # Use rowid range for chunked scan (no ORDER BY needed)
            chunk = wi_conn.execute(f"""
                SELECT _id, word, count FROM word_index
                LIMIT {chunk_size} OFFSET {offset}
            """).fetchall()
            if not chunk:
                break

            for _id, word, count in chunk:
                meta = text_meta.get(_id)
                if not meta:
                    continue
                year, corpus, genre, n_words = meta
                key = (word, year, corpus, genre)
                agg[key][0] += count
                agg[key][1] += 1
                if _id not in seen_ids:
                    seen_ids.add(_id)
                    totals[(year, corpus, genre)][0] += 1
                    totals[(year, corpus, genre)][1] += n_words

            if pbar:
                pbar.update(len(chunk))
            offset += chunk_size

        if pbar:
            pbar.close()

        if log: log(f'Aggregate: {len(agg):,} groups, Totals: {len(totals):,} groups')

        # Write aggregate table
        if log: log('Writing word_year_corpus...')
        wi_conn.execute("DROP TABLE IF EXISTS word_year_corpus")
        wi_conn.execute("""
            CREATE TABLE word_year_corpus (
                word TEXT NOT NULL, year INTEGER NOT NULL,
                corpus TEXT NOT NULL, genre TEXT,
                word_count BIGINT NOT NULL, n_texts INTEGER NOT NULL
            )
        """)
        agg_rows = [(w, y, c, g, wc, nt) for (w, y, c, g), (wc, nt) in agg.items()]
        for j in range(0, len(agg_rows), 500_000):
            batch = agg_rows[j:j+500_000]
            agg_df = pd.DataFrame(batch, columns=['word', 'year', 'corpus', 'genre', 'word_count', 'n_texts'])
            wi_conn.execute("INSERT INTO word_year_corpus SELECT * FROM agg_df")
        if log: log(f'word_year_corpus: {len(agg_rows):,} rows')
        del agg, agg_rows

        # Write totals table
        if log: log('Writing year_corpus_totals...')
        wi_conn.execute("DROP TABLE IF EXISTS year_corpus_totals")
        wi_conn.execute("""
            CREATE TABLE year_corpus_totals (
                year INTEGER NOT NULL, corpus TEXT NOT NULL,
                genre TEXT, n_texts INTEGER NOT NULL,
                total_words BIGINT NOT NULL
            )
        """)
        tot_rows = [(y, c, g, nt, tw) for (y, c, g), (nt, tw) in totals.items()]
        tot_df = pd.DataFrame(tot_rows, columns=['year', 'corpus', 'genre', 'n_texts', 'total_words'])
        wi_conn.execute("INSERT INTO year_corpus_totals SELECT * FROM tot_df")
        if log: log(f'year_corpus_totals: {len(tot_rows):,} rows')

        wi_conn.close()

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
        """Check if word frequency aggregate tables exist."""
        try:
            n = self.conn.execute("SELECT COUNT(*) FROM wi_db.word_year_corpus").fetchone()[0]
            return n > 0
        except Exception:
            return False

    # ── Ngram queries ──────────────────────────────────────────────

    def _ngram_where(self, genre=None, corpus=None, year_min=None, year_max=None):
        """Build WHERE clauses for ngram queries."""
        clauses = []
        if genre:
            clauses.append(f"t.genre = '{genre}'")
        if corpus:
            clauses.append(f"t.corpus = '{corpus}'")
        if year_min is not None:
            clauses.append(f't.year >= {int(year_min)}')
        if year_max is not None:
            clauses.append(f't.year <= {int(year_max)}')
        return ' AND '.join(clauses) if clauses else '1=1'

    def _ngram_dedup(self, where, dedup=False, dedup_by='rank'):
        """Return (join_clause, where_clause) for dedup in ngram queries."""
        if not dedup:
            return '', ''
        join = 'LEFT JOIN match_db.match_groups mg ON t._id = mg._id'
        dedup_clause = self._dedup_sql(where, dedup_by=dedup_by)
        return join, dedup_clause

    def ngram(self, words, genre=None, corpus=None, year_min=1500, year_max=2020,
              normalize='per_million', by='decade', dedup=False, dedup_by='rank',
              by_corpus=False):
        """Query word frequency over time using pre-aggregated table.

            lltk.db.ngram('virtue')
            lltk.db.ngram(['virtue', 'honor'], genre='Fiction', dedup=True)
            lltk.db.ngram('virtue', by_corpus=True)
        """
        if isinstance(words, str):
            words = [w.strip().lower() for w in words.split(',')]
        else:
            words = [w.lower() for w in words]

        word_list = ', '.join("'" + w.replace("'", "''") + "'" for w in words)

        if by == 'decade':
            time_expr = 'CAST(a.year / 10 AS INTEGER) * 10'
        elif by == 'year':
            time_expr = 'a.year'
        else:
            time_expr = f'CAST(a.year / {int(by)} AS INTEGER) * {int(by)}'

        # Column suffixes for dedup
        wc_col = 'word_count_dedup' if dedup else 'word_count'
        nt_col = 'n_texts_dedup' if dedup else 'n_texts'
        tw_col = 'total_words_dedup' if dedup else 'total_words'

        wheres = [f'a.word IN ({word_list})']
        if genre:
            wheres.append(f"a.genre = '{genre}'")
        if corpus:
            wheres.append(f"a.corpus = '{corpus}'")
        if year_min is not None:
            wheres.append(f'a.year >= {int(year_min)}')
        if year_max is not None:
            wheres.append(f'a.year <= {int(year_max)}')
        where = ' AND '.join(wheres)

        corpus_col = ', a.corpus' if by_corpus else ''
        corpus_group = ', a.corpus' if by_corpus else ''

        # Totals filtering (same as word filter minus the word clause)
        tot_wheres = [c for c in wheres if not c.startswith('a.word')]
        tot_where = ' AND '.join(tot_wheres) if tot_wheres else '1=1'
        tot_where_t = tot_where.replace('a.', 'tot.')
        time_expr_t = time_expr.replace('a.', 'tot.')
        corpus_col_t = corpus_col.replace('a.', 'tot.')
        corpus_group_t = corpus_group.replace('a.', 'tot.')

        if normalize == 'per_million':
            sql = f"""
                WITH counts AS (
                    SELECT {time_expr} as period,
                           a.word
                           {corpus_col},
                           SUM(a.{wc_col}) as raw_count,
                           SUM(a.{nt_col}) as n_texts
                    FROM wi_db.word_year_corpus a
                    WHERE {where}
                    GROUP BY period, a.word {corpus_group}
                ),
                totals AS (
                    SELECT {time_expr_t} as period
                           {corpus_col_t},
                           SUM(tot.{tw_col}) as total_words
                    FROM wi_db.year_corpus_totals tot
                    WHERE {tot_where_t}
                    GROUP BY period {corpus_group_t}
                )
                SELECT c.period, c.word
                       {corpus_col.replace('a.', 'c.')},
                       c.raw_count * 1000000.0 / NULLIF(tt.total_words, 0) as value,
                       c.raw_count,
                       c.n_texts
                FROM counts c
                JOIN totals tt ON c.period = tt.period
                    {'AND c.corpus = tt.corpus' if by_corpus else ''}
                ORDER BY c.period, c.word {corpus_group.replace('a.', 'c.')}
            """
        else:
            sql = f"""
                SELECT {time_expr} as period,
                       a.word
                       {corpus_col},
                       SUM(a.{wc_col}) as value,
                       SUM(a.{wc_col}) as raw_count,
                       SUM(a.{nt_col}) as n_texts
                FROM wi_db.word_year_corpus a
                WHERE {where}
                GROUP BY period, a.word {corpus_group}
                ORDER BY period, a.word {corpus_group}
            """
        return self.conn.execute(sql).fetchdf()

    def ngram_examples(self, word, genre=None, corpus=None,
                       year_min=None, year_max=None, limit=20,
                       dedup=False, dedup_by='rank'):
        """Find texts that use a word most frequently. Case-insensitive (sums case variants per text).

            lltk.db.ngram_examples('virtue', genre='Fiction', year_min=1750, year_max=1759, dedup=True)
        """
        word = word.lower()
        where = self._ngram_where(genre=genre, corpus=corpus, year_min=year_min, year_max=year_max)
        dedup_join, dedup_clause = self._ngram_dedup(where, dedup=dedup, dedup_by=dedup_by)

        sql = f"""
            SELECT t._id, t.corpus, t.title, t.author, t.year, t.genre,
                   SUM(wi.count) as count,
                   SUM(wi.count) * 1000000.0 / NULLIF(t.n_words, 0) as per_million
            FROM wi_db.word_index wi
            JOIN texts t ON wi._id = t._id
            {dedup_join}
            WHERE LOWER(wi.word) = '{word}'
              AND {where}
              {dedup_clause}
            GROUP BY t._id, t.corpus, t.title, t.author, t.year, t.genre, t.n_words
            ORDER BY per_million DESC
            LIMIT {int(limit)}
        """
        return self.conn.execute(sql).fetchdf()

    def ngram_collocates(self, word, genre=None, corpus=None,
                         year_min=None, year_max=None, limit=50,
                         dedup=False, dedup_by='rank'):
        """Find words that co-occur with a given word (document-level). Case-insensitive.

            lltk.db.ngram_collocates('virtue', genre='Fiction', year_min=1750, year_max=1759, dedup=True)
        """
        word = word.lower()
        where_parts = [f"LOWER(w1.word) = '{word}'", f"LOWER(w2.word) != '{word}'"]
        if genre:
            where_parts.append(f"t.genre = '{genre}'")
        if corpus:
            where_parts.append(f"t.corpus = '{corpus}'")
        if year_min is not None:
            where_parts.append(f't.year >= {int(year_min)}')
        if year_max is not None:
            where_parts.append(f't.year <= {int(year_max)}')
        where = ' AND '.join(where_parts)

        filter_where = self._ngram_where(genre=genre, corpus=corpus, year_min=year_min, year_max=year_max)
        dedup_join, dedup_clause = self._ngram_dedup(filter_where, dedup=dedup, dedup_by=dedup_by)

        sql = f"""
            SELECT LOWER(w2.word) as word, COUNT(DISTINCT w1._id) as n_texts, SUM(w2.count) as total_count
            FROM wi_db.word_index w1
            JOIN wi_db.word_index w2 ON w1._id = w2._id
            JOIN texts t ON w1._id = t._id
            {dedup_join}
            WHERE {where}
              {dedup_clause}
            GROUP BY LOWER(w2.word)
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

    # ── Passage search ─────────────────────────────────────────────

    def search(self, query, genre=None, corpus=None, lang=None,
               year_min=None, year_max=None, limit=20, offset=0,
               snippet_words=30):
        """Full-text search across passages via FTS5.

        Args:
            query: FTS5 query (word, "phrase", NEAR(a b, 5), bool)
            genre/corpus/lang/year_min/year_max: filter via metadb texts table
            limit/offset: pagination
            snippet_words: words of context around match

        Returns:
            list of dicts with _id, seq, snippet, n_words, title, author, year, corpus, lang
        """
        import sqlite3

        if not os.path.exists(PATH_PASSAGESDB):
            raise FileNotFoundError(f'Passages DB not found at {PATH_PASSAGESDB}. Run lltk db-passages first.')

        # Build metadata filter via DuckDB
        where_parts = []
        if genre:
            where_parts.append(f"t.genre = '{genre}'")
        if corpus:
            where_parts.append(f"t.corpus = '{corpus}'")
        if lang:
            where_parts.append(f"t.lang = '{lang}'")
        if year_min is not None:
            where_parts.append(f't.year >= {int(year_min)}')
        if year_max is not None:
            where_parts.append(f't.year <= {int(year_max)}')

        filtered_ids = None
        if where_parts:
            where_sql = ' AND '.join(where_parts)
            id_df = self.conn.execute(f"SELECT _id FROM texts t WHERE {where_sql}").fetchdf()
            filtered_ids = set(id_df['_id'].tolist())
            if not filtered_ids:
                return []

        pconn = sqlite3.connect(PATH_PASSAGESDB)
        pconn.row_factory = sqlite3.Row

        if filtered_ids is not None:
            # Create temp table for ID filter
            pconn.execute("CREATE TEMP TABLE _filter_ids (_id TEXT PRIMARY KEY)")
            pconn.executemany("INSERT INTO _filter_ids VALUES (?)",
                              [(i,) for i in filtered_ids])
            sql = f"""
                SELECT p._id, p.seq, p.n_words, p.lang,
                       snippet(passages_fts, 1, '**', '**', '...', {int(snippet_words)}) as snippet
                FROM passages_fts
                JOIN passages p ON passages_fts.rowid = p.rowid
                JOIN _filter_ids f ON p._id = f._id
                WHERE passages_fts MATCH ?
                ORDER BY rank
                LIMIT ? OFFSET ?
            """
        else:
            sql = f"""
                SELECT p._id, p.seq, p.n_words, p.lang,
                       snippet(passages_fts, 1, '**', '**', '...', {int(snippet_words)}) as snippet
                FROM passages_fts
                JOIN passages p ON passages_fts.rowid = p.rowid
                WHERE passages_fts MATCH ?
                ORDER BY rank
                LIMIT ? OFFSET ?
            """

        rows = pconn.execute(sql, (query, limit, offset)).fetchall()
        pconn.close()

        # Enrich with metadata from DuckDB
        result_ids = list(set(r['_id'] for r in rows))
        meta_map = {}
        if result_ids:
            id_list = ', '.join(f"'{i}'" for i in result_ids)
            meta_df = self.conn.execute(f"""
                SELECT _id, title, author, year, corpus, genre, lang
                FROM texts WHERE _id IN ({id_list})
            """).fetchdf()
            for _, r in meta_df.iterrows():
                meta_map[r['_id']] = r.to_dict()

        results = []
        for r in rows:
            _id = r['_id']
            meta = meta_map.get(_id, {})
            results.append({
                '_id': _id,
                'seq': r['seq'],
                'snippet': r['snippet'],
                'n_words': r['n_words'],
                'title': meta.get('title', ''),
                'author': meta.get('author', ''),
                'year': meta.get('year'),
                'corpus': meta.get('corpus', ''),
                'genre': meta.get('genre', ''),
                'lang': r['lang'] or meta.get('lang', ''),
            })
        return results

    def search_count(self, query):
        """Count total FTS5 matches for a query."""
        import sqlite3
        if not os.path.exists(PATH_PASSAGESDB):
            return 0
        pconn = sqlite3.connect(PATH_PASSAGESDB)
        n = pconn.execute(
            "SELECT COUNT(*) FROM passages_fts WHERE passages_fts MATCH ?", (query,)
        ).fetchone()[0]
        pconn.close()
        return n

    def close(self):
        # Guard against instances created via __new__ that never ran __init__
        # (e.g. clickhouse_rebuild reusing prepare_corpus_df without a conn).
        conn = getattr(self, '_conn', None)
        if conn is not None:
            conn.close()
            self._conn = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __repr__(self):
        try:
            n = self.conn.execute("SELECT COUNT(*) FROM texts").fetchone()[0]
            nc = self.conn.execute("SELECT COUNT(DISTINCT corpus) FROM texts").fetchone()[0]
            return f'MetaDB({n} texts, {nc} corpora, {self.path})'
        except Exception:
            return f'MetaDB({self.path})'


# Module-level singleton — ClickHouse-backed.
# The legacy DuckDB-backed MetaDB class stays in this module as an
# emergency fallback; import it explicitly if needed:
#     from lltk.tools.metadb import MetaDB
from lltk.tools.metadb_ch import MetaDBCH
metadb = MetaDBCH()
