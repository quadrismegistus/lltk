"""
Shared constants and utility functions used across lltk.tools modules.

This module is the single source of truth for all lltk constants.
Imported by lltk.imports (which re-exports everything) and by
lltk.tools.tools (which needs a few constants at module level without
triggering circular imports through lltk.imports).
"""

import os
import re
import multiprocessing as mp
from collections import defaultdict


# ── Process / CPU constants ─────────────────────────────────────────

try:
    mp.set_start_method('fork')
except RuntimeError:
    pass
mp_cpu_count = mp.cpu_count()
DEFAULT_NUM_PROC = max(1, mp_cpu_count - 2)

# ── Paths (no config dependency) ────────────────────────────────────

HOME = os.path.expanduser('~')
LLTK_ROOT = PATH_HERE = ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PATH_BASE_CONF = os.path.join(HOME, '.lltk_config')
PATH_DEFAULT_LLTK_HOME = os.path.join(HOME, 'lltk_data')
PATH_DEFAULT_CONF = os.path.abspath(os.path.join(PATH_DEFAULT_LLTK_HOME, 'config_default.txt'))
PATH_LLTK_CONFIG_DIR = os.path.abspath(os.path.join(PATH_DEFAULT_LLTK_HOME, 'config'))
PATH_LLTK_CONFIG_USR = os.path.join(PATH_DEFAULT_LLTK_HOME, '.user.json')

# ── Separators / keys ──────────────────────────────────────────────

META_KEY_SEP = '__'
MATCHRELNAME = 'rdf:type'

# ── Minimal metadata schema ────────────────────────────────────────

MINIMETAD = {
    'author': ['author'],
    'title': ['title'],
    'year': ['year', 'date'],
}

# ── Mutable singletons ────────────────────────────────────────────

DATABOX = defaultdict(dict)

# ── Text / column constants ────────────────────────────────────────

DIR_SECTION_NAME = 'sections'
TEXT_META_DEFAULT = {'id': '', 'author': '', 'title': '', 'year': ''}

BAD_COLS = {'Unnamed: 0', '_llp_'}
CHECKMARK = '✓'
CROSSMARK = '✗'
DIR_TEXTS_NAME = 'texts'

COL_ID = 'id'
COL_ADDR = '_id'
COL_CORPUS = '_corpus'
IDSEP_START = '_'
IDSEP = '/'

NULL_QID = 'Q0'
ANNO_EXTS = ['.anno.xlsx', '.anno.xls', '.anno.csv', '.xlsx', '.xls']

EMPTY_GROUP = '(all)'
TMP_CORPUS_ID = 'tmp'

MODERNIZE_SPELLING = False
ZIP_PART_DEFAULTS = {'txt', 'freqs', 'metadata', 'xml', 'data'}
DOWNLOAD_PART_DEFAULTS = ['metadata']
PREPROC_CMDS = ['txt', 'freqs', 'mfw', 'dtm']
DEFAULT_MFW_N = 25000
DEFAULT_DTM_N = 25000
DEFAULT_MFW_YEARBIN = 100
MANIFEST_REQUIRED_DATA = ['name', 'id']
TEXT_PATH_KEYS = [
    'path_freqs',
    'path_txt',
    'path_xml',
]
BROKENSTATE = '__Broken_state__'
PATH_CORENLP = '~/lltk_data/tools/corenlp'

# ── Data file paths (relative to lltk home) ────────────────────────

PATH_TO_ENGLISH_WORDLIST = 'data/default/wordlist.aspell.net.with_caps.txt.gz'
PATH_TO_ENGLISH_STOPWORDS = 'data/default/stopwords.onix.txt.gz'
PATH_TO_ENGLISH_SPELLING_MODERNIZER = 'data/default/spelling_variants_from_morphadorner.txt.gz'
PATH_TO_ENGLISH_OCR_CORRECTION_RULES = 'data/default/CorrectionRules.txt.gz'
PATH_TO_ENGLISH_WORD2POS = 'data/default/word2pos.json.gz'

PATH_TO_BOOKNLP_BINARY = 'bin/book-nlp/runjava'

# ── Manifest defaults ──────────────────────────────────────────────

MANIFEST_DEFAULTS = dict(
    path_txt='txt',
    path_txt_clean='txt_clean',
    path_xml='xml',
    path_nlp='nlp',
    path_pos='pos',
    path_index='',
    ext_xml='.xml',
    ext_txt='.txt',
    ext_nlp='.jsonl',
    ext_freqs='.json',
    path_cadence_scan='cadence_scan',
    path_cadence_parse='cadence_parse',
    path_model='',
    path_header=None,
    path_metadata='metadata.csv',
    path_metadata_init='metadata_init.csv',
    path_metadata_letters='metadata_letters.pkl',
    path_notebook='notebook.ipynb',
    paths_text_data=[],
    paths_rel_data=[],
    class_name='',
    path_freq_table={},
    col_id='id',
    col_addr='_id',
    col_id_corpus='id_corpus',
    col_id_text='id_text',
    idsep='|', col_t='_text',
    col_fn='fn',
    path_root='',
    path_raw='raw',
    path_spacy='spacy',
    path_freqs='freqs',
    manifest={},
    path_python='',
    manifest_override=True,
    path_data='data',
    path_chars='chars',
    path_letters='letters',
    is_meta='',
    public='',
    private='',
    mfw_yearbin=False,
    mfw_n=25000,
    license='',
    license_type='',
    year_start=None,
    year_end=None,
    lang='en',
)

BAD_TAGS = {'figdesc', 'head', 'edit', 'note', 'header', 'footer', 'dochead', 'front'}

# ── BookNLP constants ──────────────────────────────────────────────

BOOKNLP_NARRATOR_ID = 'NARRATOR'
BOOKNLP_DEFAULT_LANGUAGE = "en"
BOOKNLP_DEFAULT_MODEL = 'small'
BOOKNLP_DEFAULT_PIPELINE = "entity,quote,supersense,event,coref"
BOOKNLP_RENAME_COLS = {
    'paragraph_ID': 'para_i',
    'sentence_ID': 'sent_i',
    'token_ID_within_sentence': 'sent_token_i',
    'token_ID_within_document': 'token_i',
    'word': 'token',
    'lemma': 'lemma',
    'byte_onset': 'onset',
    'byte_offset': 'offset',
    'POS_tag': 'pos',
    'fine_POS_tag': 'pos2',
    'dependency_relation': 'deprel',
    'syntactic_head_ID': 'head',
    'event': 'event',
}
BAD_CHAR_IDS = {'?', '?!', 'x', 'nan', 'None'}
chardata_metakeys_initial = dict(
    char_race='',
    char_gender='',
    char_class='',
    char_geo_birth='',
    char_geo_marriage='',
    char_geo_death='',
    char_geo_begin='',
    char_geo_middle='',
    char_geo_end='',
)

# ── Year keys ──────────────────────────────────────────────────────

YEARKEYS = ['year', 'date']

# ── Init DB corpora ────────────────────────────────────────────────

INIT_DB_WITH_CORPORA = {
    'chadwyck',
    'chicago',
    'markmark',
    'txtlab',
    'tedjdh',
    'gildedage',
    'clmet',
    'dta',
    'dialnarr',
    'estc',
    'eebo_tcp',
    'ecco_tcp',
    'ecco',
    'evans_tcp',
    'litlab',
    'semantic_cohort',
    'spectator',
}


# ── Vocab imports ──────────────────────────────────────────────────

from lltk.tools.vocabs import (
    GENRE_VOCAB,
    LANG_ISO639_1,
    LANG_NORMALIZE,
    normalize_lang,
)

# ── DB constants (from metadb.py extraction) ───────────────────────

DB_BLACKLIST = {'hathi', 'bighist', 'spanish_pd_books'}

GENRE_AUTHORITY_CORPORA = {
    'fiction_biblio': 10,
    'end': 10,
    'ravengarside': 10,
}

GENRE_SOURCE_PRIORITY = {
    'bibliography': 50,
    'form': 30,
    'topic': 20,
    'title': 5,
    'corpus': 10,
}

CORPUS_SOURCE_RANKS = {
    'chadwyck': 1, 'chadwyck_drama': 1, 'chadwyck_poetry': 1,
    'earlyprint': 2,
    'eebo_tcp': 3, 'ecco_tcp': 3, 'evans_tcp': 3,
    'markmark': 3, 'chicago': 3, 'clmet': 3,
    'gildedage': 4, 'coca': 4, 'coha': 4, 'sellers': 4, 'new_yorker': 4, 'spectator': 4,
    'tedjdh': 5, 'long_arc_prestige': 5,
    'hathi_englit': 5, 'hathi_novels': 5, 'hathi_romances': 5, 'hathi_treatises': 5,
    'hathi_almanacs': 5, 'hathi_essays': 5, 'hathi_letters': 5, 'hathi_sermons': 5,
    'hathi_stories': 5, 'hathi_tales': 5, 'hathi_proclamations': 5, 'hathi_bio': 5,
    'ecco': 6, 'bpo': 6, 'litlab': 6, 'pmla': 6, 'sotu': 6, 'gale_amfic': 6,
    'internet_archive': 7,
    'blbooks': 8,
    'canon_fiction': 9, 'dialogues': 9, 'fanfic': 9,
    'ravengarside': 9, 'estc': 10, 'semantic_cohort': 10,
    'dta': 11, 'dialnarr': 11, 'txtlab': 11, 'hathi': 11, 'oldbailey': 11, 'epistolary': 11,
    'test_fixture': 100, 'test_fixture_linked': 100,
    'arc_fiction': 101, 'arc_poetry': 101, 'arc_periodical': 101, 'tmp': 101,
}

CORE_COLS = [
    '_id', 'corpus', 'id', 'title', 'author', 'year', 'genre', 'genre_raw',
    'lang', 'is_translated', 'title_norm', 'author_norm', 'path_freqs',
]
STANDARD_COLS = ['id', 'title', 'author', 'year', 'genre', 'genre_raw']


# ── Title / author normalization ─────────────────────────────────────

_TITLE_NORM_PUNCS = re.compile(r'[;:.\(\[,!?]')
_TITLE_END_PHRASES = sorted([
    'edited by', 'written by', 'by the author', 'by mr', 'by mrs',
    'by miss', 'by dr', 'a novel', 'a romance', 'a tale', 'a poem',
    'a tragedy', 'a comedy', 'a farce', 'in two volumes', 'in three volumes',
    'in four volumes', 'the second edition', 'the third edition',
    'the fourth edition', 'a new edition', 'translated from',
    'translated by', 'with a preface', 'with an introduction',
], key=len, reverse=True)

_spelling_modernizer = None


def _get_spelling_modernizer():
    global _spelling_modernizer
    if _spelling_modernizer is not None:
        return _spelling_modernizer
    try:
        path = PATH_TO_ENGLISH_SPELLING_MODERNIZER
        if not os.path.isabs(path):
            # PATH_LLTK_HOME depends on config; lazy import avoids circularity
            from lltk.imports import PATH_LLTK_HOME
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
    import html
    t = html.unescape(t)
    t = re.sub(r'[‐‑‒–—―−﹘﹣－]', '-', t)
    t = t.replace('--', ' ')
    t = re.sub(r'[\[\]]', '', t).strip()
    _ABBREV_PREFIXES = {
        'mr', 'mrs', 'ms', 'dr', 'st', 'sr', 'jr', 'esq', 'rev', 'gen',
        'col', 'capt', 'maj', 'sgt', 'vol', 'pt', 'no', 'ed', 'edn',
    }
    t = re.sub(r'\b([a-z])\.\s', r'\1 ', t)
    for abbr in _ABBREV_PREFIXES:
        t = re.sub(r'\b' + abbr + r'\.\s', abbr + ' ', t)
        t = re.sub(r'\b' + abbr + r'\.$', abbr, t)
    mod = _get_spelling_modernizer()
    if mod:
        t = ' '.join(mod.get(w, w) for w in t.split())
    m = _TITLE_NORM_PUNCS.search(t)
    if m:
        t = t[:m.start()].strip()
    else:
        tl = t.lower()
        for phrase in _TITLE_END_PHRASES:
            idx = tl.find(phrase)
            if idx > 3:
                t = t[:idx].strip()
                break
    t = t.rstrip('. ')
    t = ' '.join(t.split())
    return t if len(t) > 1 else None


def normalize_author(author):
    """Normalize an author name: lowercase last name before first comma."""
    if not author or not isinstance(author, str) or author == 'nan':
        return None
    a = author.strip().lower()
    if ',' in a:
        a = a.split(',')[0].strip()
    a = a.rstrip('.')
    a = ' '.join(a.split())
    return a if len(a) > 1 else None


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
    prefix = 0
    for i in range(min(4, len1, len2)):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break
    return jaro + prefix * 0.1 * (1 - jaro)


def _parse_year(val):
    """Parse a year value to integer. Handles ranges, circa dates, etc."""
    import re as _re
    import numpy as np
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s in ('nan', 'None', ''):
        return None
    for pfx in ('c.', 'c ', 'ca.', 'ca ', '[', ']', '?', '~'):
        s = s.replace(pfx, '')
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
    m = _re.search(r'\b(\d{4})\b', s)
    if m:
        try:
            return int(m.group(1))
        except (ValueError, OverflowError):
            pass
    return None


def chunk_sentences(sents, n=500):
    """Split a list of sentences into ~n-word chunks.

    Yields (chunk_text, word_start, word_end, num_words) tuples.
    Word counting uses str.split() for consistency between
    in-memory sectioning and ClickHouse passage ingest.
    """
    chunk_sents = []
    chunk_word_count = 0
    word_offset = 0
    for sent in sents:
        sent_n = len(sent.split())
        chunk_sents.append(sent)
        chunk_word_count += sent_n
        if chunk_word_count >= n:
            yield (' '.join(chunk_sents), word_offset, word_offset + chunk_word_count, chunk_word_count)
            word_offset += chunk_word_count
            chunk_sents = []
            chunk_word_count = 0
    if chunk_sents:
        yield (' '.join(chunk_sents), word_offset, word_offset + chunk_word_count, chunk_word_count)


def _chunk_text_to_passages(args):
    """Chunk a single text file into ~n-word passages using sentence splitting.

    Returns (_id, corpus_id, passages, error_or_none). A non-None error means the
    text could not be chunked (bad encoding, missing language model, etc.); the
    caller should surface it rather than silently treat the text as empty.
    """
    _id, corpus_id, txt_path, lang, n = args
    try:
        from lltk.text.text import _open_file, _lang_to_punkt
        import nltk
        with _open_file(txt_path) as f:
            txt = f.read()
        if not txt or not txt.strip():
            return (_id, corpus_id, [], None)
        punkt_lang = _lang_to_punkt(lang)
        sents = nltk.sent_tokenize(txt, language=punkt_lang)
        passages = []
        for seq, (chunk_txt, word_start, word_end, num_words) in enumerate(chunk_sentences(sents, n)):
            passages.append((_id, seq, chunk_txt, num_words, lang))
        return (_id, corpus_id, passages, None)
    except Exception as e:
        return (_id, corpus_id, [], f'{type(e).__name__}: {str(e)[:200]}')
