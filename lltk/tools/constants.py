"""
Shared constants and utility functions used across lltk.tools modules.

Extracted from metadb.py to eliminate the 3,500-line DuckDB legacy module.
"""

import os
import re

from lltk.tools.vocabs import (
    GENRE_VOCAB,
    LANG_ISO639_1,
    LANG_NORMALIZE,
    normalize_lang,
)

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
        for seq, (chunk_txt, word_start, word_end, num_words) in enumerate(chunk_sentences(sents, n)):
            passages.append((_id, seq, chunk_txt, num_words, lang))
        return (_id, corpus_id, passages)
    except Exception:
        return (_id, corpus_id, [])
