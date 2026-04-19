"""
Controlled vocabularies shared across lltk and client packages
(abstraction, largeliterarymodels, etc.).

Kept separate from metadb / schema modules so client packages can import
without pulling in ClickHouse or DuckDB deps. `from lltk.tools.vocabs import
GENRE_VOCAB` works in any environment with lltk installed.

Historical note: GENRE_VOCAB and LANG_NORMALIZE originally lived in
`lltk.tools.metadb`. They are still re-exported from there for
backwards-compat; new code should import from here.
"""

from __future__ import annotations


# ── GENRE_VOCAB ─────────────────────────────────────────────────────────
# Canonical high-level genre labels. Used by:
#   - lltk ingest (CORE_COLS enforces one of these in `texts.genre`)
#   - db-enrich-genres (authority-corpus overrides)
#   - lltk.annotations field spec for `genre`
#   - largeliterarymodels GenreTask (classify_genre.py) output validation
#
# `genre_raw` is an uncontrolled companion field that preserves the finer
# label the source gave us ("Epistolary fiction", "Novel of manners").

GENRE_VOCAB = frozenset({
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
})


# ── LANG_ISO639_1 ───────────────────────────────────────────────────────
# Accepted two-letter ISO 639-1 codes. Keep aligned with `normalize_lang`'s
# output range. Used by lltk.annotations field spec for `original_lang`.

LANG_ISO639_1 = frozenset({
    'en', 'fr', 'de', 'es', 'it', 'pt', 'ru', 'la', 'el', 'nl',
    'sv', 'da', 'no', 'pl', 'hu', 'cs', 'ro', 'fi',
})


# ── LANG_NORMALIZE ──────────────────────────────────────────────────────
# Accept ISO 639-2/B, 639-2/T, full English names, and 639-1 passthrough.
# Values are always 639-1 two-letter codes (members of LANG_ISO639_1).

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
    """Normalize a language code/name to ISO 639-1. Returns None if unknown.

    Input cleaning: str.strip().lower(). Empty / None / 'nan' → None.
    """
    if not lang or not isinstance(lang, str):
        return None
    s = lang.strip().lower()
    if s in ('', 'nan'):
        return None
    return LANG_NORMALIZE.get(s)
