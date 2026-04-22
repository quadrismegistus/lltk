"""
Per-text language detection via exclusivity-weighted frequency words.

For each supported language, a list of top-500 most frequent words (from
OpenSubtitles via FrequencyWords, MIT license) is loaded. Each word is
weighted by 1/N where N is the number of languages that share it. This
strongly favors language-exclusive discriminators over shared function words.

Latin and Greek use hand-curated lists (no subtitle corpora exist).

Public API:
    function_words_table()    -> list[(word, lang, weight)]
    function_word_counts()    -> dict[lang, int]
"""
from __future__ import annotations

import json
import os
from collections import defaultdict

_DATA_PATH = os.path.join(os.path.dirname(__file__), 'data', 'freq_words_top500.json')

# --- Latin function words (hand-curated, ~100 items) ---
LATIN_FUNCTION_WORDS = [
    'est', 'sunt', 'sum', 'sumus', 'estis', 'erat', 'erant', 'eram', 'eras',
    'eramus', 'eratis', 'fuit', 'fuerunt', 'fuerat', 'erit', 'erunt', 'esse',
    'esset', 'essent', 'fuisse', 'fore', 'futurus',
    'ego', 'tu', 'nos', 'vos', 'mihi', 'tibi', 'sibi', 'mei', 'tui', 'sui',
    'meus', 'tuus', 'suus', 'noster', 'vester', 'nostri', 'vestri',
    'hic', 'haec', 'hoc', 'huius', 'huic', 'hunc', 'hanc',
    'ille', 'illa', 'illud', 'illius', 'illi', 'illum', 'illam',
    'ipse', 'ipsa', 'ipsum', 'idem', 'eadem', 'idemque',
    'qui', 'quae', 'quod', 'quem', 'quam', 'quos', 'quas', 'quibus',
    'cuius', 'cui', 'quo', 'qua', 'quis', 'quid',
    'et', 'atque', 'ac', 'sed', 'aut', 'vel', 'seu', 'sive', 'nec', 'neque',
    'nam', 'enim', 'igitur', 'ergo', 'itaque', 'autem', 'quoque', 'etiam',
    'tamen', 'quidem', 'vero', 'quia', 'quod', 'cum', 'dum', 'donec',
    'quando', 'quoniam', 'ubi', 'ut', 'ne', 'si', 'nisi', 'quasi',
    'ad', 'ab', 'ex', 'de', 'pro', 'per', 'cum', 'sub', 'super', 'ante',
    'post', 'inter', 'contra', 'apud', 'propter', 'praeter', 'sine', 'absque',
    'erga', 'extra', 'intra', 'ultra', 'circa', 'penes',
    'non', 'ita', 'sic', 'tum', 'tunc', 'iam', 'nunc', 'hodie', 'semper',
    'numquam', 'saepe', 'interdum', 'statim', 'mox', 'bene', 'male',
    'multum', 'parum', 'satis', 'valde',
]

GREEK_FUNCTION_WORDS = [
    'καί', 'και', 'δέ', 'δε', 'γάρ', 'γαρ', 'μέν', 'μεν', 'οὖν', 'ουν',
    'τε', 'ἀλλά', 'αλλα', 'ἤ', 'η',
    'ὁ', 'ἡ', 'τό', 'το', 'τοῦ', 'του', 'τῆς', 'της', 'τόν', 'τον',
    'τήν', 'την', 'τῶν', 'των', 'τοῖς', 'τοις', 'ταῖς', 'ταις',
    'οἱ', 'οι', 'αἱ', 'αι',
    'ἐν', 'εν', 'εἰς', 'εις', 'ἐκ', 'εκ', 'ἐξ', 'εξ', 'ἀπό', 'απο',
    'διά', 'δια', 'κατά', 'κατα', 'μετά', 'μετα', 'περί', 'περι',
    'πρός', 'προς', 'ὑπό', 'υπο', 'ἐπί', 'επι',
    'οὐ', 'ου', 'οὐκ', 'ουκ', 'μή', 'μη',
    'ἐγώ', 'εγω', 'σύ', 'συ', 'ἡμεῖς', 'ημεις', 'ὑμεῖς', 'υμεις',
    'αὐτός', 'αυτος', 'αὐτή', 'αυτη', 'αὐτό', 'αυτο',
    'ὅς', 'ος', 'ἥ', 'ἐστι', 'εστι', 'ἐστιν', 'εστιν', 'εἰσιν', 'εισιν',
    'ἦν', 'ην', 'γε', 'δή', 'δη',
]


def _load_freq_words():
    """Load top-500 frequency words per language from bundled JSON."""
    with open(_DATA_PATH, encoding='utf-8') as f:
        return json.load(f)


def _build_function_words():
    """Return list of (word, lang, weight) triples.

    Weight = 1/N where N = number of languages containing the word.
    Words exclusive to one language get weight 1.0; words shared across
    3 languages get 0.33. This strongly favors discriminative words over
    shared Romance/Germanic function words.
    """
    # Frequency-ranked words for modern languages
    freq_data = _load_freq_words()

    # Add curated Latin/Greek
    lang_lists = dict(freq_data)
    lang_lists['la'] = [w.lower().strip() for w in LATIN_FUNCTION_WORDS if w.strip()]
    lang_lists['el'] = [w.lower().strip() for w in GREEK_FUNCTION_WORDS if w.strip()]

    # Deduplicate within each language
    for lang in lang_lists:
        seen = set()
        deduped = []
        for w in lang_lists[lang]:
            w = w.lower().strip()
            if w and w not in seen:
                seen.add(w)
                deduped.append(w)
        lang_lists[lang] = deduped

    # Count how many languages each word appears in
    word_n_langs: dict[str, int] = defaultdict(int)
    for words in lang_lists.values():
        for w in words:
            word_n_langs[w] += 1

    # Build weighted triples
    triples = []
    for lang, words in lang_lists.items():
        for w in words:
            weight = 1.0 / word_n_langs[w]
            triples.append((w, lang, weight))
    return triples


_CACHED = None

def function_words_table():
    """List of (word, lang, weight) triples. Safe to call repeatedly (cached)."""
    global _CACHED
    if _CACHED is None:
        _CACHED = _build_function_words()
    return _CACHED


def function_word_counts():
    """Return {lang: count} -- vocabulary size per language."""
    from collections import Counter
    return dict(Counter(lang for _, lang, _ in function_words_table()))
