"""
Per-text language detection via function-word stopword intersection.

For each text with freqs, compute how much of its token weight is captured by
each supported language's function-word list. Assign to the language with
the highest coverage if it dominates the runner-up by a factor of >=2.

Supported languages: en, fr, de, la, it, es, pt, nl, el.

NLTK provides stopwords for most; Latin/Greek are hand-curated below.

Function word lists are restricted to short, high-frequency closed-class
tokens (articles, prepositions, pronouns, auxiliaries, conjunctions). Avoid
content words so that a short poem about "la rose" isn't misclassified.
"""
from __future__ import annotations

# --- Latin function words (hand-curated, ~100 items) ---
# Prepositions, conjunctions, pronouns, common auxiliary/copula forms, adverbs.
# Deliberately excludes tokens that overlap heavily with English/French
# ("a", "in") since those defeat the signal.
LATIN_FUNCTION_WORDS = [
    # copula & esse
    'est', 'sunt', 'sum', 'es', 'sumus', 'estis', 'erat', 'erant', 'eram', 'eras',
    'eramus', 'eratis', 'fuit', 'fuerunt', 'fuerat', 'erit', 'erunt', 'esse',
    'esset', 'essent', 'fuisse', 'fore', 'futurus',
    # pronouns
    'ego', 'tu', 'nos', 'vos', 'mihi', 'tibi', 'sibi', 'mei', 'tui', 'sui',
    'meus', 'tuus', 'suus', 'noster', 'vester', 'nostri', 'vestri',
    'hic', 'haec', 'hoc', 'huius', 'huic', 'hunc', 'hanc',
    'ille', 'illa', 'illud', 'illius', 'illi', 'illum', 'illam',
    'ipse', 'ipsa', 'ipsum', 'idem', 'eadem', 'idemque',
    'qui', 'quae', 'quod', 'quem', 'quam', 'quos', 'quas', 'quibus',
    'cuius', 'cui', 'quo', 'qua', 'quis', 'quid',
    # conjunctions & particles
    'et', 'atque', 'ac', 'sed', 'aut', 'vel', 'seu', 'sive', 'nec', 'neque',
    'nam', 'enim', 'igitur', 'ergo', 'itaque', 'autem', 'quoque', 'etiam',
    'tamen', 'quidem', 'vero', 'quia', 'quod', 'cum', 'dum', 'donec',
    'quando', 'quoniam', 'ubi', 'ut', 'ne', 'si', 'nisi', 'quasi',
    # prepositions
    'ad', 'ab', 'ex', 'de', 'pro', 'per', 'cum', 'sub', 'super', 'ante',
    'post', 'inter', 'contra', 'apud', 'propter', 'praeter', 'sine', 'absque',
    'erga', 'extra', 'intra', 'ultra', 'circa', 'penes',
    # adverbs
    'non', 'ita', 'sic', 'tum', 'tunc', 'iam', 'nunc', 'hodie', 'semper',
    'numquam', 'saepe', 'interdum', 'statim', 'mox', 'bene', 'male',
    'multum', 'parum', 'satis', 'valde',
]

# Greek (polytonic normalized to monotonic lower-case; but texts will vary)
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


def _nltk_stopwords(lang_name, iso):
    """Load NLTK stopwords for a language; return empty list if unavailable."""
    try:
        from nltk.corpus import stopwords as _sw
        try:
            return list(_sw.words(lang_name))
        except LookupError:
            import nltk
            nltk.download('stopwords', quiet=True)
            return list(_sw.words(lang_name))
    except Exception:
        return []


def _build_function_words():
    """Return list of (word, lang) pairs for detection, lowercased and deduplicated-per-lang."""
    lang_lists = {
        'en': _nltk_stopwords('english', 'en'),
        'fr': _nltk_stopwords('french', 'fr'),
        'de': _nltk_stopwords('german', 'de'),
        'it': _nltk_stopwords('italian', 'it'),
        'es': _nltk_stopwords('spanish', 'es'),
        'pt': _nltk_stopwords('portuguese', 'pt'),
        'nl': _nltk_stopwords('dutch', 'nl'),
        'la': LATIN_FUNCTION_WORDS,
        'el': GREEK_FUNCTION_WORDS,
    }
    pairs = []
    for lang, words in lang_lists.items():
        seen = set()
        for w in words:
            w = (w or '').strip().lower()
            if not w or w in seen:
                continue
            seen.add(w)
            pairs.append((w, lang))
    return pairs


def function_words_table():
    """List of (word, lang) pairs. Safe to call repeatedly (cached)."""
    global _CACHED
    try:
        return _CACHED
    except NameError:
        pass
    _CACHED = _build_function_words()
    return _CACHED


def function_word_counts():
    """Return {lang: count} — vocabulary size per language."""
    from collections import Counter
    return dict(Counter(lang for _, lang in function_words_table()))
