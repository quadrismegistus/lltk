"""
Tests for lltk.tools.lang_detect — exclusivity-weighted frequency word lists.
"""

import pytest


class TestFunctionWordsTable:
    def test_returns_triples(self):
        from lltk.tools.lang_detect import function_words_table
        triples = function_words_table()
        assert len(triples) > 100
        word, lang, weight = triples[0]
        assert isinstance(word, str)
        assert isinstance(lang, str)
        assert isinstance(weight, float)

    def test_all_weights_positive(self):
        from lltk.tools.lang_detect import function_words_table
        for word, lang, weight in function_words_table():
            assert weight > 0, f'{word}/{lang} has non-positive weight {weight}'
            assert weight <= 1.0, f'{word}/{lang} has weight > 1.0'

    def test_exclusive_word_has_weight_one(self):
        from lltk.tools.lang_detect import function_words_table
        by_word = {}
        for word, lang, weight in function_words_table():
            by_word.setdefault(word, []).append((lang, weight))
        found_exclusive = False
        for word, entries in by_word.items():
            if len(entries) == 1:
                assert entries[0][1] == 1.0, f'{word} is exclusive but weight != 1.0'
                found_exclusive = True
                break
        assert found_exclusive

    def test_shared_word_has_fractional_weight(self):
        from lltk.tools.lang_detect import function_words_table
        by_word = {}
        for word, lang, weight in function_words_table():
            by_word.setdefault(word, []).append((lang, weight))
        found_shared = False
        for word, entries in by_word.items():
            if len(entries) > 1:
                for lang, weight in entries:
                    assert weight == pytest.approx(1.0 / len(entries)), \
                        f'{word} shared by {len(entries)} langs but weight={weight}'
                found_shared = True
                break
        assert found_shared

    def test_latin_and_greek_included(self):
        from lltk.tools.lang_detect import function_words_table
        langs = {lang for _, lang, _ in function_words_table()}
        assert 'la' in langs
        assert 'el' in langs

    def test_modern_langs_included(self):
        from lltk.tools.lang_detect import function_words_table
        langs = {lang for _, lang, _ in function_words_table()}
        for expected in ('en', 'fr', 'de', 'es', 'pt', 'it', 'nl'):
            assert expected in langs, f'{expected} missing'

    def test_no_duplicate_word_per_lang(self):
        from lltk.tools.lang_detect import function_words_table
        seen = set()
        for word, lang, _ in function_words_table():
            key = (word, lang)
            assert key not in seen, f'duplicate ({word}, {lang})'
            seen.add(key)

    def test_cached_on_repeat_call(self):
        from lltk.tools.lang_detect import function_words_table
        a = function_words_table()
        b = function_words_table()
        assert a is b


class TestFunctionWordCounts:
    def test_returns_dict(self):
        from lltk.tools.lang_detect import function_word_counts
        counts = function_word_counts()
        assert isinstance(counts, dict)
        assert counts['en'] > 100
        assert counts['la'] > 50

    def test_spanish_has_many_words(self):
        from lltk.tools.lang_detect import function_word_counts
        assert function_word_counts()['es'] >= 300
