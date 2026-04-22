"""
Unit tests for lltk.tools.genre_tags — normalize_genre_raw, tag_facets.

Pure Python: no ClickHouse, no DuckDB required.
"""

import pytest


class TestNormalizeGenreRaw:
    def test_empty(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        assert normalize_genre_raw('') == []
        assert normalize_genre_raw('  ') == []

    def test_single_known_atom(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        assert normalize_genre_raw('novel') == ['novel']

    def test_case_insensitive(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        assert normalize_genre_raw('Novel') == ['novel']
        assert normalize_genre_raw('FICTION') == normalize_genre_raw('fiction')

    def test_semicolon_separator(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('novel; satire')
        assert 'novel' in tags
        assert 'satire' in tags

    def test_pipe_separator(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('novel | drama')
        assert 'novel' in tags
        assert 'drama' in tags

    def test_comma_split(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        # "Novel, satire" → look up each part
        tags = normalize_genre_raw('novel, satire')
        assert 'novel' in tags

    def test_multi_word_critical_term_kept_atomic(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('imaginary voyage')
        assert 'imaginary voyage' in tags
        assert 'imaginary' not in tags
        assert 'voyage' not in tags

    def test_null_atom_returns_empty(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        # Atoms mapped to null (e.g. German academic disciplines) produce no tags
        tags = normalize_genre_raw('Literaturwissenschaft')
        assert tags == []

    def test_deduplication(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('novel; novel')
        assert tags.count('novel') == 1

    def test_non_english_mapped(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('Bildungsroman')
        assert 'bildungsroman' in tags

    def test_llm_passthrough_it_narrative(self):
        from lltk.tools.genre_tags import normalize_genre_raw
        tags = normalize_genre_raw('it narrative')
        assert 'it-narrative' in tags


class TestTagFacets:
    def test_novel_has_form(self):
        from lltk.tools.genre_tags import tag_facets
        assert 'form' in tag_facets('novel')

    def test_gothic_has_mode_and_register(self):
        from lltk.tools.genre_tags import tag_facets
        facets = tag_facets('gothic')
        assert 'mode' in facets
        assert 'register' in facets

    def test_bildungsroman_has_form_and_subgenre(self):
        from lltk.tools.genre_tags import tag_facets
        facets = tag_facets('bildungsroman')
        assert 'form' in facets
        assert 'subgenre' in facets

    def test_imaginary_voyage_has_subgenre(self):
        from lltk.tools.genre_tags import tag_facets
        facets = tag_facets('imaginary voyage')
        assert 'subgenre' in facets

    def test_unknown_tag_returns_empty(self):
        from lltk.tools.genre_tags import tag_facets
        assert tag_facets('xyzzy_not_a_tag') == []


class TestIsRecognized:
    def test_author_match(self):
        from lltk.tools.genre_tags import is_recognized
        assert is_recognized('Jane', None, 'Austen, Jane', None, 'Pride')

    def test_author_substring(self):
        from lltk.tools.genre_tags import is_recognized
        assert is_recognized('Mary', None, 'Shelley, Mary Wollstonecraft', None, 'Frankenstein')

    def test_author_case_insensitive(self):
        from lltk.tools.genre_tags import is_recognized
        assert is_recognized('jane', None, 'Austen, Jane', None, 'Pride')

    def test_author_too_short(self):
        from lltk.tools.genre_tags import is_recognized
        assert not is_recognized('J', None, 'Austen, Jane', None, 'Pride')

    def test_year_match(self):
        from lltk.tools.genre_tags import is_recognized
        assert is_recognized(None, '1813', 'Unknown', 1813, 'Pride')

    def test_year_mismatch(self):
        from lltk.tools.genre_tags import is_recognized
        assert not is_recognized(None, '1800', 'Unknown', 1813, 'Pride')

    def test_year_in_title_requires_author(self):
        from lltk.tools.genre_tags import is_recognized
        # Year matches AND appears in title — year match doesn't count
        assert not is_recognized(None, '1813', 'Unknown', 1813,
                                 'Pride and Prejudice (1813)')
        # But with author match it passes
        assert is_recognized('Jane', '1813', 'Austen, Jane', 1813,
                             'Pride and Prejudice (1813)')

    def test_year_in_estc_title(self):
        import json
        from lltk.tools.genre_tags import is_recognized
        meta = json.dumps({'estc_title': 'A Novel. 1750.'})
        assert not is_recognized(None, '1750', 'Unknown', 1750,
                                 'A Novel', meta)

    def test_neither_match(self):
        from lltk.tools.genre_tags import is_recognized
        assert not is_recognized('Wrong', '1900', 'Austen, Jane', 1813, 'Pride')

    def test_none_values(self):
        from lltk.tools.genre_tags import is_recognized
        assert not is_recognized(None, None, None, None, None)

    def test_non_llm_not_gated(self):
        # is_recognized is only called for llm:* sources;
        # non-LLM sources bypass it entirely in build_genre_tags
        from lltk.tools.genre_tags import is_recognized
        # But the function itself doesn't know about sources —
        # verify it works with empty author (corpus data has no LLM predictions)
        assert not is_recognized('', None, 'Austen', 1813, 'Pride')
