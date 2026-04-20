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
