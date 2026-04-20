"""
Unit tests for lltk.tools.clickhouse_passages — query parsing and snippet
extraction. Pure Python: no ClickHouse server required.
"""

import pytest


class TestQueryToChCondition:
    def _cond(self, q):
        from lltk.tools.clickhouse_passages import _query_to_ch_condition
        return _query_to_ch_condition(q)

    def test_single_token(self):
        c = self._cond('virtue')
        assert "hasTokenCaseInsensitive(text, 'virtue')" in c

    def test_multi_token_and(self):
        c = self._cond('virtue honor')
        assert 'hasTokenCaseInsensitive' in c
        assert ' AND ' in c

    def test_phrase_uses_position(self):
        c = self._cond('"virtue and honor"')
        assert 'positionCaseInsensitive' in c
        assert 'virtue and honor' in c

    def test_near_becomes_and(self):
        c = self._cond('NEAR(virtue vice, 5)')
        assert 'hasTokenCaseInsensitive' in c
        assert ' AND ' in c
        assert 'virtue' in c
        assert 'vice' in c

    def test_apostrophe_escaped(self):
        c = self._cond("o'clock")
        assert "o''clock" in c

    def test_no_sql_injection_via_quote(self):
        c = self._cond("word' OR '1'='1")
        # The single quote must be doubled, not raw
        assert "' OR '" not in c


class TestExtractSnippet:
    def _snip(self, text, query, context_words=10):
        from lltk.tools.clickhouse_passages import _extract_snippet
        return _extract_snippet(text, query, context_words=context_words)

    def test_term_found(self):
        text = 'the quick brown fox jumps over the lazy dog'
        s = self._snip(text, 'fox')
        assert 'fox' in s

    def test_no_match_returns_start(self):
        text = 'the quick brown fox'
        s = self._snip(text, 'zzznomatch')
        assert 'quick' in s

    def test_ellipsis_prefix_when_not_at_start(self):
        words = ['word'] * 20 + ['TARGET'] + ['word'] * 20
        text = ' '.join(words)
        s = self._snip(text, 'TARGET', context_words=6)
        assert s.startswith('...')
        assert 'TARGET' in s

    def test_phrase_match(self):
        text = 'she wrote an epistolary novel in 1740'
        s = self._snip(text, '"epistolary novel"')
        assert 'epistolary' in s

    def test_near_extracts_first_term(self):
        text = 'virtue leads to honor among men'
        s = self._snip(text, 'NEAR(virtue honor, 5)')
        assert 'virtue' in s


class TestImpactESXml2Txt:
    """Test xml2txt_impact_es without requiring the full corpus."""

    def _make_gt_xml(self, paragraphs):
        """Build a minimal GT-format TEI XML string."""
        ns = 'http://www.tei-c.org/ns/1.0'
        paras = ''.join(f'<p xmlns="{ns}">{p}</p>' for p in paragraphs)
        return f'''<?xml version="1.0"?>
<TEI xmlns="{ns}">
  <text><body>{paras}</body></text>
</TEI>'''

    def test_basic_gt(self, tmp_path):
        from lltk.corpus.impact_es.impact_es import xml2txt_impact_es
        xml = self._make_gt_xml(['Hello world.', 'Second paragraph.'])
        p = tmp_path / 'test.xml'
        p.write_text(xml, encoding='utf-8')
        result = xml2txt_impact_es(str(p))
        assert 'Hello world' in result
        assert 'Second paragraph' in result

    def test_dehyphenation(self, tmp_path):
        from lltk.corpus.impact_es.impact_es import xml2txt_impact_es
        # Page-break hyphen: "to-" at end of one <p>, "mado" at start of next
        xml = self._make_gt_xml(['se ha to-', 'mado el medio'])
        p = tmp_path / 'test.xml'
        p.write_text(xml, encoding='utf-8')
        result = xml2txt_impact_es(str(p))
        assert 'tomado' in result
        assert 'to-' not in result

    def test_empty_file(self, tmp_path):
        from lltk.corpus.impact_es.impact_es import xml2txt_impact_es
        p = tmp_path / 'bad.xml'
        p.write_text('not xml', encoding='utf-8')
        assert xml2txt_impact_es(str(p)) == ''

    def test_xml2txt_callable_as_plain_function(self):
        """Regression: XML2TXT must not be wrapped in staticmethod()."""
        from lltk.corpus.impact_es.impact_es import TextImpactES
        t = TextImpactES.__new__(TextImpactES)
        # xml2txt_func property should return the raw callable without AttributeError
        fn = t.xml2txt_func
        assert callable(fn)
