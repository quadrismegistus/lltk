"""Tests for OCR cleaning pipeline — BaseText.clean_txt() and ECCO override."""

import json
import os
import pytest
from unittest.mock import MagicMock, patch
from lxml import etree


# ── ecco_page_texts ───────────────────────────────────────────────────

SAMPLE_XML = b"""<?xml version="1.0"?>
<book>
  <text>
    <page type="titlePage">
      <pageInfo><ocr>75.0</ocr></pageInfo>
      <pageContent>
        <p><wd pos="100,50,200,80">THE</wd>
           <wd pos="100,100,200,130">TITLE</wd></p>
      </pageContent>
    </page>
    <page type="bodyPage">
      <pageInfo><sourcePage>1</sourcePage><ocr>88.5</ocr></pageInfo>
      <pageContent>
        <p><wd pos="100,50,200,80">Firft</wd>
           <wd pos="300,50,400,80">paragraph</wd>
           <wd pos="100,100,200,130">of</wd>
           <wd pos="300,100,400,130">text.</wd></p>
        <p><wd pos="100,200,200,230">Second</wd>
           <wd pos="300,200,400,230">paragraph.</wd></p>
      </pageContent>
    </page>
    <page type="bodyPage">
      <pageInfo><sourcePage>2</sourcePage><ocr>91.0</ocr></pageInfo>
      <pageContent>
        <p><wd pos="100,50,200,80">Page</wd>
           <wd pos="300,50,400,80">two</wd>
           <wd pos="100,100,200,130">content.</wd></p>
      </pageContent>
    </page>
  </text>
</book>
"""


class TestEccoPageTexts:
    def test_extracts_all_pages(self):
        from lltk.corpus.ecco.ecco import ecco_page_texts
        root = etree.fromstring(SAMPLE_XML)
        pages = ecco_page_texts(root)
        assert len(pages) == 3

    def test_page_metadata(self):
        from lltk.corpus.ecco.ecco import ecco_page_texts
        root = etree.fromstring(SAMPLE_XML)
        pages = ecco_page_texts(root)
        assert pages[0]['page_type'] == 'titlePage'
        assert pages[0]['page_ocr'] == 75.0
        assert pages[0]['page_num'] is None
        assert pages[1]['page_type'] == 'bodyPage'
        assert pages[1]['page_num'] == 1
        assert pages[1]['page_ocr'] == 88.5

    def test_page_text_joins_paragraphs(self):
        from lltk.corpus.ecco.ecco import ecco_page_texts
        root = etree.fromstring(SAMPLE_XML)
        pages = ecco_page_texts(root)
        assert '\n\n' in pages[1]['text']
        assert 'Firft paragraph' in pages[1]['text']
        assert 'Second paragraph.' in pages[1]['text']

    def test_page_i_sequential(self):
        from lltk.corpus.ecco.ecco import ecco_page_texts
        root = etree.fromstring(SAMPLE_XML)
        pages = ecco_page_texts(root)
        assert [p['page_i'] for p in pages] == [0, 1, 2]


# ── ecco_lines ────────────────────────────────────────────────────────

class TestEccoLines:
    def test_includes_all_page_types(self):
        from lltk.corpus.ecco.ecco import ecco_lines
        root = etree.fromstring(SAMPLE_XML)
        lines = ecco_lines(root)
        types = {r['page_type'] for r in lines}
        assert 'titlePage' in types
        assert 'bodyPage' in types

    def test_filter_page_types(self):
        from lltk.corpus.ecco.ecco import ecco_lines
        root = etree.fromstring(SAMPLE_XML)
        lines = ecco_lines(root, page_types={'bodyPage'})
        types = {r['page_type'] for r in lines}
        assert types == {'bodyPage'}


# ── _remove_catchwords_from_text ──────────────────────────────────────

class TestRemoveCatchwords:
    def test_removes_matching_catchword(self):
        from lltk.corpus.ecco.ecco import _remove_catchwords_from_text
        text = 'first page ends with catchword\n\n\ncatchword starts next page'
        result = _remove_catchwords_from_text(text)
        assert result == 'first page ends with\n\n\ncatchword starts next page'

    def test_no_match_no_change(self):
        from lltk.corpus.ecco.ecco import _remove_catchwords_from_text
        text = 'page one text\n\n\npage two text'
        assert _remove_catchwords_from_text(text) == text

    def test_single_page(self):
        from lltk.corpus.ecco.ecco import _remove_catchwords_from_text
        text = 'just one page'
        assert _remove_catchwords_from_text(text) == text


# ── _chunk_on_paragraphs ─────────────────────────────────────────────

class TestChunkOnParagraphs:
    def test_basic_chunking(self):
        from lltk.text.text import _chunk_on_paragraphs
        txt = 'Short para one.\n\nShort para two.\n\nShort para three.'
        chunks = _chunk_on_paragraphs(txt, target_words=5)
        assert len(chunks) >= 2

    def test_single_large_paragraph(self):
        from lltk.text.text import _chunk_on_paragraphs
        txt = ' '.join(['word'] * 1000)
        chunks = _chunk_on_paragraphs(txt, target_words=500)
        assert len(chunks) == 1

    def test_empty_text(self):
        from lltk.text.text import _chunk_on_paragraphs
        assert _chunk_on_paragraphs('') == []
        assert _chunk_on_paragraphs('   ') == []

    def test_preserves_all_text(self):
        from lltk.text.text import _chunk_on_paragraphs
        txt = 'Para one.\n\nPara two.\n\nPara three.'
        chunks = _chunk_on_paragraphs(txt, target_words=3)
        reassembled = '\n\n'.join(chunks)
        assert reassembled == txt


# ── helpers ───────────────────────────────────────────────────────────

def _make_text(cls, corpus, text_id):
    """Create a BaseText/TextECCO with required internal state, bypassing __init__."""
    t = cls.__new__(cls)
    t.corpus = corpus
    t.id = text_id
    t._meta_hydrated = True
    t._meta = {}
    t._dom = None
    t._txt = None
    t._xml = None
    t._node = None
    t._freqs = None
    t._minhash = None
    t._characters = None
    t._section_corpus = None
    t._sections = {}
    t._rels = {}
    t.BAD_TAGS = []
    t.BODY_TAG = None
    return t


# ── get_path txt_clean preference ─────────────────────────────────────

class TestGetPathTxtClean:
    def test_prefers_txt_clean(self, tmp_path):
        from lltk.text.text import BaseText

        txt_dir = tmp_path / 'txt'
        txt_clean_dir = tmp_path / 'txt_clean'
        txt_dir.mkdir()
        txt_clean_dir.mkdir()
        (txt_dir / 'test.txt').write_text('dirty')
        (txt_clean_dir / 'test.txt').write_text('clean')

        mock_corpus = MagicMock()
        mock_corpus.path_txt = str(txt_dir)
        mock_corpus.path_txt_clean = str(txt_clean_dir)
        mock_corpus.ext_txt = '.txt'

        t = _make_text(BaseText, mock_corpus, 'test')
        path = t.get_path('txt')
        assert 'txt_clean' in path
        assert path == str(txt_clean_dir / 'test.txt')

    def test_falls_back_to_txt(self, tmp_path):
        from lltk.text.text import BaseText

        txt_dir = tmp_path / 'txt'
        txt_dir.mkdir()
        (txt_dir / 'test.txt').write_text('dirty')

        mock_corpus = MagicMock()
        mock_corpus.path_txt = str(txt_dir)
        mock_corpus.path_txt_clean = str(tmp_path / 'txt_clean')
        mock_corpus.ext_txt = '.txt'

        t = _make_text(BaseText, mock_corpus, 'test')
        path = t.get_path('txt')
        assert 'txt_clean' not in path
        assert path == str(txt_dir / 'test.txt')


# ── BaseText.clean_txt ───────────────────────────────────────────────

class TestBaseTextCleanTxt:
    def test_cleans_and_writes(self, tmp_path):
        from lltk.text.text import BaseText

        txt_dir = tmp_path / 'txt'
        txt_clean_dir = tmp_path / 'txt_clean'
        txt_dir.mkdir()
        (txt_dir / 'test.txt').write_text('Dirty para one.\n\nDirty para two.')

        mock_corpus = MagicMock()
        mock_corpus.path_txt = str(txt_dir)
        mock_corpus.path_txt_clean = str(txt_clean_dir)
        mock_corpus.ext_txt = '.txt'

        t = _make_text(BaseText, mock_corpus, 'test')

        mock_task = MagicMock()
        mock_task.map.return_value = ['Clean para one.\n\nClean para two.']

        status = t.clean_txt(task=mock_task)
        assert status == 'cleaned'
        assert (txt_clean_dir / 'test.txt').exists()
        content = (txt_clean_dir / 'test.txt').read_text()
        assert 'Clean para' in content

    def test_skips_existing(self, tmp_path):
        from lltk.text.text import BaseText

        txt_clean_dir = tmp_path / 'txt_clean'
        txt_clean_dir.mkdir()
        (txt_clean_dir / 'test.txt').write_text('already cleaned')

        mock_corpus = MagicMock()
        mock_corpus.path_txt_clean = str(txt_clean_dir)
        mock_corpus.ext_txt = '.txt'

        t = _make_text(BaseText, mock_corpus, 'test')
        status = t.clean_txt(task=MagicMock())
        assert status == 'skipped'


# ── TextECCO.clean_txt ───────────────────────────────────────────────

class TestTextECCOCleanTxt:
    def test_writes_txt_and_json(self, tmp_path):
        from lltk.corpus.ecco.ecco import TextECCO

        xml_dir = tmp_path / 'xml'
        txt_clean_dir = tmp_path / 'txt_clean'
        xml_dir.mkdir()

        import gzip
        with gzip.open(str(xml_dir / 'test.xml.gz'), 'wb') as f:
            f.write(SAMPLE_XML)

        mock_corpus = MagicMock()
        mock_corpus.path_xml = str(xml_dir)
        mock_corpus.path_txt_clean = str(txt_clean_dir)
        mock_corpus.ext_xml = '.xml.gz'
        mock_corpus.ext_txt = '.txt'

        t = _make_text(TextECCO, mock_corpus, 'test')

        mock_task = MagicMock()
        mock_task.map.return_value = ['cleaned title', 'cleaned page 1', 'cleaned page 2']

        status = t.clean_txt(task=mock_task)
        assert status == 'cleaned'
        assert (txt_clean_dir / 'test.txt').exists()
        assert (txt_clean_dir / 'test.json').exists()

        content = (txt_clean_dir / 'test.txt').read_text()
        assert 'cleaned title' in content
        assert '\n\n\n' in content

        meta = json.loads((txt_clean_dir / 'test.json').read_text())
        assert len(meta) == 3
        assert meta[0]['page_type'] == 'titlePage'
        assert meta[1]['page_num'] == 1
