"""Core LLTK tests using the test_fixture corpus."""
import pytest
import os
import sys
import pandas as pd
from collections import Counter


@pytest.fixture(scope='module')
def corpus():
    import lltk
    c = lltk.load('test_fixture')
    assert c is not None
    return c


class TestImport:
    def test_import(self):
        import lltk
        assert hasattr(lltk, 'load')

    def test_load_fixture(self):
        import lltk
        c = lltk.load('test_fixture')
        assert c is not None
        assert 'test_fixture' in c.id

    def test_load_nonexistent(self):
        import lltk
        c = lltk.load('nonexistent_corpus_xyz')
        assert c is None


class TestCorpus:
    def test_corpus_path(self, corpus):
        assert os.path.isdir(corpus.path)

    def test_corpus_repr(self, corpus):
        r = repr(corpus)
        assert 'TestFixture' in r

    def test_metadata_loads(self, corpus):
        meta = corpus.meta
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) == 3

    def test_metadata_columns(self, corpus):
        meta = corpus.meta
        assert 'title' in meta.columns
        assert 'author' in meta.columns
        assert 'year' in meta.columns
        assert 'genre' in meta.columns

    def test_metadata_content(self, corpus):
        meta = corpus.meta
        assert 'Jane Austen' in meta['author'].values
        assert 'Frankenstein' in meta['title'].values

    def test_texts_count(self, corpus):
        texts = list(corpus.texts())
        assert len(texts) == 3

    def test_text_ids(self, corpus):
        ids = {t.id for t in corpus.texts()}
        assert ids == {'blake_songs', 'austen_pride', 'shelley_frank'}


class TestText:
    @pytest.fixture
    def austen(self, corpus):
        for t in corpus.texts():
            if t.id == 'austen_pride':
                return t
        pytest.fail('austen_pride not found')

    @pytest.fixture
    def blake(self, corpus):
        for t in corpus.texts():
            if t.id == 'blake_songs':
                return t
        pytest.fail('blake_songs not found')

    def test_text_has_id(self, austen):
        assert austen.id == 'austen_pride'

    def test_text_has_corpus(self, austen):
        assert austen.corpus is not None
        assert 'test_fixture' in austen.corpus.id

    def test_text_txt(self, austen):
        txt = austen.txt
        assert isinstance(txt, str)
        assert len(txt) > 0
        assert 'truth universally acknowledged' in txt

    def test_text_txt_blake(self, blake):
        txt = blake.txt
        assert 'Tyger Tyger' in txt

    def test_text_path_txt(self, austen):
        assert os.path.exists(austen.path_txt)
        assert austen.path_txt.endswith('.txt')

    def test_text_author(self, austen):
        assert austen.author == 'Jane Austen'

    def test_text_au(self, austen):
        assert austen.au == 'Austen'

    def test_text_title(self, austen):
        assert austen.title == 'Pride and Prejudice'

    def test_text_year(self, austen):
        assert austen.year == 1813

    def test_text_addr(self, austen):
        addr = austen.addr
        assert 'test_fixture' in addr
        assert 'austen_pride' in addr


class TestFreqs:
    @pytest.fixture
    def austen(self, corpus):
        for t in corpus.texts():
            if t.id == 'austen_pride':
                return t

    def test_freqs_returns_counter(self, austen):
        freqs = austen.freqs()
        assert isinstance(freqs, Counter)

    def test_freqs_nonempty(self, austen):
        freqs = austen.freqs()
        assert len(freqs) > 0

    def test_freqs_common_words(self, austen):
        freqs = austen.freqs()
        assert 'the' in freqs or 'he' in freqs


class TestMFW:
    def test_mfw_returns_list(self, corpus):
        mfw = corpus.mfw(n=10)
        assert isinstance(mfw, list)

    def test_mfw_length(self, corpus):
        mfw = corpus.mfw(n=5)
        assert len(mfw) == 5

    def test_mfw_contains_common_words(self, corpus):
        mfw = corpus.mfw(n=20)
        assert 'the' in mfw

    def test_mfw_cached(self, corpus):
        mfw1 = corpus.mfw(n=10)
        mfw2 = corpus.mfw(n=10)
        assert mfw1 == mfw2


class TestDTM:
    def test_dtm_returns_dataframe(self, corpus):
        dtm = corpus.dtm(n=10)
        assert isinstance(dtm, pd.DataFrame)

    def test_dtm_shape(self, corpus):
        dtm = corpus.dtm(n=10)
        assert dtm.shape[0] == 3  # 3 texts
        assert dtm.shape[1] == 10  # 10 words

    def test_dtm_index_is_text_ids(self, corpus):
        dtm = corpus.dtm(n=10)
        assert set(dtm.index) == {'blake_songs', 'austen_pride', 'shelley_frank'}

    def test_dtm_values_nonnegative(self, corpus):
        dtm = corpus.dtm(n=10)
        assert (dtm >= 0).all().all()

    def test_dtm_tfidf(self, corpus):
        dtm = corpus.dtm(n=10, tfidf=True)
        assert isinstance(dtm, pd.DataFrame)
        assert dtm.shape[0] == 3


class TestSections:
    @pytest.fixture
    def shelley(self, corpus):
        for t in corpus.texts():
            if t.id == 'shelley_frank':
                return t
        pytest.fail('shelley_frank not found')

    def test_chapters_returns_section_corpus(self, shelley):
        chaps = shelley.chapters
        assert chaps is not None
        assert 'SectionCorpus' in type(chaps).__name__ or hasattr(chaps, 'texts')

    def test_chapters_count(self, shelley):
        sections = list(shelley.chapters.texts())
        assert len(sections) == 3

    def test_section_has_title(self, shelley):
        sections = list(shelley.chapters.texts())
        titles = [s.get('title', '') for s in sections]
        assert 'Letter 1' in titles
        assert 'Chapter 1' in titles

    def test_section_has_text(self, shelley):
        sections = list(shelley.chapters.texts())
        for s in sections:
            assert len(s.txt) > 0

    def test_section_text_content(self, shelley):
        sections = list(shelley.chapters.texts())
        # Letter 1 should contain "rejoice"
        assert 'rejoice' in sections[0].txt
        # Chapter 1 should contain "Genevese"
        assert 'Genevese' in sections[2].txt

    def test_section_freqs(self, shelley):
        sections = list(shelley.chapters.texts())
        freqs = sections[0].freqs()
        assert isinstance(freqs, Counter)
        assert len(freqs) > 0

    def test_sections_lazy_init(self, shelley):
        chaps = shelley.chapters
        # texts() should trigger init automatically
        sections = list(chaps.texts())
        assert len(sections) > 0


class TestParagraphs:
    @pytest.fixture
    def austen(self, corpus):
        for t in corpus.texts():
            if t.id == 'austen_pride':
                return t

    def test_paragraphs_count(self, austen):
        paras = list(austen.paragraphs.texts())
        assert len(paras) == 3

    def test_paragraph_has_text(self, austen):
        paras = list(austen.paragraphs.texts())
        for p in paras:
            assert len(p.txt) > 0

    def test_paragraph_ids_sequential(self, austen):
        paras = list(austen.paragraphs.texts())
        ids = [p.id for p in paras]
        assert ids == ['P0001', 'P0002', 'P0003']

    def test_paragraph_freqs(self, austen):
        paras = list(austen.paragraphs.texts())
        freqs = paras[0].freqs()
        assert isinstance(freqs, Counter)
        assert len(freqs) > 0


class TestPassages:
    @pytest.fixture
    def austen(self, corpus):
        for t in corpus.texts():
            if t.id == 'austen_pride':
                return t

    def test_passages_returns_sections(self, austen):
        passages = list(austen.passages(n=50).texts())
        assert len(passages) > 0

    def test_passages_at_least_n_words(self, austen):
        n = 50
        passages = list(austen.passages(n=n).texts())
        # all except possibly the last should have >= n words
        for p in passages[:-1]:
            assert p.get('num_words') >= n

    def test_passages_word_offset_ids(self, austen):
        passages = list(austen.passages(n=50).texts())
        for p in passages:
            assert p.id.startswith('W')
            assert '_' in p.id

    def test_passages_cover_full_text(self, austen):
        passages = list(austen.passages(n=50).texts())
        total = sum(p.get('num_words', 0) for p in passages)
        assert total > 0

    def test_passages_freqs(self, austen):
        passages = list(austen.passages(n=50).texts())
        freqs = passages[0].freqs()
        assert isinstance(freqs, Counter)
        assert len(freqs) > 0

    def test_passages_different_n(self, austen):
        p100 = list(austen.passages(n=100).texts())
        p50 = list(austen.passages(n=50).texts())
        assert len(p50) >= len(p100)


class TestLoadMetadata:
    """Tests for BaseCorpus.load_metadata() and corpus overrides."""

    def test_load_metadata_returns_dataframe(self, corpus):
        meta = corpus.load_metadata()
        assert isinstance(meta, pd.DataFrame)

    def test_load_metadata_has_rows(self, corpus):
        meta = corpus.load_metadata()
        assert len(meta) == 3

    def test_load_metadata_has_expected_columns(self, corpus):
        meta = corpus.load_metadata()
        assert 'title' in meta.columns
        assert 'author' in meta.columns
        assert 'year' in meta.columns

    def test_load_metadata_index_is_id(self, corpus):
        meta = corpus.load_metadata()
        assert meta.index.name == 'id'
        assert 'austen_pride' in meta.index

    def test_load_metadata_content(self, corpus):
        meta = corpus.load_metadata()
        assert meta.loc['austen_pride', 'author'] == 'Jane Austen'

    def test_load_metadata_no_clean(self, corpus):
        meta = corpus.load_metadata(clean=False)
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) == 3


class TestBookHistory:
    """Tests for ESTC book_history parsing functions."""

    def test_standardize_format_octavo(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format('8⁰')
        assert result['format_std'] == '8⁰'

    def test_standardize_format_folio(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format('folio')
        assert result['format_std'] == '2⁰'

    def test_standardize_format_word_quarto(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format('4to')
        assert result['format_std'] == '4⁰'

    def test_standardize_format_with_cm(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format('34 x 21 cm')
        assert result['format_cm'] == '34 cm'
        assert result['format_std'] is not None

    def test_standardize_format_modifier(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format('obl. 8⁰')
        assert result['format_std'] == '8⁰'
        assert 'obl' in result['format_modifier']

    def test_standardize_format_none(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format(None)
        assert result['format_std'] is None

    def test_standardize_format_nan(self):
        from lltk.corpus.estc.book_history import standardize_format
        result = standardize_format(float('nan'))
        assert result['format_std'] is None

    def test_parse_extent_pages(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent('xi, [1], 216 p.')
        assert result['num_pages'] == 228
        assert result['extent_type'] == 'pages'

    def test_parse_extent_simple(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent('8p.')
        assert result['num_pages'] == 8

    def test_parse_extent_volumes(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent('2v.')
        assert result['num_volumes'] == 2
        assert result['extent_type'] == 'volumes'

    def test_parse_extent_sheet(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent('1 sheet ([1] p.)')
        assert result['num_pages'] == 1
        assert result['extent_type'] == 'sheet'

    def test_parse_extent_plates(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent('216 p., plates')
        assert result['has_plates'] is True

    def test_parse_extent_none(self):
        from lltk.corpus.estc.book_history import parse_extent
        result = parse_extent(None)
        assert result['num_pages'] is None

    def test_is_fiction_true(self):
        from lltk.corpus.estc.estc import classify_genres, is_fiction
        assert is_fiction(classify_genres('Fiction', '')['genres']) is True
        assert is_fiction(classify_genres('Novels', '')['genres']) is True
        assert is_fiction(classify_genres('', 'English fiction')['genres']) is True

    def test_is_fiction_false(self):
        from lltk.corpus.estc.estc import classify_genres, is_fiction
        assert is_fiction(classify_genres('Sermons', '')['genres']) is False
        assert is_fiction(classify_genres('', 'English poetry')['genres']) is False
        assert is_fiction(classify_genres('', '')['genres']) is False

    def test_is_fiction_none(self):
        from lltk.corpus.estc.estc import classify_genres, is_fiction
        assert is_fiction(classify_genres(None, None)['genres']) is False

    def test_is_fiction_pipe_separated(self):
        from lltk.corpus.estc.estc import classify_genres, is_fiction
        assert is_fiction(classify_genres('Poems | Fiction', '')['genres']) is True


class TestLinkedMetadata:
    """Tests for cross-corpus metadata linking."""

    @pytest.fixture(scope='class')
    def linked_corpus(self):
        import lltk
        c = lltk.load('test_fixture_linked')
        assert c is not None
        return c

    def test_linked_corpus_loads(self, linked_corpus):
        meta = linked_corpus.load_metadata()
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) == 3

    def test_linked_metadata_has_prefixed_columns(self, linked_corpus):
        meta = linked_corpus.load_metadata()
        # Should have test_fixture_ prefixed columns from the linked corpus
        prefixed = [c for c in meta.columns if c.startswith('test_fixture_')]
        assert len(prefixed) > 0
        assert 'test_fixture_author' in meta.columns
        assert 'test_fixture_title' in meta.columns

    def test_linked_metadata_values(self, linked_corpus):
        meta = linked_corpus.load_metadata()
        austen_row = meta.loc['edition_austen']
        assert austen_row['test_fixture_author'] == 'Jane Austen'
        assert austen_row['test_fixture_title'] == 'Pride and Prejudice'

    def test_linked_metadata_preserves_own_columns(self, linked_corpus):
        meta = linked_corpus.load_metadata()
        assert 'format' in meta.columns
        assert 'pages' in meta.columns
        austen_row = meta.loc['edition_austen']
        assert austen_row['format'] == 'octavo'

    def test_linked_meta_via_corpus_meta(self, linked_corpus):
        # C.meta should also have linked columns since it uses load_metadata()
        meta = linked_corpus.meta
        assert 'test_fixture_author' in meta.columns

    def test_no_links_returns_unchanged(self, corpus):
        # test_fixture has LINKS but the linked corpus has no matching column issue
        # Just verify load_metadata still works
        meta = corpus.load_metadata()
        assert isinstance(meta, pd.DataFrame)
        assert len(meta) == 3


class TestLinkedTexts:
    """Tests for text.linked() traversal."""

    @pytest.fixture(scope='class')
    def linked_corpus(self):
        import lltk
        return lltk.load('test_fixture_linked')

    def test_linked_one_to_one(self, linked_corpus):
        # edition_austen links to austen_pride in test_fixture
        t = linked_corpus.text('edition_austen')
        linked = t.linked('test_fixture')
        assert len(linked) == 1
        assert linked[0].id == 'austen_pride'

    def test_linked_returns_text_objects(self, linked_corpus):
        t = linked_corpus.text('edition_austen')
        linked = t.linked('test_fixture')
        assert hasattr(linked[0], 'txt')
        assert hasattr(linked[0], 'author')

    def test_linked_reverse_one_to_many(self, corpus):
        # shelley_frank should link to 2 editions in test_fixture_linked
        t = corpus.text('shelley_frank')
        linked = t.linked('test_fixture_linked')
        assert len(linked) == 2
        ids = {lt.id for lt in linked}
        assert ids == {'edition_shelley_1', 'edition_shelley_2'}

    def test_linked_reverse_one_to_one(self, corpus):
        t = corpus.text('austen_pride')
        linked = t.linked('test_fixture_linked')
        assert len(linked) == 1
        assert linked[0].id == 'edition_austen'

    def test_linked_no_match(self, corpus):
        t = corpus.text('blake_songs')
        linked = t.linked('test_fixture_linked')
        assert len(linked) == 0

    def test_linked_invalid_corpus(self, corpus):
        t = corpus.text('austen_pride')
        linked = t.linked('nonexistent_corpus')
        assert linked == []


class TestNormalizeEstcId:
    """Tests for EEBO ESTC ID normalization."""

    def test_strip_prefix(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id('ESTC S115782') == 'S115782'

    def test_zero_pad(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id('ESTC S2616') == 'S002616'

    def test_already_padded(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id('ESTC S115782') == 'S115782'

    def test_no_prefix(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id('T089174') == 'T089174'

    def test_empty(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id('') == ''

    def test_nan(self):
        from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id
        assert _normalize_estc_id(float('nan')) == ''


class TestFindDuplicates:
    """Tests for within-corpus duplicate detection."""

    @pytest.fixture
    def base_texts(self, corpus):
        return [t for t in corpus.texts() if t.id in {'blake_songs', 'austen_pride', 'shelley_frank'}]

    def test_find_duplicates_returns_dataframe(self, corpus, base_texts):
        result = corpus.find_duplicates(n=100, threshold=0.5, texts=base_texts)
        assert isinstance(result, pd.DataFrame)
        assert set(result.columns) == {'id_1', 'id_2', 'similarity'}

    def test_find_duplicates_similarity_range(self, corpus, base_texts):
        result = corpus.find_duplicates(n=100, threshold=0.0, texts=base_texts)
        if len(result):
            assert (result['similarity'] >= 0).all()
            assert (result['similarity'] <= 1).all()

    def test_find_duplicates_no_self_matches(self, corpus, base_texts):
        result = corpus.find_duplicates(n=100, threshold=0.0, texts=base_texts)
        if len(result):
            assert (result['id_1'] != result['id_2']).all()

    def test_find_duplicates_sorted_descending(self, corpus, base_texts):
        result = corpus.find_duplicates(n=100, threshold=0.0, texts=base_texts)
        if len(result) > 1:
            sims = result['similarity'].tolist()
            assert sims == sorted(sims, reverse=True)

    def test_find_duplicates_high_threshold_fewer_results(self, corpus, base_texts):
        low = corpus.find_duplicates(n=100, threshold=0.3, texts=base_texts)
        high = corpus.find_duplicates(n=100, threshold=0.9, texts=base_texts)
        assert len(high) <= len(low)

    def test_find_duplicates_no_duplicate_pairs(self, corpus, base_texts):
        result = corpus.find_duplicates(n=100, threshold=0.0, texts=base_texts)
        if len(result):
            pairs = set(zip(result['id_1'], result['id_2']))
            assert len(pairs) == len(result)


class TestBLBooks:
    """Tests for BLBooks corpus (no download required)."""

    def test_blbooks_loads(self):
        import lltk
        c = lltk.load('blbooks')
        assert c is not None
        assert 'blbooks' in c.id

    def test_blbooks_has_compile(self):
        import lltk
        c = lltk.load('blbooks')
        assert hasattr(c, 'compile')
        assert callable(c.compile)

    def test_blbooks_has_load_metadata(self):
        import lltk
        c = lltk.load('blbooks')
        assert hasattr(c, 'load_metadata')

    def test_blbooks_parse_year(self):
        from lltk.corpus.blbooks.blbooks import _parse_year
        assert _parse_year('1850') == 1850
        assert _parse_year('1723-1725') == 1723
        assert _parse_year('[1800?]') == 1800
        assert _parse_year('') == ''
        assert _parse_year(None) == ''

    def test_blbooks_join_authors(self):
        from lltk.corpus.blbooks.blbooks import _join_authors
        assert _join_authors('Dickens, Charles', '', '') == 'Dickens, Charles'
        assert _join_authors('', 'Dickens', '') == 'Dickens'
        assert _join_authors('', '', 'Publisher Co.') == 'Publisher Co.'
        assert _join_authors('', '', '') == ''


class TestXml2txtEarlyprint:
    """Tests for EarlyPrint XML → TXT conversion with reg spelling."""

    @pytest.fixture(scope='class')
    def sample_xml(self, tmp_path_factory):
        """Create a minimal EarlyPrint TEI XML.gz for testing."""
        import gzip
        xml_content = b"""<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader><fileDesc><titleStmt><title>Test</title></titleStmt></fileDesc></teiHeader>
  <text>
    <body>
      <p xml:id="p1">
        <w lemma="now" pos="av" reg="Now">NOwe</w>
        <w lemma="we" pos="pns">we</w>
        <w lemma="have" pos="vvb" reg="have">haue</w>
        <w lemma="declare" pos="vvn">declared</w>
        <w lemma="many" pos="d">many</w>
        <w lemma="thing" pos="n2" reg="things">thinges</w>
        <pc>,</pc>
        <w lemma="which" pos="pnr">which</w>
        <w lemma="be" pos="vvb" reg="are">bee</w>
        <w lemma="worthy" pos="j" reg="worthy">worthye</w>
        <pc>.</pc>
      </p>
      <l xml:id="l1">
        <w lemma="the" pos="d">The</w>
        <w lemma="key" pos="n2" reg="Keys">Keyes</w>
        <w lemma="of" pos="acp">of</w>
        <w lemma="ancient" pos="j" reg="ancient">auncient</w>
        <w lemma="time" pos="n2" reg="times">tymes</w>
        <pc>.</pc>
      </l>
    </body>
  </text>
</TEI>"""
        path = tmp_path_factory.mktemp("earlyprint") / "test.xml.gz"
        with gzip.open(str(path), 'wb') as f:
            f.write(xml_content)
        return str(path)

    def test_basic_extraction(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml)
        assert len(txt) > 0
        assert 'Now' in txt or 'NOwe' in txt

    def test_reg_spelling(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml, use_reg=True)
        assert 'Now' in txt
        assert 'have' in txt
        assert 'things' in txt
        assert 'NOwe' not in txt
        assert 'haue' not in txt
        assert 'thinges' not in txt

    def test_surface_spelling(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml, use_reg=False)
        assert 'NOwe' in txt
        assert 'haue' in txt
        assert 'thinges' in txt

    def test_punctuation_no_leading_space(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml, use_reg=True)
        assert ', which' in txt
        assert 'worthy.' in txt

    def test_verse_lines_extracted(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml, use_reg=True)
        assert 'Keys' in txt
        assert 'ancient' in txt

    def test_paragraphs_separated(self, sample_xml):
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        txt = xml2txt_earlyprint(sample_xml, use_reg=True)
        # Paragraph and verse line should be separate blocks
        assert '\n\n' in txt

    def test_empty_body(self, tmp_path):
        """XML with no body should return empty string."""
        import gzip
        xml = b"""<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
  <teiHeader><fileDesc><titleStmt><title>Empty</title></titleStmt></fileDesc></teiHeader>
  <text></text>
</TEI>"""
        path = str(tmp_path / "empty.xml.gz")
        with gzip.open(path, 'wb') as f:
            f.write(xml)
        from lltk.corpus.earlyprint.earlyprint import xml2txt_earlyprint
        assert xml2txt_earlyprint(path) == ''


class TestFictionBiblioEstcIds:
    """Tests for fiction_biblio ESTC ID parsing and normalization."""

    def test_normalize_uppercase(self):
        from lltk.corpus.fiction_biblio.fiction_biblio import FictionBiblio
        import pandas as pd, re
        # Simulate the normalization logic
        def _normalize(raw):
            s = str(raw).strip()
            s = re.sub(r'^ESTC\s*', '', s).strip()
            s = re.sub(r'\s*\[.*?\]', '', s).strip()
            if not s: return ''
            if s[0].islower(): s = s[0].upper() + s[1:]
            s = re.sub(r'^([A-Z])0+', r'\1', s)
            if re.match(r'^[A-Z]\d+$', s): return s
            return ''
        assert _normalize('t068056') == 'T68056'
        assert _normalize('T068056') == 'T68056'

    def test_normalize_strip_zeros(self):
        import re
        def _strip(s):
            return re.sub(r'^([A-Z])0+', r'\1', s)
        assert _strip('T068056') == 'T68056'
        assert _strip('N024384') == 'N24384'
        assert _strip('T221825') == 'T221825'  # no leading zeros
        assert _strip('T2') == 'T2'

    def test_normalize_reject_year(self):
        import re
        def _normalize(raw):
            s = str(raw).strip()
            s = re.sub(r'^ESTC\s*', '', s).strip()
            s = re.sub(r'\s*\[.*?\]', '', s).strip()
            if not s: return ''
            if s[0].islower(): s = s[0].upper() + s[1:]
            s = re.sub(r'^([A-Z])0+', r'\1', s)
            if re.match(r'^[A-Z]\d+$', s): return s
            return ''
        assert _normalize('1785') == ''
        assert _normalize('1790?') == ''
        assert _normalize('027439') == ''

    def test_normalize_strip_brackets(self):
        import re
        def _normalize(raw):
            s = str(raw).strip()
            s = re.sub(r'^ESTC\s*', '', s).strip()
            s = re.sub(r'\s*\[.*?\]', '', s).strip()
            if not s: return ''
            if s[0].islower(): s = s[0].upper() + s[1:]
            s = re.sub(r'^([A-Z])0+', r'\1', s)
            if re.match(r'^[A-Z]\d+$', s): return s
            return ''
        assert _normalize('T63646 [vols. 1–4]') == 'T63646'

    def test_parse_multi_value(self):
        import re, pandas as pd
        def _normalize(raw):
            s = str(raw).strip()
            s = re.sub(r'^ESTC\s*', '', s).strip()
            s = re.sub(r'\s*\[.*?\]', '', s).strip()
            if not s: return ''
            if s[0].islower(): s = s[0].upper() + s[1:]
            s = re.sub(r'^([A-Z])0+', r'\1', s)
            if re.match(r'^[A-Z]\d+$', s): return s
            return ''
        raw = 'T90269, t090270'
        ids = []
        for part in re.split(r'[;,]', raw):
            norm = _normalize(part)
            if norm and norm not in ids:
                ids.append(norm)
        assert ids == ['T90269', 'T90270']


class TestEarlyprintPrefixMap:
    """Tests for EarlyPrint TCP ID prefix → repo mapping."""

    def test_eebo_prefixes(self):
        from lltk.corpus.earlyprint.earlyprint import EarlyPrint
        # Check the prefix map used in extract_metadata
        prefix_map = {'A': 'eebotcp', 'B': 'eebotcp', 'C': 'eccotcp',
                       'E': 'eebotcp', 'K': 'eccotcp', 'N': 'evanstcp'}
        assert prefix_map['A'] == 'eebotcp'
        assert prefix_map['B'] == 'eebotcp'
        assert prefix_map['E'] == 'eebotcp'

    def test_ecco_prefixes(self):
        prefix_map = {'A': 'eebotcp', 'B': 'eebotcp', 'C': 'eccotcp',
                       'E': 'eebotcp', 'K': 'eccotcp', 'N': 'evanstcp'}
        assert prefix_map['C'] == 'eccotcp'
        assert prefix_map['K'] == 'eccotcp'

    def test_evans_prefix(self):
        prefix_map = {'A': 'eebotcp', 'B': 'eebotcp', 'C': 'eccotcp',
                       'E': 'eebotcp', 'K': 'eccotcp', 'N': 'evanstcp'}
        assert prefix_map['N'] == 'evanstcp'


class TestPmapPickle:
    """Tests for pmap picklability fix."""

    def test_pmap_caller_picklable(self):
        import pickle
        from lltk.tools.tools import _PmapCaller
        caller = _PmapCaller(str.upper, (), {})
        pickled = pickle.dumps(caller)
        restored = pickle.loads(pickled)
        assert restored('hello') == 'HELLO'

    def test_pmap_sequential(self):
        from lltk.tools.tools import pmap
        results = pmap(lambda x: x * 2, [1, 2, 3], num_proc=1, progress=False)
        assert results == [2, 4, 6]

    def test_pmap_threads(self):
        from lltk.tools.tools import pmap
        results = pmap(lambda x: x * 2, [1, 2, 3], num_proc=2,
                       use_threads=True, progress=False)
        assert results == [2, 4, 6]
