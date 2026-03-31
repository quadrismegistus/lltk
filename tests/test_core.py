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
        from lltk.corpus.estc.book_history import is_fiction
        assert is_fiction('Fiction', '') is True
        assert is_fiction('Novels', '') is True
        assert is_fiction('', 'English fiction') is True

    def test_is_fiction_false(self):
        from lltk.corpus.estc.book_history import is_fiction
        assert is_fiction('Sermons', '') is False
        assert is_fiction('', 'English poetry') is False
        assert is_fiction('', '') is False

    def test_is_fiction_none(self):
        from lltk.corpus.estc.book_history import is_fiction
        assert is_fiction(None, None) is False

    def test_is_fiction_pipe_separated(self):
        from lltk.corpus.estc.book_history import is_fiction
        assert is_fiction('Poems | Fiction', '') is True
