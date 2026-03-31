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
