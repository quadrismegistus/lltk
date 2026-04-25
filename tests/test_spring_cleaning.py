"""
Tests for spring-cleaning refactors: text properties, task API,
preprocess func_ref, passage export format, metadata resolution.

Uses test_fixture corpus — no ClickHouse required.
"""

import pytest
import os
import json
import tempfile

# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────

@pytest.fixture(scope='module')
def corpus():
    import lltk
    return lltk.load('test_fixture')


@pytest.fixture(scope='module')
def text(corpus):
    return corpus.text('austen_pride')


# ──────────────────────────────────────────────────────────────────────
# Text property direct access (bypass __getattr__ → get() chain)
# ──────────────────────────────────────────────────────────────────────

class TestTextProperties:
    def test_title_is_string(self, text):
        assert isinstance(text.title, str)
        assert len(text.title) > 0

    def test_title_value(self, text):
        assert text.title == 'Pride and Prejudice'

    def test_author_is_string(self, text):
        assert isinstance(text.author, str)
        assert len(text.author) > 0

    def test_author_value(self, text):
        assert text.author == 'Jane Austen'

    def test_genre_is_string(self, text):
        assert isinstance(text.genre, str)

    def test_genre_value(self, text):
        assert text.genre == 'Fiction'

    def test_year_numeric(self, text):
        assert text.year == 1813

    def test_n_words_is_int(self, text):
        assert isinstance(text.n_words, int)

    def test_genre_raw_is_string(self, text):
        assert isinstance(text.genre_raw, str)

    def test_title_norm_is_string(self, text):
        assert isinstance(text.title_norm, str)

    def test_author_norm_is_string(self, text):
        assert isinstance(text.author_norm, str)

    def test_is_translated_is_bool(self, text):
        assert isinstance(text.is_translated, bool)

    def test_original_lang_is_string(self, text):
        assert isinstance(text.original_lang, str)

    def test_lang_detected_is_string(self, text):
        assert isinstance(text.lang_detected, str)

    def test_property_matches_get(self, text):
        assert text.title == text.get('title')

    def test_property_matches_meta(self, text):
        meta = text.metadata()
        assert text.title == meta.get('title', '')

    def test_len_uses_n_words(self, text):
        assert len(text) == text.n_words


class TestTextPropertiesUnhydrated:
    """Test that properties work even when _meta has no data yet."""

    def test_empty_title_not_none(self, corpus):
        t = corpus.TEXT_CLASS.__new__(corpus.TEXT_CLASS)
        t._meta = {}
        t._meta_hydrated = True
        t.__meta = {}
        t._gcache = {}
        assert t.title == ''
        assert t.author == ''
        assert t.genre == ''
        assert t.n_words == 0
        assert t.is_translated == False


# ──────────────────────────────────────────────────────────────────────
# __getattr__ fallback for dynamic keys
# ──────────────────────────────────────────────────────────────────────

class TestGetAttrFallback:
    def test_path_txt_via_getattr(self, text):
        assert text.path_txt.endswith('.txt')

    def test_path_tasks_on_corpus(self, corpus):
        assert corpus.path_tasks.endswith('tasks')

    def test_path_prosodic_on_corpus(self, corpus):
        assert 'prosodic' in corpus.path_prosodic

    def test_unknown_attr_raises(self, text):
        with pytest.raises(AttributeError):
            _ = text.this_attribute_does_not_exist_xyz


# ──────────────────────────────────────────────────────────────────────
# meta_(), meta_l(), meta_1() — suffix dispatch from get()
# ──────────────────────────────────────────────────────────────────────

class TestMetaSuffixDispatch:
    def test_meta_l_returns_sequence(self, text):
        result = text.get('title_l')
        assert hasattr(result, '__iter__')
        assert len(result) > 0

    def test_meta_1_returns_single(self, text):
        result = text.get('title_1')
        assert isinstance(result, str)

    def test_meta_underscore_returns_tuples(self, text):
        result = text.meta_('title')
        assert isinstance(result, list)
        if result:
            assert len(result[0]) == 3  # (text, key, value)


# ──────────────────────────────────────────────────────────────────────
# Task API
# ──────────────────────────────────────────────────────────────────────

class TestTaskAPI:
    def test_task_path_standalone(self):
        import lltk
        p = lltk.task_path('_chadwyck/ncf0204.01', 'social_network')
        assert 'chadwyck' in p
        assert 'tasks' in p
        assert 'social_network' in p
        assert 'ncf0204.01' in p

    def test_task_path_strips_underscore(self):
        import lltk
        p = lltk.task_path('_chadwyck/foo', 'bar')
        assert '/_chadwyck/' not in p
        assert '/chadwyck/' in p

    def test_task_path_with_subcollection(self):
        import lltk
        p = lltk.task_path('_chadwyck/Early_English/ee01.01', 'genre')
        assert p.endswith('Early_English/ee01.01')

    def test_task_dir_on_text(self, text):
        d = text.task_dir('social_network')
        assert 'tasks/social_network' in d
        assert text.id in d

    def test_task_returns_none_when_empty(self, text):
        assert text.task('nonexistent_task_xyz') is None

    def test_task_sources_empty(self, text):
        assert text.task_sources('nonexistent_task_xyz') == []

    def test_task_roundtrip(self, text, tmp_path):
        task_dir = str(tmp_path / 'social_network' / text.id)
        os.makedirs(task_dir, exist_ok=True)
        data = {'characters': [{'id': 'C01', 'name': 'Elizabeth'}],
                'metadata': {'model': 'test', 'schema_version': 1}}
        with open(os.path.join(task_dir, 'test-model.json'), 'w') as f:
            json.dump(data, f)

        # Monkeypatch task_dir to use tmp_path
        original = text.task_dir
        text.task_dir = lambda name: str(tmp_path / name / text.id)
        try:
            result = text.task('social_network')
            assert result is not None
            assert result['characters'][0]['name'] == 'Elizabeth'

            sources = text.task_sources('social_network')
            assert 'test-model' in sources

            specific = text.task('social_network', source='test-model')
            assert specific['metadata']['model'] == 'test'

            assert text.task('social_network', source='nonexistent') is None
        finally:
            text.task_dir = original


# ──────────────────────────────────────────────────────────────────────
# Preprocess func_ref
# ──────────────────────────────────────────────────────────────────────

class TestFuncRef:
    def test_func_ref_returns_tuple(self):
        from lltk.model.preprocess import _func_ref
        def sample_func():
            pass
        ref = _func_ref(sample_func)
        assert ref is not None
        assert isinstance(ref, tuple)
        assert len(ref) == 2
        assert ref[1] == 'sample_func'

    def test_func_ref_file_path(self):
        from lltk.model.preprocess import _func_ref
        from lltk.tools.clickhouse_passages import _escape
        ref = _func_ref(_escape)
        assert ref is not None
        assert ref[0].endswith('clickhouse_passages.py')
        assert ref[1] == '_escape'

    def test_resolve_roundtrip(self):
        from lltk.model.preprocess import _func_ref, _resolve_func_ref
        from lltk.tools.clickhouse_passages import _escape
        ref = _func_ref(_escape)
        func = _resolve_func_ref(ref)
        assert func("it's") == "it''s"

    def test_func_ref_picklable(self):
        import pickle
        from lltk.model.preprocess import _func_ref
        from lltk.tools.clickhouse_passages import _escape
        ref = _func_ref(_escape)
        roundtripped = pickle.loads(pickle.dumps(ref))
        assert roundtripped == ref


# ──────────────────────────────────────────────────────────────────────
# Passage query helpers (extended)
# ──────────────────────────────────────────────────────────────────────

class TestPassageQueryHelpers:
    def test_escape_single_quote(self):
        from lltk.tools.clickhouse_passages import _escape
        assert _escape("it's") == "it''s"

    def test_escape_double_quote_unchanged(self):
        from lltk.tools.clickhouse_passages import _escape
        assert _escape('say "hello"') == 'say "hello"'

    def test_escape_empty(self):
        from lltk.tools.clickhouse_passages import _escape
        assert _escape('') == ''

    def test_condition_whitespace_handling(self):
        from lltk.tools.clickhouse_passages import _query_to_ch_condition
        c = _query_to_ch_condition('  virtue  ')
        assert 'virtue' in c

    def test_near_case_insensitive(self):
        from lltk.tools.clickhouse_passages import _query_to_ch_condition
        c = _query_to_ch_condition('near(foo bar, 3)')
        assert 'AND' in c
        assert 'foo' in c
        assert 'bar' in c

    def test_snippet_short_text_no_ellipsis(self):
        from lltk.tools.clickhouse_passages import _extract_snippet
        s = _extract_snippet('one two three', 'two', context_words=30)
        assert not s.startswith('...')
        assert not s.endswith('...')

    def test_snippet_context_size(self):
        from lltk.tools.clickhouse_passages import _extract_snippet
        words = ['w'] * 50 + ['TARGET'] + ['w'] * 50
        text = ' '.join(words)
        s = _extract_snippet(text, 'TARGET', context_words=10)
        assert len(s.split()) <= 15  # ~10 + some slop


# ──────────────────────────────────────────────────────────────────────
# Passage export JSONL format
# ──────────────────────────────────────────────────────────────────────

class TestPassageExportFormat:
    def _make_jsonl(self, tmp_path):
        """Create a sample JSONL file in the expected format."""
        out = tmp_path / 'test.jsonl'
        header = {'_id': '_test/foo', 'corpus': 'test', 'scheme': 'p500',
                  'n_passages': 2, 'lang': 'en', 'title': 'Foo', 'author': 'Bar', 'year': 1700}
        p0 = {'seq': 0, 'text': 'First passage text here.', 'n_words': 4}
        p1 = {'seq': 1, 'text': 'Second passage text here.', 'n_words': 4}
        with open(out, 'w') as f:
            f.write(json.dumps(header) + '\n')
            f.write(json.dumps(p0) + '\n')
            f.write(json.dumps(p1) + '\n')
        return out

    def test_header_has_no_text_key(self, tmp_path):
        p = self._make_jsonl(tmp_path)
        with open(p) as f:
            header = json.loads(f.readline())
        assert 'text' not in header
        assert '_id' in header

    def test_passage_rows_have_text_key(self, tmp_path):
        p = self._make_jsonl(tmp_path)
        with open(p) as f:
            lines = [json.loads(l) for l in f]
        passages = [l for l in lines if 'text' in l]
        assert len(passages) == 2

    def test_consumer_pattern(self, tmp_path):
        """The documented consumer pattern: filter on 'text' in row."""
        p = self._make_jsonl(tmp_path)
        metadata = None
        passages = []
        with open(p) as f:
            for line in f:
                row = json.loads(line)
                if 'text' in row:
                    passages.append(row)
                else:
                    metadata = row
        assert metadata is not None
        assert metadata['_id'] == '_test/foo'
        assert len(passages) == 2
        assert passages[0]['seq'] == 0
        assert passages[1]['seq'] == 1

    def test_header_has_metadata_fields(self, tmp_path):
        p = self._make_jsonl(tmp_path)
        with open(p) as f:
            header = json.loads(f.readline())
        for key in ('_id', 'corpus', 'scheme', 'n_passages', 'title', 'author'):
            assert key in header


# ──────────────────────────────────────────────────────────────────────
# Metadata resolution
# ──────────────────────────────────────────────────────────────────────

class TestMetadataResolution:
    def test_metadata_returns_dict(self, text):
        m = text.metadata()
        assert isinstance(m, dict)

    def test_metadata_has_standard_keys(self, text):
        m = text.metadata()
        for key in ('title', 'author', 'id'):
            assert key in m

    def test_metadata_idempotent(self, text):
        m1 = text.metadata()
        m2 = text.metadata()
        assert m1['title'] == m2['title']

    def test_hydrate_meta_runs_once(self, text):
        text._meta_hydrated = False
        text._hydrate_meta()
        assert text._meta_hydrated is True
        title_after = text._meta.get('title')
        text._hydrate_meta()  # should be no-op
        assert text._meta.get('title') == title_after


# ──────────────────────────────────────────────────────────────────────
# Corpus path resolution
# ──────────────────────────────────────────────────────────────────────

class TestCorpusPathResolution:
    def test_path_tasks_exists(self, corpus):
        assert hasattr(corpus, 'path_tasks') or corpus.path_tasks is not None

    def test_path_tasks_under_corpus(self, corpus):
        assert corpus.path_tasks.startswith(corpus.path)
        assert corpus.path_tasks.endswith('tasks')

    def test_path_prosodic_under_corpus(self, corpus):
        assert 'prosodic' in corpus.path_prosodic

    def test_text_from_returns_text(self, corpus):
        t = corpus.text_from('austen_pride')
        assert t is not None
        assert t.id == 'austen_pride'


# ──────────────────────────────────────────────────────────────────────
# Text passages() method
# ──────────────────────────────────────────────────────────────────────

class TestTextPassages:
    def test_passages_returns_corpus(self, text):
        from lltk.corpus.corpus import PassageSectionCorpus
        psg = text.passages(n=500)
        assert isinstance(psg, PassageSectionCorpus)

    def test_passages_parse(self, text):
        psg = text.passages(n=100)
        psg.parse_sections()
        texts = list(psg.texts())
        assert len(texts) > 0

    def test_passages_text_content(self, text):
        psg = text.passages(n=100)
        psg.parse_sections()
        first = list(psg.texts())[0]
        assert first.txt is not None
        assert len(first.txt) > 0

    def test_passages_different_n(self, text):
        psg100 = text.passages(n=100)
        psg100.parse_sections()
        psg500 = text.passages(n=500)
        psg500.parse_sections()
        n100 = len(list(psg100.texts()))
        n500 = len(list(psg500.texts()))
        assert n100 >= n500


# ──────────────────────────────────────────────────────────────────────
# Guard imports for optional deps
# ──────────────────────────────────────────────────────────────────────

class TestOptionalDeps:
    def test_duckdb_guarded_in_metadb(self):
        from lltk.tools import metadb
        # Should import without error even if duckdb missing
        assert hasattr(metadb, 'duckdb')

    def test_db_module_removed(self):
        import importlib
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module('lltk.tools.db')
