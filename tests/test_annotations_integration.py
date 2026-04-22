"""
Integration tests for lltk.tools.annotations against chdb.

Monkeypatches annotations._db to use the chdb fixture instead of the
global lltk.db singleton, so the full write/resolve/disagreements/mirror
pipeline runs against a real ClickHouse engine.
"""

import pytest

try:
    import chdb  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False

needs_chdb = pytest.mark.skipif(not _HAS_CHDB, reason='chdb not installed')


@pytest.fixture
def ann(ch, monkeypatch):
    """Patch annotations._db to use the chdb fixture."""
    import importlib
    A = importlib.import_module('lltk.tools.annotations')
    monkeypatch.setattr(A, '_db', lambda: (ch, 'lltk'))
    return A


def _insert_text(ch, _id, corpus, genre='', genre_raw='', genre_enriched_source=''):
    ch.client.insert('lltk.texts', [[
        _id, corpus, _id.split('/')[-1], 'Title', 'Author', 1750,
        genre, genre_raw, '', genre_enriched_source, '', '',
        None, None, 'en', '', '', None, None, None, '', '',
    ]], column_names=[
        '_id', 'corpus', 'id', 'title', 'author', 'year',
        'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source',
        'title_norm', 'author_norm', 'path_freqs', 'n_words',
        'lang', 'lang_metadata', 'lang_detected', 'lang_coverage',
        'lang_confidence', 'is_translated', 'original_lang', 'meta',
    ])


# ── seed_default_sources ──────────────────────────────────────────────

@needs_chdb
class TestSeedSources:
    def test_seeds_all_defaults(self, ann, ch):
        written = ann.seed_default_sources()
        assert 'human' in written
        assert 'heuristic' in written
        rows = ch.query("SELECT source, priority FROM lltk.annotation_sources FINAL ORDER BY source")
        sources = {r[0] for r in rows}
        assert 'human' in sources

    def test_idempotent(self, ann, ch):
        ann.seed_default_sources()
        second = ann.seed_default_sources()
        assert second == []


# ── write ─────────────────────────────────────────────────────────────

@needs_chdb
class TestWrite:
    def test_basic_write(self, ann, ch):
        ann.seed_default_sources()
        n = ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
            {'_id': '_estc/T002', 'field': 'genre', 'value': 'Poetry'},
        ])
        assert n == 2
        rows = ch.query("SELECT count() FROM lltk.annotations")
        assert int(rows[0][0]) == 2

    def test_validates_field(self, ann, ch):
        with pytest.raises(ValueError, match='unknown field'):
            ann.write('human', [
                {'_id': '_estc/T001', 'field': 'nonexistent', 'value': 'x'},
            ])

    def test_validates_vocab(self, ann, ch):
        with pytest.raises(ValueError, match='not in controlled vocab'):
            ann.write('human', [
                {'_id': '_estc/T001', 'field': 'genre', 'value': 'Novella'},
            ])

    def test_skip_validation(self, ann, ch):
        n = ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'AnythingGoes'},
        ], validate=False)
        assert n == 1

    def test_auto_registers_unknown_source(self, ann, ch):
        ann.write('llm:test-model', [
            {'_id': '_estc/T001', 'field': 'genre_raw', 'value': 'Novel'},
        ])
        rows = ch.query(
            "SELECT priority FROM lltk.annotation_sources FINAL "
            "WHERE source = 'llm:test-model'"
        )
        assert int(rows[0][0]) == ann.DEFAULT_LLM_PRIORITY

    def test_confidence_and_meta(self, ann, ch):
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction',
             'confidence': 0.9, 'meta': {'model': 'test'}},
        ])
        df = ch.query_df("SELECT confidence, meta FROM lltk.annotations")
        assert float(df['confidence'].iloc[0]) == pytest.approx(0.9, abs=0.01)
        assert 'test' in df['meta'].iloc[0]

    def test_rejects_missing_id(self, ann, ch):
        with pytest.raises(ValueError, match='missing _id'):
            ann.write('human', [{'field': 'genre', 'value': 'Fiction'}])

    def test_empty_rows_returns_zero(self, ann, ch):
        assert ann.write('human', []) == 0

    def test_bool_field_coercion(self, ann, ch):
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'is_translated', 'value': True},
        ])
        df = ch.query_df("SELECT value FROM lltk.annotations WHERE field = 'is_translated'")
        assert df['value'].iloc[0] == '1'


# ── resolve ───────────────────────────────────────────────────────────

@needs_chdb
class TestResolve:
    def _seed(self, ann, ch):
        ann.seed_default_sources()
        ann.write('llm:sonnet', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Poetry'},
        ])
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])

    def test_picks_highest_priority(self, ann, ch):
        self._seed(ann, ch)
        df = ann.resolve(ids=['_estc/T001'], fields=['genre'])
        assert len(df) == 1
        assert df['value'].iloc[0] == 'Fiction'
        assert df['source'].iloc[0] == 'human'

    def test_decode_types(self, ann, ch):
        ann.seed_default_sources()
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'is_translated', 'value': True},
            {'_id': '_estc/T001', 'field': 'year_estimated', 'value': 1789},
        ])
        df = ann.resolve(ids=['_estc/T001'])
        is_trans = df[df['field'] == 'is_translated']['value'].iloc[0]
        year = df[df['field'] == 'year_estimated']['value'].iloc[0]
        assert is_trans is True
        assert year == 1789

    def test_empty_ids_returns_empty_df(self, ann, ch):
        df = ann.resolve(ids=[], fields=['genre'])
        assert len(df) == 0

    def test_no_filter_returns_all(self, ann, ch):
        self._seed(ann, ch)
        df = ann.resolve()
        assert len(df) >= 1


# ── resolve_by_source ────────────────────────────────────────────────

@needs_chdb
class TestResolveBySource:
    def test_returns_single_source(self, ann, ch):
        ann.seed_default_sources()
        ann.write('llm:sonnet', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Poetry'},
        ])
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])
        df = ann.resolve_by_source('llm:sonnet', ids=['_estc/T001'])
        assert len(df) == 1
        assert df['value'].iloc[0] == 'Poetry'

    def test_empty_ids(self, ann, ch):
        df = ann.resolve_by_source('human', ids=[])
        assert len(df) == 0


# ── disagreements ─────────────────────────────────────────────────────

@needs_chdb
class TestDisagreements:
    def test_finds_disagreement(self, ann, ch):
        ann.seed_default_sources()
        ann.write('llm:sonnet', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Poetry'},
        ])
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])
        df = ann.disagreements('genre', min_sources=2)
        assert len(df) == 1
        assert '_estc/T001' in df['_id'].values

    def test_no_disagreement_when_same_value(self, ann, ch):
        ann.seed_default_sources()
        ann.write('llm:sonnet', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])
        df = ann.disagreements('genre', min_sources=2)
        assert len(df) == 0

    def test_min_sources_threshold(self, ann, ch):
        ann.seed_default_sources()
        ann.write('human', [
            {'_id': '_estc/T001', 'field': 'genre', 'value': 'Fiction'},
        ])
        df = ann.disagreements('genre', min_sources=2)
        assert len(df) == 0


# ── mirror_genres_from_texts ──────────────────────────────────────────

@needs_chdb
class TestMirrorGenres:
    def test_mirrors_genre_and_raw(self, ann, ch):
        _insert_text(ch, '_estc/T001', 'estc', genre='Fiction',
                     genre_raw='Novel', genre_enriched_source='corpus')
        _insert_text(ch, '_estc/T002', 'estc', genre='Poetry',
                     genre_raw='Verse', genre_enriched_source='bibliography:fiction_biblio')
        n = ann.mirror_genres_from_texts()
        assert n == 4  # 2 genre + 2 genre_raw

        df = ch.query_df(
            "SELECT _id, field, value, source FROM lltk.annotations ORDER BY _id, field"
        )
        genre_rows = df[df['field'] == 'genre']
        assert len(genre_rows) == 2

        t002_genre = genre_rows[genre_rows['_id'] == '_estc/T002']
        assert t002_genre['source'].iloc[0] == 'bibliography:fiction_biblio'

        t001_genre = genre_rows[genre_rows['_id'] == '_estc/T001']
        assert t001_genre['source'].iloc[0] == 'corpus:estc'

    def test_idempotent_with_replace(self, ann, ch):
        _insert_text(ch, '_estc/T001', 'estc', genre='Fiction', genre_raw='Novel')
        n1 = ann.mirror_genres_from_texts()
        n2 = ann.mirror_genres_from_texts()
        assert n1 == n2
        rows = ch.query("SELECT count() FROM lltk.annotations")
        assert int(rows[0][0]) == n2

    def test_skips_empty_values(self, ann, ch):
        _insert_text(ch, '_estc/T001', 'estc', genre='Fiction', genre_raw='')
        n = ann.mirror_genres_from_texts()
        assert n == 1  # only genre, not genre_raw
