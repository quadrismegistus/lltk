"""
Integration tests for lltk.tools.genre_tags.build_genre_tags against chdb.
"""

import pytest

try:
    import chdb  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False

needs_chdb = pytest.mark.skipif(not _HAS_CHDB, reason='chdb not installed')


def _seed_genre_raw_annotations(ch, rows):
    """Insert genre_raw annotations directly for testing build_genre_tags."""
    from datetime import datetime
    now = datetime.now()
    for _id, source, value in rows:
        ch.client.insert('lltk.annotations', [
            [_id, 'genre_raw', value, source, 'test:1', now, 1.0, '{}'],
        ], column_names=[
            '_id', 'field', 'value', 'source', 'run_id',
            'annotated_at', 'confidence', 'meta',
        ])


@needs_chdb
class TestBuildGenreTags:
    def test_basic_normalization(self, ch):
        from lltk.tools.genre_tags import build_genre_tags
        _seed_genre_raw_annotations(ch, [
            ('_estc/T001', 'human', 'Novel'),
            ('_estc/T002', 'human', 'Gothic, Epistolary fiction'),
        ])
        n = build_genre_tags(ch, progress=False)
        assert n > 0
        df = ch.query_df("SELECT _id, tag, facet FROM lltk.text_genre_tags ORDER BY _id, tag")
        t001_tags = df[df['_id'] == '_estc/T001']['tag'].tolist()
        assert 'novel' in t001_tags
        t002_tags = df[df['_id'] == '_estc/T002']['tag'].tolist()
        assert 'gothic' in t002_tags

    def test_deduplicates_across_sources(self, ch):
        from lltk.tools.genre_tags import build_genre_tags
        _seed_genre_raw_annotations(ch, [
            ('_estc/T001', 'human', 'Novel'),
            ('_estc/T001', 'llm:sonnet', 'Novel'),
        ])
        build_genre_tags(ch, progress=False)
        df = ch.query_df(
            "SELECT tag, count() as n FROM lltk.text_genre_tags "
            "WHERE _id = '_estc/T001' GROUP BY tag"
        )
        for _, row in df.iterrows():
            assert int(row['n']) == 1, f"tag '{row['tag']}' duplicated"

    def test_truncates_on_rebuild(self, ch):
        from lltk.tools.genre_tags import build_genre_tags
        _seed_genre_raw_annotations(ch, [
            ('_estc/T001', 'human', 'Novel'),
        ])
        n1 = build_genre_tags(ch, progress=False)
        n2 = build_genre_tags(ch, progress=False)
        assert n1 == n2
        rows = ch.query("SELECT count() FROM lltk.text_genre_tags")
        assert int(rows[0][0]) == n2

    def test_empty_annotations(self, ch):
        from lltk.tools.genre_tags import build_genre_tags
        n = build_genre_tags(ch, progress=False)
        assert n == 0

    def test_facets_assigned(self, ch):
        from lltk.tools.genre_tags import build_genre_tags
        _seed_genre_raw_annotations(ch, [
            ('_estc/T001', 'human', 'Gothic'),
        ])
        build_genre_tags(ch, progress=False)
        df = ch.query_df(
            "SELECT tag, facet FROM lltk.text_genre_tags WHERE _id = '_estc/T001'"
        )
        facets = df['facet'].tolist()
        assert len(facets) > 0
        assert all(f != '' for f in facets)
