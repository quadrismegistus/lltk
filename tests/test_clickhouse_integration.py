"""
Integration tests against a real ClickHouse engine (chdb, in-process).

These exercise the actual CH SQL dialect — RMT dedup, FINAL, VIEWs,
arrayMap, annotation priority resolution — without a running server.
"""

import pytest

try:
    import chdb  # noqa: F401
    import pyarrow as pa
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False
    pa = None

needs_chdb = pytest.mark.skipif(not _HAS_CHDB, reason='chdb not installed')


def _insert_text(ch, _id, corpus, title, author='', year=None, genre='',
                 genre_raw='', title_norm='', author_norm='', lang=''):
    ch.client.insert('lltk.texts', [[
        _id, corpus, _id.split('/')[-1], title, author, year,
        genre, genre_raw, '', '', title_norm, author_norm,
        None, None, lang, '', '', None, None, None, '', '',
    ]], column_names=[
        '_id', 'corpus', 'id', 'title', 'author', 'year',
        'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source',
        'title_norm', 'author_norm', 'path_freqs', 'n_words',
        'lang', 'lang_metadata', 'lang_detected', 'lang_coverage',
        'lang_confidence', 'is_translated', 'original_lang', 'meta',
    ])


# ── Schema creation ──────────────────────────────────────────────────

@needs_chdb
class TestSchemaCreation:
    def test_all_tables_created(self, ch):
        from lltk.tools.clickhouse_schema import CLICKHOUSE_SCHEMA
        for name in CLICKHOUSE_SCHEMA:
            assert ch.table_exists(f'lltk.{name}'), f'{name} not created'

    def test_views_created(self, ch):
        assert ch.table_exists('lltk.annotations_latest')
        assert ch.table_exists('lltk.annotations_by_source')

    def test_idempotent_recreation(self, ch):
        from lltk.tools.clickhouse_schema import create_all_tables
        tables = create_all_tables(ch, database='lltk')
        assert 'texts' in tables


# ── RMT dedup (ReplacingMergeTree + FINAL) ────────────────────────────

@needs_chdb
class TestRMTDedup:
    def test_final_deduplicates(self, ch):
        _insert_text(ch, '_estc/T001', 'estc', 'Original Title')
        _insert_text(ch, '_estc/T001', 'estc', 'Updated Title')
        df = ch.query_df("SELECT title FROM lltk.texts FINAL WHERE _id = '_estc/T001'")
        assert len(df) == 1
        assert df['title'].iloc[0] == 'Updated Title'

    def test_different_ids_preserved(self, ch):
        _insert_text(ch, '_estc/T010', 'estc', 'Book A')
        _insert_text(ch, '_estc/T011', 'estc', 'Book B')
        df = ch.query_df("SELECT _id FROM lltk.texts FINAL WHERE _id LIKE '_estc/T01%' ORDER BY _id")
        assert len(df) == 2


# ── Match groups ──────────────────────────────────────────────────────

@needs_chdb
class TestMatchGroups:
    def test_insert_and_query(self, ch):
        ch.client.insert_arrow('matches', pa.Table.from_arrays(
            [pa.array(['_a/1', '_a/2']), pa.array(['_b/1', '_b/2']),
             pa.array([0.95, 0.88], type=pa.float32()),
             pa.array(['exact_norm', 'exact_norm'])],
            names=['_id_a', '_id_b', 'similarity', 'match_type'],
        ))
        rows = ch.query("SELECT count() FROM lltk.matches")
        assert int(rows[0][0]) == 2

    def test_match_groups_rmt(self, ch):
        ch.client.insert('lltk.match_groups',
            [['_x/1', 1, 1], ['_x/2', 1, 2], ['_x/3', 2, 1]],
            column_names=['_id', 'group_id', 'rank'],
        )
        df = ch.query_df(
            "SELECT group_id, count() as n FROM lltk.match_groups FINAL "
            "GROUP BY group_id ORDER BY group_id"
        )
        assert df['n'].tolist() == [2, 1]


# ── Annotations + priority resolution ────────────────────────────────

@needs_chdb
class TestAnnotations:
    def _seed_sources(self, ch):
        ch.client.insert('lltk.annotation_sources',
            [['human', 100, 'manual'], ['llm:sonnet', 10, 'LLM'],
             ['corpus:estc', 30, 'corpus']],
            column_names=['source', 'priority', 'description'],
        )

    def test_latest_picks_highest_priority(self, ch):
        self._seed_sources(ch)
        ch.execute(
            "INSERT INTO lltk.annotations (_id, field, value, source, run_id) VALUES "
            "('_estc/T001', 'genre', 'Poetry', 'llm:sonnet', 'r1'), "
            "('_estc/T001', 'genre', 'Fiction', 'human', 'r2')"
        )
        df = ch.query_df(
            "SELECT value, source FROM lltk.annotations_latest "
            "WHERE _id = '_estc/T001' AND field = 'genre'"
        )
        assert len(df) == 1
        assert df['value'].iloc[0] == 'Fiction'
        assert df['source'].iloc[0] == 'human'

    def test_by_source_keeps_all(self, ch):
        self._seed_sources(ch)
        ch.execute(
            "INSERT INTO lltk.annotations (_id, field, value, source, run_id) VALUES "
            "('_estc/T050', 'genre', 'Poetry', 'llm:sonnet', 'r1'), "
            "('_estc/T050', 'genre', 'Fiction', 'human', 'r2'), "
            "('_estc/T050', 'genre', 'Drama', 'corpus:estc', 'r3')"
        )
        df = ch.query_df(
            "SELECT source, value FROM lltk.annotations_by_source "
            "WHERE _id = '_estc/T050' AND field = 'genre' "
            "ORDER BY source"
        )
        assert len(df) == 3
        sources = set(df['source'].tolist())
        assert sources == {'human', 'llm:sonnet', 'corpus:estc'}

    def test_by_source_dedup_within_source(self, ch):
        self._seed_sources(ch)
        ch.execute(
            "INSERT INTO lltk.annotations (_id, field, value, source, run_id) VALUES "
            "('_estc/T060', 'genre', 'Poetry', 'llm:sonnet', 'r1')"
        )
        ch.execute(
            "INSERT INTO lltk.annotations (_id, field, value, source, run_id) VALUES "
            "('_estc/T060', 'genre', 'Fiction', 'llm:sonnet', 'r2')"
        )
        df = ch.query_df(
            "SELECT value FROM lltk.annotations_by_source "
            "WHERE _id = '_estc/T060' AND field = 'genre' AND source = 'llm:sonnet'"
        )
        assert len(df) == 1
        assert df['value'].iloc[0] == 'Fiction'


# ── Passages ──────────────────────────────────────────────────────────

@needs_chdb
class TestPassages:
    def test_insert_and_retrieve(self, ch):
        ch.client.insert('lltk.passages', [
            ['_c/1', 'corp', 'p500', 0, 'The quick brown fox.', 4, 'en'],
            ['_c/1', 'corp', 'p500', 1, 'Jumped over the lazy dog.', 5, 'en'],
        ], column_names=['_id', 'corpus', 'scheme', 'seq', 'text', 'n_words', 'lang'])
        df = ch.query_df(
            "SELECT _id, seq, text FROM lltk.passages "
            "WHERE _id = '_c/1' ORDER BY seq"
        )
        assert len(df) == 2
        assert df['text'].iloc[0] == 'The quick brown fox.'

    def test_passages_meta_rmt(self, ch):
        ch.client.insert('lltk.passages_meta',
            [['_c/10', 'corp', 'p500', 2]],
            column_names=['_id', 'corpus', 'scheme', 'n_passages'],
        )
        ch.client.insert('lltk.passages_meta',
            [['_c/10', 'corp', 'p500', 5]],
            column_names=['_id', 'corpus', 'scheme', 'n_passages'],
        )
        df = ch.query_df(
            "SELECT n_passages FROM lltk.passages_meta FINAL WHERE _id = '_c/10'"
        )
        assert len(df) == 1
        assert int(df['n_passages'].iloc[0]) == 5


# ── Embeddings (Array(Float32) + server-side dot product) ─────────────

@needs_chdb
class TestEmbeddings:
    def test_insert_and_retrieve_vector(self, ch):
        vec = [0.1, 0.2, 0.3, 0.4]
        ch.client.insert('lltk.passage_embeddings', [
            ['_c/1', 'p500', 0, 'e5-large', vec],
        ], column_names=['_id', 'scheme', 'seq', 'model', 'embedding'])
        df = ch.query_df(
            "SELECT embedding FROM lltk.passage_embeddings WHERE _id = '_c/1'"
        )
        import numpy as np
        retrieved = np.array(df['embedding'].iloc[0], dtype=np.float32)
        np.testing.assert_allclose(retrieved, vec, atol=1e-6)

    def test_server_side_dot_product(self, ch):
        v1 = [1.0, 0.0, 0.0]
        v2 = [0.0, 1.0, 0.0]
        v3 = [0.707, 0.707, 0.0]
        ch.client.insert('lltk.passage_embeddings', [
            ['_d/1', 'p500', 0, 'test', v1],
            ['_d/2', 'p500', 0, 'test', v2],
            ['_d/3', 'p500', 0, 'test', v3],
        ], column_names=['_id', 'scheme', 'seq', 'model', 'embedding'])

        query_vec = '[1.0, 0.0, 0.0]'
        df = ch.query_df(f"""
            SELECT _id,
                   arraySum(arrayMap((a, b) -> a * b, embedding, {query_vec})) AS score
            FROM lltk.passage_embeddings
            WHERE model = 'test'
            ORDER BY score DESC
        """)
        assert df['_id'].iloc[0] == '_d/1'
        assert float(df['score'].iloc[0]) == pytest.approx(1.0, abs=1e-3)
        assert float(df['score'].iloc[1]) == pytest.approx(0.707, abs=1e-2)

    def test_mean_pool_numpy(self, ch):
        import numpy as np
        ch.client.insert('lltk.passage_embeddings', [
            ['_e/1', 'p500', 0, 'test', [1.0, 0.0]],
            ['_e/1', 'p500', 1, 'test', [0.0, 1.0]],
        ], column_names=['_id', 'scheme', 'seq', 'model', 'embedding'])
        df = ch.query_df(
            "SELECT embedding FROM lltk.passage_embeddings "
            "WHERE _id = '_e/1' AND model = 'test'"
        )
        vectors = np.array(df['embedding'].tolist(), dtype=np.float32)
        mean = vectors.mean(axis=0)
        norm = np.linalg.norm(mean)
        mean /= norm
        expected = np.array([0.707, 0.707], dtype=np.float32)
        np.testing.assert_allclose(mean, expected, atol=1e-2)

    def test_embeddings_meta_idempotency(self, ch):
        ch.client.insert('lltk.passage_embeddings_meta',
            [['_e/10', 'p500', 'e5', 3]],
            column_names=['_id', 'scheme', 'model', 'n_passages'],
        )
        df = ch.query_df(
            "SELECT DISTINCT _id FROM lltk.passage_embeddings_meta FINAL "
            "WHERE model = 'e5'"
        )
        assert '_e/10' in df['_id'].tolist()


# ── Language detection tables ─────────────────────────────────────────

@needs_chdb
class TestLangDetection:
    def test_text_words_order_by(self, ch):
        ch.client.insert('lltk.text_words', [
            ['the', '_c/1', 50, 'corp'],
            ['the', '_c/2', 30, 'corp'],
            ['virtue', '_c/1', 5, 'corp'],
        ], column_names=['word', '_id', 'count', 'corpus'])
        df = ch.query_df(
            "SELECT _id, sum(count) as total FROM lltk.text_words "
            "WHERE word = 'the' GROUP BY _id ORDER BY _id"
        )
        assert len(df) == 2
        assert int(df['total'].iloc[0]) == 50

    def test_stopwords_join(self, ch):
        ch.execute("""
            CREATE TABLE IF NOT EXISTS lltk.stopwords (
                word LowCardinality(String), lang LowCardinality(String),
                weight Float32 DEFAULT 1.0
            ) ENGINE = MergeTree() ORDER BY word
        """)
        ch.execute(
            "INSERT INTO lltk.stopwords (word, lang, weight) VALUES "
            "('the', 'en', 1.0), ('le', 'fr', 1.0), ('the', 'fr', 0.2)"
        )
        ch.client.insert('lltk.text_words', [
            ['the', '_lang/1', 100, 'corp'],
            ['le', '_lang/1', 5, 'corp'],
        ], column_names=['word', '_id', 'count', 'corpus'])

        df = ch.query_df("""
            SELECT sw.lang,
                   sum(tw.count * sw.weight) AS hits
            FROM lltk.text_words tw
            INNER JOIN lltk.stopwords sw ON tw.word = sw.word
            WHERE tw._id = '_lang/1'
            GROUP BY sw.lang
            ORDER BY hits DESC
        """)
        assert df['lang'].iloc[0] == 'en'
        assert float(df['hits'].iloc[0]) == 100.0

    def test_text_langs_rmt(self, ch):
        ch.client.insert('lltk.text_langs', [
            ['_l/1', 'en', 0.15, 5.0],
        ], column_names=['_id', 'lang_detected', 'lang_coverage', 'lang_confidence'])
        ch.client.insert('lltk.text_langs', [
            ['_l/1', 'fr', 0.20, 8.0],
        ], column_names=['_id', 'lang_detected', 'lang_coverage', 'lang_confidence'])
        df = ch.query_df("SELECT lang_detected FROM lltk.text_langs FINAL WHERE _id = '_l/1'")
        assert len(df) == 1
        assert df['lang_detected'].iloc[0] == 'fr'


# ── Genre enrichment tables ──────────────────────────────────────────

@needs_chdb
class TestGenreEnrichment:
    def test_text_genres_rmt(self, ch):
        ch.client.insert('lltk.text_genres', [
            ['_g/1', 'Fiction', 'Novel', 'Fiction', 'corpus'],
        ], column_names=['_id', 'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source'])
        ch.client.insert('lltk.text_genres', [
            ['_g/1', 'Fiction', 'Novel', 'Fiction', 'bibliography:fiction_biblio'],
        ], column_names=['_id', 'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source'])
        df = ch.query_df(
            "SELECT genre_enriched_source FROM lltk.text_genres FINAL WHERE _id = '_g/1'"
        )
        assert len(df) == 1
        assert df['genre_enriched_source'].iloc[0] == 'bibliography:fiction_biblio'

    def test_genre_tags_insert(self, ch):
        ch.client.insert('lltk.text_genre_tags', [
            ['_g/1', 'novel', 'form'],
            ['_g/1', 'gothic', 'mode'],
        ], column_names=['_id', 'tag', 'facet'])
        df = ch.query_df(
            "SELECT tag, facet FROM lltk.text_genre_tags WHERE _id = '_g/1' ORDER BY tag"
        )
        assert df['tag'].tolist() == ['gothic', 'novel']


# ── ChDBAdapter specific ─────────────────────────────────────────────

@needs_chdb
class TestChDBAdapter:
    def test_engine_is_clickhouse(self, ch):
        assert ch.engine == 'clickhouse'

    def test_table_exists_true(self, ch):
        assert ch.table_exists('lltk.texts')

    def test_table_exists_false(self, ch):
        assert not ch.table_exists('lltk.nonexistent_table')

    def test_query_empty_table(self, ch):
        rows = ch.query("SELECT count() FROM lltk.texts")
        assert int(rows[0][0]) == 0

    def test_query_df_empty(self, ch):
        df = ch.query_df("SELECT * FROM lltk.texts FINAL WHERE 1=0")
        assert len(df) == 0
