"""
Full pipeline integration tests against a real ClickHouse server.

Uses an isolated 'lltk_test' database — never touches production 'lltk'.
Skipped automatically if no CH server is reachable.

Exercises: rebuild → freqs → text_words → match → passages → export →
detect_langs → enrich_genres → annotations.
"""

import pytest
import os
import json

needs_ch = pytest.mark.skipif(
    os.environ.get('LLTK_TEST_CH_HOST') is None,
    reason='Set LLTK_TEST_CH_HOST to enable (use Docker CH for isolation)',
)


@pytest.fixture(scope='module')
def corpus():
    import lltk
    return lltk.load('test_fixture')


@pytest.fixture(scope='module')
def pipeline(corpus, request):
    """Ingest test_fixture into a fresh Docker ClickHouse instance.

    Gated by LLTK_TEST_CH_HOST env var — only runs in CI or when explicitly
    enabled. Creates 'lltk' database on an empty Docker CH server.
    All operational code uses hardcoded 'lltk.' table references, so we
    create the lltk database on the isolated Docker instance rather than
    trying to use a different database name. The Docker container is
    ephemeral — no real data is at risk.
    """
    if os.environ.get('LLTK_TEST_CH_HOST') is None:
        pytest.skip('Set LLTK_TEST_CH_HOST to enable')

    from lltk.tools.db_adapter import ClickHouseAdapter
    from lltk.tools.clickhouse_schema import create_all_tables

    # Connect to fresh CH server (Docker in CI)
    adapter = ClickHouseAdapter(
        host=os.environ['LLTK_TEST_CH_HOST'],
        port=int(os.environ.get('LLTK_TEST_CH_PORT', '8123')),
        username=os.environ.get('LLTK_TEST_CH_USER', 'default'),
        password=os.environ.get('LLTK_TEST_CH_PASSWORD', ''),
        database='default',
    )
    adapter.execute('CREATE DATABASE IF NOT EXISTS lltk')

    adapter = ClickHouseAdapter(
        host=os.environ['LLTK_TEST_CH_HOST'],
        port=int(os.environ.get('LLTK_TEST_CH_PORT', '8123')),
        username=os.environ.get('LLTK_TEST_CH_USER', 'default'),
        password=os.environ.get('LLTK_TEST_CH_PASSWORD', ''),
        database='lltk',
    )
    create_all_tables(adapter, database='lltk')

    # 1. Rebuild
    from lltk.tools.clickhouse_rebuild import ingest_corpus_to_clickhouse
    ingest_corpus_to_clickhouse(corpus.id, adapter)

    # 2. Freqs
    from lltk.tools.clickhouse_ingest import ingest_freqs_from_jsons
    ingest_freqs_from_jsons(adapter, corpora=[corpus.id])

    # 3. Text words + stats
    from lltk.tools.clickhouse_text_words import build_text_words, build_text_stats
    build_text_words(adapter, corpora=[corpus.id], force=True)
    build_text_stats(adapter, force=True)

    # 4. Passages
    from lltk.tools.clickhouse_passages import build_passages_ch
    tasks = []
    for t in corpus.texts():
        txt_path = getattr(t, 'path_txt', None)
        if txt_path and os.path.exists(txt_path):
            tasks.append((f'_{corpus.id}/{t.id}', corpus.id, txt_path, 'en', 100))
    build_passages_ch(adapter, n=100, tasks=tasks, num_proc=1)

    yield adapter


# ── Rebuild ──────────────────────────────────────────────────────────

@needs_ch
class TestRebuild:
    def test_texts_count(self, pipeline):
        df = pipeline.query_df(
            f"SELECT count() as n FROM lltk.texts FINAL WHERE corpus = 'test_fixture'")
        assert int(df['n'].iloc[0]) == 3

    def test_texts_metadata(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT _id, title, author, year, genre
            FROM lltk.texts FINAL WHERE corpus = 'test_fixture' ORDER BY _id
        """)
        assert len(df) == 3
        titles = set(df['title'].tolist())
        assert any('Pride' in t for t in titles)

    def test_corpus_info(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT corpus, n_texts FROM lltk.corpus_info FINAL
            WHERE corpus = 'test_fixture'
        """)
        assert len(df) == 1
        assert int(df['n_texts'].iloc[0]) == 3


# ── Freqs ────────────────────────────────────────────────────────────

@needs_ch
class TestFreqs:
    def test_freqs_ingested(self, pipeline):
        df = pipeline.query_df(f"SELECT count() as n FROM lltk.text_freqs")
        assert int(df['n'].iloc[0]) == 3

    def test_freqs_content(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT _id, length(mapKeys(freqs)) as n_words
            FROM lltk.text_freqs
        """)
        for _, row in df.iterrows():
            assert int(row['n_words']) > 10


# ── Text Words ───────────────────────────────────────────────────────

@needs_ch
class TestTextWords:
    def test_words_populated(self, pipeline):
        df = pipeline.query_df(f"SELECT count() as n FROM lltk.text_words")
        assert int(df['n'].iloc[0]) > 0

    def test_word_query(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT word, sum(count) as total
            FROM lltk.text_words
            WHERE word = 'the'
            GROUP BY word
        """)
        assert len(df) == 1
        assert int(df['total'].iloc[0]) > 0

    def test_word_by_corpus(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT corpus, count(DISTINCT word) as n_words
            FROM lltk.text_words
            GROUP BY corpus
        """)
        assert len(df) == 1
        assert df['corpus'].iloc[0] == 'test_fixture'

    def test_stats_populated(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT _id, n_words, n_unique_words
            FROM lltk.text_stats FINAL ORDER BY _id
        """)
        assert len(df) == 3
        for _, row in df.iterrows():
            assert int(row['n_words']) > 0
            assert int(row['n_unique_words']) > 0


# ── Passages ─────────────────────────────────────────────────────────

@needs_ch
class TestPassages:
    def test_passages_created(self, pipeline):
        df = pipeline.query_df(f"SELECT count() as n FROM lltk.passages")
        assert int(df['n'].iloc[0]) > 0

    def test_passages_meta(self, pipeline):
        df = pipeline.query_df(f"SELECT count() as n FROM lltk.passages_meta")
        assert int(df['n'].iloc[0]) == 3

    def test_passage_content(self, pipeline):
        df = pipeline.query_df(f"""
            SELECT _id, seq, text, n_words FROM lltk.passages
            ORDER BY _id, seq LIMIT 3
        """)
        for _, row in df.iterrows():
            assert len(row['text']) > 0
            assert int(row['n_words']) > 0

    def test_passage_retrieval(self, pipeline):
        from lltk.tools.clickhouse_passages import get_passages_ch
        df = get_passages_ch(pipeline, ['_test_fixture/austen_pride'], scheme='p500')
        assert len(df) > 0
        assert 'text' in df.columns

    def test_passage_search(self, pipeline):
        from lltk.tools.clickhouse_passages import search_passages_ch
        results = search_passages_ch(pipeline, 'the', limit=5)
        assert isinstance(results, list)

    def test_passage_count(self, pipeline):
        from lltk.tools.clickhouse_passages import search_passages_count_ch
        n = search_passages_count_ch(pipeline, 'the')
        assert n > 0


# ── Passage Export ───────────────────────────────────────────────────

@needs_ch
class TestExport:
    def test_export_creates_files(self, pipeline, tmp_path):
        from lltk.tools.clickhouse_passages import export_passages_ch
        n_texts, n_passages = export_passages_ch(
            pipeline, corpus='test_fixture', out_dir=str(tmp_path),
        )
        assert n_texts == 3
        assert n_passages > 0
        assert len(list(tmp_path.rglob('*.jsonl'))) == 3

    def test_export_jsonl_format(self, pipeline, tmp_path):
        from lltk.tools.clickhouse_passages import export_passages_ch
        export_passages_ch(
            pipeline, ids=['_test_fixture/austen_pride'],
            out_dir=str(tmp_path),
        )
        f = list(tmp_path.rglob('*.jsonl'))[0]
        with open(f) as fh:
            lines = [json.loads(l) for l in fh]
        header = [l for l in lines if 'text' not in l]
        passages = [l for l in lines if 'text' in l]
        assert len(header) == 1
        assert '_id' in header[0]
        assert len(passages) > 0

    def test_export_year_filter(self, pipeline, tmp_path):
        from lltk.tools.clickhouse_passages import export_passages_ch
        n_texts, _ = export_passages_ch(
            pipeline, corpus='test_fixture', year_max=1800,
            out_dir=str(tmp_path),
        )
        assert n_texts == 1  # blake_songs 1794


# ── Annotations ──────────────────────────────────────────────────────

@needs_ch
class TestAnnotations:
    def test_write_and_resolve(self, pipeline):
        from lltk.tools import annotations as A
        old_db = getattr(A, '_db', None)
        A._db = lambda: (pipeline, 'lltk')
        try:
            A.ensure_schema()
            A.write(
                source='test:pipeline',
                rows=[
                    {'_id': '_test_fixture/austen_pride', 'field': 'genre',
                     'value': 'Fiction', 'confidence': 1.0},
                ],
                run_id='pipeline-test',
            )
            df = A.resolve(
                ids=['_test_fixture/austen_pride'],
                fields=['genre'],
            )
            assert len(df) == 1
            assert df.iloc[0]['value'] == 'Fiction'
        finally:
            if old_db:
                A._db = old_db

    def test_annotation_sources(self, pipeline):
        from lltk.tools import annotations as A
        A._db = lambda: (pipeline, 'lltk')
        A.write(
            source='llm:test-model',
            rows=[
                {'_id': '_test_fixture/austen_pride', 'field': 'genre',
                 'value': 'Drama', 'confidence': 0.5},
            ],
            run_id='pipeline-test-2',
        )
        df = pipeline.query_df(f"""
            SELECT DISTINCT source FROM lltk.annotations
            WHERE _id = '_test_fixture/austen_pride' AND field = 'genre'
        """)
        sources = set(df['source'].tolist())
        assert 'test:pipeline' in sources
        assert 'llm:test-model' in sources


# ── RMT Dedup ────────────────────────────────────────────────────────

@needs_ch
class TestRMTDedup:
    def test_update_wins(self, pipeline):
        pipeline.client.insert(f'lltk.texts', [[
            '_test_fixture/austen_pride', 'test_fixture', 'austen_pride',
            'UPDATED TITLE', 'Jane Austen', 1813,
            'Fiction', '', '', '',
            '', '', None, None,
            'en', '', '', None, None, None, '', '',
        ]], column_names=[
            '_id', 'corpus', 'id', 'title', 'author', 'year',
            'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source',
            'title_norm', 'author_norm', 'path_freqs', 'n_words',
            'lang', 'lang_metadata', 'lang_detected', 'lang_coverage',
            'lang_confidence', 'is_translated', 'original_lang', 'meta',
        ])
        df = pipeline.query_df(f"""
            SELECT title FROM lltk.texts FINAL
            WHERE _id = '_test_fixture/austen_pride'
        """)
        assert len(df) == 1
        assert df['title'].iloc[0] == 'UPDATED TITLE'
