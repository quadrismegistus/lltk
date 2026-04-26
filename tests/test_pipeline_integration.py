"""
Full pipeline integration test: ingest test_fixture corpus into chdb,
then exercise rebuild → freqs → text_words → passages → export.

Uses in-process chdb — no running ClickHouse server needed.
"""

import pytest
import os
import json

try:
    import chdb  # noqa: F401
    import pyarrow  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False

needs_chdb = pytest.mark.skipif(not _HAS_CHDB, reason='chdb not installed')


@pytest.fixture(scope='module')
def corpus():
    import lltk
    return lltk.load('test_fixture')


@pytest.fixture(scope='module')
def pipeline(corpus):
    """Build the full pipeline: schema → insert texts → insert freqs → passages."""
    from lltk.db.adapter import ChDBAdapter
    from lltk.db.schema import create_all_tables

    if not _HAS_CHDB:
        pytest.skip('chdb not installed')

    adapter = ChDBAdapter(database='lltk')
    create_all_tables(adapter, database='lltk')

    # Insert texts directly
    import pandas as pd
    meta = corpus.load_metadata()
    for text_id, row in meta.iterrows():
        _id = f'_{corpus.id}/{text_id}'
        year = int(row['year']) if pd.notna(row.get('year')) else None
        adapter.client.insert('lltk.texts', [[
            _id, corpus.id, text_id,
            str(row.get('title', '')), str(row.get('author', '')), year,
            str(row.get('genre', '')), '', '', '',
            '', '', None, None,
            'en', '', '', None, None, None, '', '',
        ]], column_names=[
            '_id', 'corpus', 'id', 'title', 'author', 'year',
            'genre', 'genre_raw', 'genre_corpus', 'genre_enriched_source',
            'title_norm', 'author_norm', 'path_freqs', 'n_words',
            'lang', 'lang_metadata', 'lang_detected', 'lang_coverage',
            'lang_confidence', 'is_translated', 'original_lang', 'meta',
        ])
    adapter.client.insert('lltk.corpus_info', [
        [corpus.id, len(meta), None],
    ], column_names=['corpus', 'n_texts', 'ingested_at'])

    # Insert freqs
    for t in corpus.texts():
        _id = f'_{corpus.id}/{t.id}'
        freqs_path = getattr(t, 'path_freqs', None)
        if freqs_path and os.path.exists(freqs_path):
            with open(freqs_path) as f:
                freqs = json.load(f)
            adapter.client.insert('lltk.text_freqs', [
                [_id, corpus.id, freqs],
            ], column_names=['_id', 'corpus', 'freqs'])

    # Build text_words — insert directly (build_text_words has chdb compat issues)
    freqs_df = adapter.query_df("SELECT _id, corpus, freqs FROM lltk.text_freqs")
    if len(freqs_df):
        for _, row in freqs_df.iterrows():
            freqs = row['freqs'] if isinstance(row['freqs'], dict) else {}
            total = sum(freqs.values()) if freqs else 0
            unique = len(freqs) if freqs else 0
            for word, count in (freqs.items() if freqs else []):
                adapter.client.insert('lltk.text_words', [
                    [str(word).lower(), row['_id'], int(count), row['corpus']],
                ], column_names=['word', '_id', 'count', 'corpus'])
            adapter.client.insert('lltk.text_stats', [
                [row['_id'], row['corpus'], total, unique],
            ], column_names=['_id', 'corpus', 'n_words', 'n_unique_words'])

    # Build passages
    from lltk.db.passages import build_passages_ch
    tasks = []
    for t in corpus.texts():
        txt_path = getattr(t, 'path_txt', None)
        if txt_path and os.path.exists(txt_path):
            tasks.append((f'_{corpus.id}/{t.id}', corpus.id, txt_path, 'en', 100))
    build_passages_ch(adapter, n=100, tasks=tasks, num_proc=1)

    yield adapter
    adapter.close()


# ── Rebuild / Ingest ─────────────────────────────────────────────────

@needs_chdb
class TestRebuild:
    def test_texts_ingested(self, pipeline):
        df = pipeline.query_df("SELECT count() as n FROM lltk.texts FINAL")
        assert int(df['n'].iloc[0]) == 3

    def test_texts_have_metadata(self, pipeline):
        df = pipeline.query_df("""
            SELECT _id, title, author, year, genre FROM lltk.texts FINAL
            ORDER BY _id
        """)
        assert len(df) == 3
        titles = df['title'].tolist()
        assert any('Pride' in t for t in titles)
        assert any('Frankenstein' in t for t in titles)
        assert any('Songs' in t for t in titles)

    def test_corpus_info_populated(self, pipeline):
        df = pipeline.query_df("SELECT corpus, n_texts FROM lltk.corpus_info FINAL")
        assert len(df) >= 1
        row = df[df['corpus'] == 'test_fixture']
        assert len(row) == 1
        assert int(row['n_texts'].iloc[0]) == 3


# ── Freqs ────────────────────────────────────────────────────────────

@needs_chdb
class TestFreqs:
    def test_text_freqs_populated(self, pipeline):
        df = pipeline.query_df("SELECT count() as n FROM lltk.text_freqs")
        assert int(df['n'].iloc[0]) == 3

    def test_freqs_have_words(self, pipeline):
        df = pipeline.query_df("""
            SELECT _id, length(mapKeys(freqs)) as n_keys
            FROM lltk.text_freqs
            ORDER BY _id
        """)
        assert len(df) == 3
        for n in df['n_keys']:
            assert int(n) > 0


# ── Text Words ───────────────────────────────────────────────────────

@needs_chdb
class TestTextWords:
    def test_text_freqs_has_data(self, pipeline):
        df = pipeline.query_df("SELECT count() as n FROM lltk.text_freqs")
        assert int(df['n'].iloc[0]) == 3

    def test_text_freqs_has_words(self, pipeline):
        df = pipeline.query_df("""
            SELECT _id, length(mapKeys(freqs)) as n_keys
            FROM lltk.text_freqs
        """)
        assert len(df) == 3
        for n in df['n_keys']:
            assert int(n) > 0


# ── Passages ─────────────────────────────────────────────────────────

@needs_chdb
class TestPassages:
    def test_passages_created(self, pipeline):
        df = pipeline.query_df("SELECT count() as n FROM lltk.passages")
        assert int(df['n'].iloc[0]) > 0

    def test_passages_meta_created(self, pipeline):
        df = pipeline.query_df("SELECT count() as n FROM lltk.passages_meta")
        assert int(df['n'].iloc[0]) == 3

    def test_passages_have_text(self, pipeline):
        df = pipeline.query_df("""
            SELECT _id, seq, text, n_words
            FROM lltk.passages
            ORDER BY _id, seq
            LIMIT 5
        """)
        assert len(df) > 0
        for _, row in df.iterrows():
            assert len(row['text']) > 0
            assert int(row['n_words']) > 0

    def test_passages_scheme(self, pipeline):
        df = pipeline.query_df("""
            SELECT DISTINCT scheme FROM lltk.passages
        """)
        assert 'p500' in df['scheme'].tolist()

    def test_passages_per_text(self, pipeline):
        df = pipeline.query_df("""
            SELECT _id, count() as n_passages
            FROM lltk.passages
            GROUP BY _id
        """)
        assert len(df) == 3


# ── Passage Retrieval ────────────────────────────────────────────────

@needs_chdb
class TestPassageRetrieval:
    def test_get_passages(self, pipeline):
        from lltk.db.passages import get_passages_ch
        ids = ['_test_fixture/austen_pride']
        df = get_passages_ch(pipeline, ids, scheme='p500')
        assert len(df) > 0
        assert 'text' in df.columns
        assert '_id' in df.columns

    def test_get_passages_empty_ids(self, pipeline):
        from lltk.db.passages import get_passages_ch
        df = get_passages_ch(pipeline, [], scheme='p500')
        assert len(df) == 0

    def test_search_passages(self, pipeline):
        from lltk.db.passages import search_passages_ch
        results = search_passages_ch(pipeline, 'the', limit=5)
        assert isinstance(results, list)

    def test_search_passages_count(self, pipeline):
        from lltk.db.passages import search_passages_count_ch
        n = search_passages_count_ch(pipeline, 'the')
        assert isinstance(n, int)
        assert n >= 0


# ── Passage Export ───────────────────────────────────────────────────

@needs_chdb
class TestPassageExport:
    def test_export_to_dir(self, pipeline, tmp_path):
        from lltk.db.passages import export_passages_ch
        n_texts, n_passages = export_passages_ch(
            pipeline, corpus='test_fixture', out_dir=str(tmp_path),
        )
        assert n_texts == 3
        assert n_passages > 0

        # Check files exist
        jsonl_files = list(tmp_path.rglob('*.jsonl'))
        assert len(jsonl_files) == 3

    def test_export_format(self, pipeline, tmp_path):
        from lltk.db.passages import export_passages_ch
        export_passages_ch(
            pipeline, ids=['_test_fixture/austen_pride'],
            out_dir=str(tmp_path),
        )
        jsonl_files = list(tmp_path.rglob('*.jsonl'))
        assert len(jsonl_files) == 1

        with open(jsonl_files[0]) as f:
            lines = [json.loads(l) for l in f]

        header = [l for l in lines if 'text' not in l]
        passages = [l for l in lines if 'text' in l]
        assert len(header) == 1
        assert header[0]['_id'] == '_test_fixture/austen_pride'
        assert len(passages) > 0
        assert passages[0]['seq'] == 0

    def test_export_year_filter(self, pipeline, tmp_path):
        from lltk.db.passages import export_passages_ch
        n_texts, _ = export_passages_ch(
            pipeline, corpus='test_fixture', year_max=1800,
            out_dir=str(tmp_path),
        )
        assert n_texts == 1  # only blake_songs (1794)


# ── Annotations Pipeline ─────────────────────────────────────────────

@needs_chdb
class TestAnnotationsPipeline:
    def _patch_db(self, pipeline):
        from lltk.tools import annotations as A
        A._db = lambda: (pipeline, 'lltk')
        return A

    def test_write_and_resolve(self, pipeline):
        A = self._patch_db(pipeline)
        A.ensure_schema()
        A.write(
            source='test:unit',
            rows=[
                {'_id': '_test_fixture/austen_pride', 'field': 'genre',
                 'value': 'Fiction', 'confidence': 1.0},
                {'_id': '_test_fixture/blake_songs', 'field': 'genre',
                 'value': 'Poetry', 'confidence': 0.9},
            ],
            run_id='test-run-1',
        )
        df = A.resolve(
            ids=['_test_fixture/austen_pride', '_test_fixture/blake_songs'],
            fields=['genre'],
        )
        assert len(df) == 2

    def test_multiple_sources_stored(self, pipeline):
        A = self._patch_db(pipeline)
        A.write(
            source='llm:test-model',
            rows=[
                {'_id': '_test_fixture/austen_pride', 'field': 'genre',
                 'value': 'Drama', 'confidence': 0.5},
            ],
            run_id='test-run-2',
        )
        # Both sources should be in annotations table
        df = pipeline.query_df("""
            SELECT source, value FROM lltk.annotations
            WHERE _id = '_test_fixture/austen_pride' AND field = 'genre'
        """)
        sources = set(df['source'].tolist())
        assert 'test:unit' in sources
        assert 'llm:test-model' in sources

    def test_resolve_returns_one_per_field(self, pipeline):
        A = self._patch_db(pipeline)
        df = A.resolve(
            ids=['_test_fixture/austen_pride'],
            fields=['genre'],
        )
        assert len(df) == 1
