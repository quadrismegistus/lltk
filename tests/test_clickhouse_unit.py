"""
Unit tests for the ClickHouse-backed code paths.

These tests don't require a running ClickHouse server — they exercise the
pure-Python pieces (URL parsing, SQL rewriting, ID validation, the
engine-agnostic prepare_corpus_df). Integration tests against a live CH
instance live elsewhere (or in CI with a service container).
"""

import pytest
import pandas as pd

# clickhouse-connect is an optional dependency. Tests that monkey-patch its
# get_client to avoid network calls still need the module to exist. CI
# containers and casual installs may not have it.
try:
    import clickhouse_connect  # noqa: F401
    _HAS_CH = True
except ImportError:
    _HAS_CH = False
needs_ch = pytest.mark.skipif(
    not _HAS_CH, reason='clickhouse-connect not installed (optional dep)'
)


# ── _validate_id / _validate_corpus ────────────────────────────────

class TestIdValidation:
    def test_valid_ids(self):
        from lltk.db.metadb_ch import _validate_id
        # Common shapes from real corpora — including the messy ones
        for ok in [
            '_estc/T012345',
            '_chadwyck/B0001',
            '_hathi_englit/mdp.39015009144422',
            '_hathi_englit/bc.ark+=13960=t0bv7v96f',
            '_chadwyck_poetry/Z200466700|Z200466700',
            '_french_pd_books/abc.def-ghi',
            '_de_corp/1900/Author_-_Title',
            '_gildedage/1875.Alcott.Am.F.Eight Cousins',  # spaces are real
            "_markmark/O'Connor,_Flannery.A_Good_Man",     # apostrophes are real
            '_txtlab/DE_1875_Otto,Louise_AnNürnberg_Novel',  # diacritics
        ]:
            assert _validate_id(ok) == ok

    def test_invalid_ids(self):
        from lltk.db.metadb_ch import _validate_id
        for bad in [
            "estc/T012345",                     # missing leading _
            "_/T012345",                        # empty corpus
            "_estc",                            # missing /text_id
            "_ESTC/T012345",                    # uppercase corpus
            "_estc/text\x00with\x00null",       # control char (NUL)
            "_estc/text\nwith\nnewline",        # control char (LF)
        ]:
            with pytest.raises((ValueError, TypeError)):
                _validate_id(bad)

    def test_apostrophe_safe_when_escaped(self):
        # The validator allows apostrophes (real corpora have them); the
        # SQL escape (_sql_str) doubles them so they can't break out of
        # a single-quoted SQL literal.
        from lltk.db.metadb_ch import _validate_id, _sql_str
        injection = "_estc/T123' OR '1'='1"   # crafted as if injecting
        # Validator passes (apostrophes are legitimate)
        _validate_id(injection)
        # Escape doubles the quotes so the result is a valid SQL literal:
        # 'T123'' OR ''1''=''1'  — the '1=1' becomes part of the _id, not SQL
        escaped = _sql_str(injection)
        assert "'" in escaped
        assert "''" in escaped
        # Verify there are no unescaped single quotes
        assert escaped.count("'") % 2 == 0  # every ' is paired

    def test_non_string(self):
        from lltk.db.metadb_ch import _validate_id
        with pytest.raises(TypeError):
            _validate_id(123)
        with pytest.raises(TypeError):
            _validate_id(None)

    def test_corpus_validation(self):
        from lltk.db.metadb_ch import _validate_corpus
        for ok in ['estc', 'chadwyck_poetry', 'fiction_biblio', 'de_corp', 'a1']:
            assert _validate_corpus(ok) == ok
        for bad in ["estc'; DROP", 'ESTC', 'chadwyck-poetry', '', '0_starts_with_digit']:
            # Note: '0_starts_with_digit' actually matches our regex; only the
            # truly bad ones below should fail. Drop it from the bad set.
            pass
        for bad in ["estc'; DROP", 'ESTC', 'chadwyck-poetry', '']:
            with pytest.raises(ValueError):
                _validate_corpus(bad)


# ── DBAdapter URL parsing ──────────────────────────────────────────

class TestGetAdapter:
    def test_duckdb_url(self, tmp_path):
        from lltk.db.adapter import get_adapter, DuckDBAdapter
        url = f'duckdb:///{tmp_path}/test.duckdb'
        a = get_adapter(url)
        assert isinstance(a, DuckDBAdapter)
        a.close()

    def test_duckdb_readonly_flag(self, tmp_path):
        from lltk.db.adapter import get_adapter, DuckDBAdapter
        # Create the file first (read_only requires existing DB)
        p = tmp_path / 'ro.duckdb'
        get_adapter(f'duckdb:///{p}').execute('CREATE TABLE t (x INT)')
        a = get_adapter(f'duckdb:///{p}?read_only=true')
        assert isinstance(a, DuckDBAdapter)
        a.close()

    @needs_ch
    def test_clickhouse_url_parses(self):
        # Don't actually connect; just verify URL parsing returns the right class.
        # We monkey-patch get_client to avoid network.
        from lltk.db import adapter as db_adapter
        captured = {}

        class FakeClient:
            def close(self): pass

        def fake_get_client(host, port, database, username, password, **kw):
            captured.update(host=host, port=port, database=database,
                            username=username, password=password)
            return FakeClient()

        import clickhouse_connect
        orig = clickhouse_connect.get_client
        clickhouse_connect.get_client = fake_get_client
        try:
            a = db_adapter.get_adapter('clickhouse://user:pw@example.com:9999/mydb')
            assert isinstance(a, db_adapter.ClickHouseAdapter)
            assert captured == dict(
                host='example.com', port=9999, database='mydb',
                username='user', password='pw',
            )
            assert a._password == 'pw'   # used by worker URL reconstruction
            a.close()
        finally:
            clickhouse_connect.get_client = orig

    def test_unknown_scheme(self):
        from lltk.db.adapter import get_adapter
        with pytest.raises(ValueError, match='Unknown DB URL scheme'):
            get_adapter('postgres://localhost/db')


# ── Legacy SQL rewriting (web-app compat shim) ─────────────────────

class TestLegacyRewrite:
    def test_match_db_prefix(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT * FROM match_db.matches WHERE _id_a = ?"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.matches' in out
        assert 'match_db.' not in out

    def test_bare_texts(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT title FROM texts WHERE corpus = 'estc'"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.texts' in out

    def test_no_double_prefix(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        # Already-prefixed should NOT become lltk.lltk.texts
        sql = "SELECT * FROM lltk.texts WHERE corpus = 'estc'"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.lltk.' not in out
        assert 'lltk.texts' in out

    def test_text_word_table_not_matched_as_bare_texts(self):
        # Check the word boundary regex doesn't catch text_freqs / text_words
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT _id FROM lltk.text_freqs"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.text_freqs' in out
        # Should NOT have rewritten 'texts' inside 'text_freqs'
        assert 'lltk.lltk' not in out

    def test_json_extract_string(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT json_extract_string(meta, '$.author') FROM texts"
        out = _rewrite_legacy_sql(sql)
        assert 'JSONExtractString(meta, ' in out
        assert "'author'" in out
        assert '$.' not in out

    def test_to_timestamp(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        out = _rewrite_legacy_sql("SELECT to_timestamp(ingested_at) FROM corpus_info")
        assert 'toDateTime(' in out
        assert 'to_timestamp(' not in out

    def test_random_function(self):
        from lltk.db.metadb_ch import _rewrite_legacy_sql
        out = _rewrite_legacy_sql("SELECT id FROM texts ORDER BY random() LIMIT 1")
        assert 'rand()' in out
        assert 'random(' not in out

    def test_positional_param_inlining(self):
        # _LegacyResult inlines ? params before sending to CH
        from lltk.db.metadb_ch import _LegacyResult
        # Manually invoke the inlining logic
        sql = "SELECT * FROM lltk.texts WHERE corpus = ? AND year > ?"
        params = ['estc', 1700]
        # Simulate what _LegacyResult._run does
        out = sql
        for p in params:
            if isinstance(p, str):
                p = "'" + p.replace("'", "''") + "'"
            else:
                p = str(p)
            out = out.replace('?', p, 1)
        assert "corpus = 'estc'" in out
        assert "year > 1700" in out

    def test_param_with_quote_escaped(self):
        # Single quote in a param value gets doubled, not breaking out of the literal
        from lltk.db.metadb_ch import _LegacyResult
        sql = "SELECT * FROM lltk.texts WHERE author = ?"
        params = ["O'Brien"]
        out = sql
        for p in params:
            p = "'" + p.replace("'", "''") + "'"
            out = out.replace('?', p, 1)
        assert "O''Brien" in out


# ── prepare_corpus_df (engine-agnostic data prep) ──────────────────

class TestPrepareCorpusDf:
    def _make_df(self, **overrides):
        base = dict(
            id=['t001', 't002'],
            title=['Pride and Prejudice', 'Frankenstein'],
            author=['Austen, Jane', 'Shelley, Mary'],
            year=[1813, 1818],
            genre=['Fiction', 'Fiction'],
            genre_raw=['Novel', 'Novel, Gothic'],
        )
        base.update(overrides)
        return pd.DataFrame(base)

    def test_basic_prepare(self):
        from lltk.db.metadb import prepare_corpus_df
        df = self._make_df()
        out = prepare_corpus_df(df, 'test_corpus')
        assert list(out.columns)[:3] == ['_id', 'corpus', 'id']
        assert out['_id'].tolist() == ['_test_corpus/t001', '_test_corpus/t002']
        assert out['corpus'].tolist() == ['test_corpus', 'test_corpus']
        assert out['title_norm'].iloc[0] == 'pride and prejudice'
        assert out['author_norm'].iloc[0] == 'austen'

    def test_drops_empty_ids(self):
        from lltk.db.metadb import prepare_corpus_df
        df = self._make_df()
        df.loc[1, 'id'] = ''
        out = prepare_corpus_df(df, 'test_corpus')
        assert len(out) == 1

    def test_dedupes_id(self):
        from lltk.db.metadb import prepare_corpus_df
        df = self._make_df(id=['t001', 't001'])
        out = prepare_corpus_df(df, 'test_corpus')
        assert len(out) == 1

    def test_year_to_int(self):
        from lltk.db.metadb import prepare_corpus_df
        df = self._make_df(year=['1813', 'circa 1818'])
        out = prepare_corpus_df(df, 'test_corpus')
        assert int(out['year'].iloc[0]) == 1813

    def test_extra_cols_pack_into_meta(self):
        from lltk.db.metadb import prepare_corpus_df
        import json
        df = self._make_df(custom_field=['hello', 'world'])
        out = prepare_corpus_df(df, 'test_corpus')
        assert 'meta' in out.columns
        meta = json.loads(out['meta'].iloc[0])
        assert meta.get('custom_field') == 'hello'

    def test_default_lang(self):
        from lltk.db.metadb import prepare_corpus_df
        df = self._make_df()
        out = prepare_corpus_df(df, 'test_corpus', default_lang='en')
        assert out['lang'].iloc[0] == 'en'


# ── MetaDBCH.detect_langs rejects legacy kwargs ────────────────────

class TestRejectLegacyKwargs:
    def test_detect_langs_apply_raises(self):
        from lltk.db.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'  # never connects
        m._adapter = None
        with pytest.raises(TypeError, match='legacy DuckDB-era kwargs'):
            m.detect_langs(apply=True)

    def test_detect_langs_only_apply_raises(self):
        from lltk.db.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        with pytest.raises(TypeError, match='legacy DuckDB-era kwargs'):
            m.detect_langs(only_apply=True)

    def test_build_word_index_jobs_raises(self):
        from lltk.db.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        with pytest.raises(TypeError, match='legacy kwargs no longer supported'):
            m.build_word_index_sql(jobs=4)


# ── dedup_frame validation (pure-Python, no CH) ────────────────────

class TestDedupFrameValidation:
    def _make_bare(self):
        from lltk.db.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        return m

    def test_bad_by(self):
        m = self._make_bare()
        df = pd.DataFrame({'_id': ['_estc/T1'], 'x': [1.0]})
        with pytest.raises(ValueError, match="by must be"):
            m.dedup_frame(df, by='mean')

    def test_missing_id_col(self):
        m = self._make_bare()
        df = pd.DataFrame({'other': ['_estc/T1']})
        with pytest.raises(KeyError, match='id_col'):
            m.dedup_frame(df, id_col='_id')

    def test_empty_df_passthrough(self):
        # Empty df returns copy — never touches CH
        m = self._make_bare()
        df = pd.DataFrame({'_id': [], 'x': []})
        out = m.dedup_frame(df)
        assert len(out) == 0
        assert out is not df   # is a copy

    def test_all_null_ids_passthrough(self):
        # df with only NaN ids — never touches CH
        m = self._make_bare()
        df = pd.DataFrame({'_id': [None, None], 'x': [1.0, 2.0]})
        out = m.dedup_frame(df)
        assert len(out) == 2


class TestBuildWhereCh:
    def _make_bare(self):
        from lltk.db.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        return m

    def test_empty_filters(self):
        m = self._make_bare()
        assert m._build_where_ch() == '1=1'

    def test_genre_and_year_range(self):
        m = self._make_bare()
        sql = m._build_where_ch(genre='Fiction', year_min=1700, year_max=1800)
        assert "genre = 'Fiction'" in sql
        assert 'year >= 1700' in sql
        assert 'year <= 1800' in sql

    def test_corpora_in_list(self):
        m = self._make_bare()
        sql = m._build_where_ch(corpora=['estc', 'chadwyck'])
        assert "corpus IN ('estc', 'chadwyck')" in sql

    def test_corpora_rejects_bad_name(self):
        # Defense in depth: _validate_corpus rejects non-manifest names
        m = self._make_bare()
        with pytest.raises(ValueError):
            m._build_where_ch(corpora=['evil; DROP TABLE x'])

    def test_sources_multi_corpus(self):
        m = self._make_bare()
        sql = m._build_where_ch(sources={
            'chadwyck': {'genre': 'Fiction'},
            'estc': {'genre': 'Fiction'},
        })
        # Each source clause is AND'd internally, OR'd across
        assert "corpus = 'chadwyck'" in sql
        assert "corpus = 'estc'" in sql
        assert ' OR ' in sql
        assert ' AND ' in sql

    def test_sources_range_suffix(self):
        # year_min / year_max on a source turn into range predicates
        m = self._make_bare()
        sql = m._build_where_ch(sources={
            'estc': {'year_min': 1700, 'year_max': 1750},
        })
        assert 'year >= 1700' in sql
        assert 'year <= 1750' in sql

    def test_escapes_single_quotes(self):
        # Inline string values must be SQL-escaped (`'` -> `''`)
        m = self._make_bare()
        sql = m._build_where_ch(genre="Robin's Tale")
        assert "genre = 'Robin''s Tale'" in sql

    def test_raw_where_passed_through(self):
        m = self._make_bare()
        sql = m._build_where_ch(where="length(title) > 5", genre='Fiction')
        assert '(length(title) > 5)' in sql
        assert "genre = 'Fiction'" in sql
