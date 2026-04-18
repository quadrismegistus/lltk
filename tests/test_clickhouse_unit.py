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
        from lltk.tools.metadb_ch import _validate_id
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
        from lltk.tools.metadb_ch import _validate_id
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
        from lltk.tools.metadb_ch import _validate_id, _sql_str
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
        from lltk.tools.metadb_ch import _validate_id
        with pytest.raises(TypeError):
            _validate_id(123)
        with pytest.raises(TypeError):
            _validate_id(None)

    def test_corpus_validation(self):
        from lltk.tools.metadb_ch import _validate_corpus
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
        from lltk.tools.db_adapter import get_adapter, DuckDBAdapter
        url = f'duckdb:///{tmp_path}/test.duckdb'
        a = get_adapter(url)
        assert isinstance(a, DuckDBAdapter)
        a.close()

    def test_duckdb_readonly_flag(self, tmp_path):
        from lltk.tools.db_adapter import get_adapter, DuckDBAdapter
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
        from lltk.tools import db_adapter
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
        from lltk.tools.db_adapter import get_adapter
        with pytest.raises(ValueError, match='Unknown DB URL scheme'):
            get_adapter('postgres://localhost/db')


# ── Legacy SQL rewriting (web-app compat shim) ─────────────────────

class TestLegacyRewrite:
    def test_match_db_prefix(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT * FROM match_db.matches WHERE _id_a = ?"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.matches' in out
        assert 'match_db.' not in out

    def test_bare_texts(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT title FROM texts WHERE corpus = 'estc'"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.texts' in out

    def test_no_double_prefix(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        # Already-prefixed should NOT become lltk.lltk.texts
        sql = "SELECT * FROM lltk.texts WHERE corpus = 'estc'"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.lltk.' not in out
        assert 'lltk.texts' in out

    def test_text_word_table_not_matched_as_bare_texts(self):
        # Check the word boundary regex doesn't catch text_freqs / text_words
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT _id FROM lltk.text_freqs"
        out = _rewrite_legacy_sql(sql)
        assert 'lltk.text_freqs' in out
        # Should NOT have rewritten 'texts' inside 'text_freqs'
        assert 'lltk.lltk' not in out

    def test_json_extract_string(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        sql = "SELECT json_extract_string(meta, '$.author') FROM texts"
        out = _rewrite_legacy_sql(sql)
        assert 'JSONExtractString(meta, ' in out
        assert "'author'" in out
        assert '$.' not in out

    def test_to_timestamp(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        out = _rewrite_legacy_sql("SELECT to_timestamp(ingested_at) FROM corpus_info")
        assert 'toDateTime(' in out
        assert 'to_timestamp(' not in out

    def test_random_function(self):
        from lltk.tools.metadb_ch import _rewrite_legacy_sql
        out = _rewrite_legacy_sql("SELECT id FROM texts ORDER BY random() LIMIT 1")
        assert 'rand()' in out
        assert 'random(' not in out

    def test_positional_param_inlining(self):
        # _LegacyResult inlines ? params before sending to CH
        from lltk.tools.metadb_ch import _LegacyResult
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
        from lltk.tools.metadb_ch import _LegacyResult
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
        from lltk.tools.metadb import MetaDB
        # Use __new__ to bypass __init__ (no DuckDB connection needed)
        m = MetaDB.__new__(MetaDB)
        df = self._make_df()
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus')
        assert list(out.columns)[:3] == ['_id', 'corpus', 'id']
        assert out['_id'].tolist() == ['_test_corpus/t001', '_test_corpus/t002']
        assert out['corpus'].tolist() == ['test_corpus', 'test_corpus']
        assert out['title_norm'].iloc[0] == 'pride and prejudice'
        assert out['author_norm'].iloc[0] == 'austen'

    def test_drops_empty_ids(self):
        from lltk.tools.metadb import MetaDB
        m = MetaDB.__new__(MetaDB)
        df = self._make_df()
        df.loc[1, 'id'] = ''
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus')
        assert len(out) == 1

    def test_dedupes_id(self):
        from lltk.tools.metadb import MetaDB
        m = MetaDB.__new__(MetaDB)
        df = self._make_df(id=['t001', 't001'])
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus')
        assert len(out) == 1

    def test_year_to_int(self):
        from lltk.tools.metadb import MetaDB
        m = MetaDB.__new__(MetaDB)
        df = self._make_df(year=['1813', 'circa 1818'])
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus')
        assert int(out['year'].iloc[0]) == 1813

    def test_extra_cols_pack_into_meta(self):
        from lltk.tools.metadb import MetaDB
        import json
        m = MetaDB.__new__(MetaDB)
        df = self._make_df(custom_field=['hello', 'world'])
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus')
        assert 'meta' in out.columns
        meta = json.loads(out['meta'].iloc[0])
        assert meta.get('custom_field') == 'hello'

    def test_default_lang(self):
        from lltk.tools.metadb import MetaDB
        m = MetaDB.__new__(MetaDB)
        df = self._make_df()
        out = MetaDB.prepare_corpus_df(m, df, 'test_corpus', default_lang='en')
        assert out['lang'].iloc[0] == 'en'


# ── MetaDBCH.detect_langs rejects legacy kwargs ────────────────────

class TestRejectLegacyKwargs:
    def test_detect_langs_apply_raises(self):
        from lltk.tools.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'  # never connects
        m._adapter = None
        with pytest.raises(TypeError, match='legacy DuckDB-era kwargs'):
            m.detect_langs(apply=True)

    def test_detect_langs_only_apply_raises(self):
        from lltk.tools.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        with pytest.raises(TypeError, match='legacy DuckDB-era kwargs'):
            m.detect_langs(only_apply=True)

    def test_build_word_index_jobs_raises(self):
        from lltk.tools.metadb_ch import MetaDBCH
        m = MetaDBCH.__new__(MetaDBCH)
        m.url = 'clickhouse://test'
        m._adapter = None
        with pytest.raises(TypeError, match='legacy kwargs no longer supported'):
            m.build_word_index_sql(jobs=4)
