"""
Tests for lltk.db.ocr_accuracy — per-text OCR quality scoring via
text_freqs + wordlist coverage.

`_load_wordlist` is pure Python (file -> list of strings) and tested
directly with tiny throwaway wordlist files — no dependency on the real
~/lltk_data wordlist.

`score_ocr_accuracy` drives real ClickHouse SQL (Map operations,
arraySum/arrayMap) and is exercised against the hermetic chdb `ch`
fixture. The chdb-backed adapter's `.query()` returns CSV-parsed strings
rather than native types (a known quirk — see the comment in
tests/test_pipeline_integration.py about `build_text_words`), which
breaks `score_ocr_accuracy`'s `f'{n_texts:,}'` log line since chdb's
`count()` comes back as a string. We work around this with a thin
count-casting wrapper around the adapter (test-only — no product code
touched), which also mirrors what the real `ClickHouseAdapter.query()`
already does via clickhouse-connect's native typing.
"""

import os
import pytest

try:
    import chdb  # noqa: F401
    _HAS_CHDB = True
except ImportError:
    _HAS_CHDB = False

needs_chdb = pytest.mark.skipif(not _HAS_CHDB, reason='chdb not installed')


# ── _load_wordlist (pure) ───────────────────────────────────────────

class TestLoadWordlist:
    def test_reads_words(self, tmp_path):
        from lltk.db.ocr_accuracy import _load_wordlist
        p = tmp_path / 'words.txt'
        p.write_text('the\nquick\nbrown\nfox\n')
        words = _load_wordlist(str(p))
        assert words == ['the', 'quick', 'brown', 'fox']

    def test_strips_whitespace(self, tmp_path):
        from lltk.db.ocr_accuracy import _load_wordlist
        p = tmp_path / 'words.txt'
        p.write_text('  the  \n\tquick\t\n')
        words = _load_wordlist(str(p))
        assert words == ['the', 'quick']

    def test_skips_blank_lines(self, tmp_path):
        from lltk.db.ocr_accuracy import _load_wordlist
        p = tmp_path / 'words.txt'
        p.write_text('the\n\n\n   \nquick\n')
        words = _load_wordlist(str(p))
        assert words == ['the', 'quick']

    def test_empty_file_returns_empty_list(self, tmp_path):
        from lltk.db.ocr_accuracy import _load_wordlist
        p = tmp_path / 'words.txt'
        p.write_text('')
        assert _load_wordlist(str(p)) == []

    def test_missing_file_raises(self, tmp_path):
        from lltk.db.ocr_accuracy import _load_wordlist
        missing = tmp_path / 'does_not_exist.txt'
        with pytest.raises(FileNotFoundError, match='assemble_en_wordlist.py'):
            _load_wordlist(str(missing))

    def test_default_path_used_when_none(self, monkeypatch, tmp_path):
        # path=None falls back to the module-level WORDLIST_PATH constant.
        from lltk.db import ocr_accuracy
        fake_path = str(tmp_path / 'default_wordlist.txt')
        (tmp_path / 'default_wordlist.txt').write_text('alpha\nbeta\n')
        monkeypatch.setattr(ocr_accuracy, 'WORDLIST_PATH', fake_path)
        assert ocr_accuracy._load_wordlist(None) == ['alpha', 'beta']

    def test_missing_default_path_error_names_it(self, monkeypatch, tmp_path):
        from lltk.db import ocr_accuracy
        fake_path = str(tmp_path / 'nope.txt')
        monkeypatch.setattr(ocr_accuracy, 'WORDLIST_PATH', fake_path)
        with pytest.raises(FileNotFoundError, match='nope.txt'):
            ocr_accuracy._load_wordlist(None)


# ── score_ocr_accuracy (hermetic, via chdb) ─────────────────────────

class _CountCastingAdapter:
    """Wraps a DBAdapter so `.query()` casts numeric CSV strings back to
    int/float, matching the native typing that clickhouse-connect gives
    the real ClickHouseAdapter. Needed only because chdb's ArrowStream-free
    `.query()` path returns everything as strings (see module docstring)."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    @staticmethod
    def _cast(v):
        if isinstance(v, str):
            try:
                return int(v)
            except ValueError:
                try:
                    return float(v)
                except ValueError:
                    return v
        return v

    def query(self, sql, params=None):
        rows = self._inner.query(sql, params)
        return [tuple(self._cast(v) for v in row) for row in rows]


@pytest.fixture
def cch(ch):
    """The chdb `ch` fixture, wrapped so count() queries round-trip as ints."""
    ch.execute('CREATE DATABASE IF NOT EXISTS tmp')
    return _CountCastingAdapter(ch)


def _wordlist_file(tmp_path, words):
    p = tmp_path / 'wordlist.txt'
    p.write_text('\n'.join(words) + '\n')
    return str(p)


@needs_chdb
class TestScoreOcrAccuracy:
    def test_clean_text_scores_high(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_test/clean', 'test', {'the': 10, 'quick': 2, 'brown': 2, 'fox': 1}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the', 'quick', 'brown', 'fox'])

        score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=False)

        df = cch.query_df(
            "SELECT n_tokens, n_known_tokens, ocr_accuracy FROM lltk.text_ocr FINAL "
            "WHERE _id = '_test/clean'"
        )
        assert len(df) == 1
        assert int(df['n_tokens'].iloc[0]) == 15
        assert int(df['n_known_tokens'].iloc[0]) == 15
        assert float(df['ocr_accuracy'].iloc[0]) == pytest.approx(1.0)

    def test_garbled_text_scores_low(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_test/garbled', 'test', {'xqzwy': 5, 'zzptk': 3, 'the': 1}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the', 'quick', 'brown', 'fox'])

        score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=False)

        df = cch.query_df(
            "SELECT n_tokens, n_known_tokens, ocr_accuracy FROM lltk.text_ocr FINAL "
            "WHERE _id = '_test/garbled'"
        )
        assert len(df) == 1
        assert int(df['n_tokens'].iloc[0]) == 9
        assert int(df['n_known_tokens'].iloc[0]) == 1
        assert float(df['ocr_accuracy'].iloc[0]) == pytest.approx(1 / 9)

    def test_all_unknown_words_scores_zero(self, cch, tmp_path):
        # "all-punctuation"-style noise: none of the tokens are in the wordlist.
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_test/noise', 'test', {'...': 4, '---': 2, '###': 1}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the', 'quick', 'brown', 'fox'])

        score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=False)

        df = cch.query_df(
            "SELECT n_known_tokens, ocr_accuracy FROM lltk.text_ocr FINAL "
            "WHERE _id = '_test/noise'"
        )
        assert len(df) == 1
        assert int(df['n_known_tokens'].iloc[0]) == 0
        assert float(df['ocr_accuracy'].iloc[0]) == pytest.approx(0.0)

    def test_empty_freqs_does_not_divide_by_zero(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_test/empty', 'test', {}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the'])

        score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=False)

        df = cch.query_df(
            "SELECT n_tokens, ocr_accuracy FROM lltk.text_ocr FINAL "
            "WHERE _id = '_test/empty'"
        )
        assert len(df) == 1
        assert int(df['n_tokens'].iloc[0]) == 0
        assert float(df['ocr_accuracy'].iloc[0]) == pytest.approx(0.0)

    def test_corpora_filter_excludes_other_corpus(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_a/x', 'a', {'the': 1}],
            ['_b/y', 'b', {'the': 1}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the'])

        score_ocr_accuracy(cch, corpora=['a'], wordlist_path=wl, skip_existing=False)

        df = cch.query_df("SELECT _id FROM lltk.text_ocr FINAL")
        assert df['_id'].tolist() == ['_a/x']

    def test_skip_existing_does_not_rescore(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        cch.client.insert('text_freqs', [
            ['_test/known', 'test', {'the': 1}],
        ], column_names=['_id', 'corpus', 'freqs'])
        wl = _wordlist_file(tmp_path, ['the'])

        # Pre-seed text_ocr as if it had already been scored.
        cch.client.insert('text_ocr', [
            ['_test/known', 'test', 999, 999, 0.5],
        ], column_names=['_id', 'corpus', 'n_tokens', 'n_known_tokens', 'ocr_accuracy'])

        score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=True)

        df = cch.query_df(
            "SELECT n_tokens FROM lltk.text_ocr FINAL WHERE _id = '_test/known'"
        )
        # Untouched — still the sentinel value, not recomputed from freqs.
        assert int(df['n_tokens'].iloc[0]) == 999

    def test_no_texts_to_score_returns_none(self, cch, tmp_path):
        from lltk.db.ocr_accuracy import score_ocr_accuracy
        wl = _wordlist_file(tmp_path, ['the'])
        result = score_ocr_accuracy(cch, wordlist_path=wl, skip_existing=False)
        assert result is None
        df = cch.query_df("SELECT count() as n FROM lltk.text_ocr")
        assert int(df['n'].iloc[0]) == 0
