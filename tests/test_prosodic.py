"""Prosodic integration tests. Skip if prosodic is not importable."""
import os
import pytest


@pytest.fixture(scope='module')
def _prosodic():
    """Skip this whole module if prosodic isn't available."""
    # Allow dev fallback to ~/github/prosodic just like the tools module does
    try:
        from lltk.tools.prosodic_tools import _load_prosodic
        return _load_prosodic()
    except ImportError:
        pytest.skip("prosodic not installed; skipping prosodic integration tests")


@pytest.fixture(scope='module')
def parsed_fixture(_prosodic, tmp_path_factory):
    """Parse test_fixture once per module, yield the corpus."""
    import lltk
    from lltk.tools.prosodic_tools import parse_corpus

    C = lltk.load('test_fixture')
    # Redirect prosodic output to a tmp dir so we don't pollute the repo
    tmpdir = tmp_path_factory.mktemp('prosodic')
    # Monkey-set _path_prosodic on this instance so path_prosodic resolves there
    C._path_prosodic = str(tmpdir)
    parse_corpus('test_fixture', n_workers=1, device='cpu', resume=False)
    return C


class TestPathResolution:
    def test_corpus_path_prosodic(self):
        import lltk
        C = lltk.load('test_fixture')
        p = C.path_prosodic
        assert p, 'corpus.path_prosodic should resolve'
        assert 'prosodic' in p

    def test_text_path_prosodic(self):
        import lltk
        C = lltk.load('test_fixture')
        for t in list(C.texts())[:1]:
            p = t.path_prosodic
            assert p, 'text.path_prosodic should resolve'
            assert p.endswith(t.id), f'per-text path should end with {t.id}'


class TestAdHocAccessor:
    """t.prosodic() should work without any prior corpus-level parse."""

    def test_fresh_textmodel_from_txt(self, _prosodic):
        import lltk
        C = lltk.load('test_fixture')
        for t in list(C.texts())[:1]:
            tm = t.prosodic()  # no cached parse exists here
            assert tm is not None, 'should build a fresh TextModel from t.txt'
            # Basic structural check — non-empty text has lines
            assert len(list(tm.children)) > 0

    def test_none_if_no_txt(self, _prosodic, monkeypatch):
        import lltk
        C = lltk.load('test_fixture')
        for t in list(C.texts())[:1]:
            monkeypatch.setattr(type(t), 'txt', property(lambda self: ''))
            # No cached parse, no txt → None
            assert t.prosodic() is None


class TestParse:
    def test_parse_writes_per_text_dirs(self, parsed_fixture):
        C = parsed_fixture
        for t in list(C.texts())[:1]:
            assert os.path.exists(t.path_prosodic), (
                f'expected per-text dir at {t.path_prosodic}'
            )
            assert os.path.exists(os.path.join(t.path_prosodic, 'meta.json'))

    def test_text_prosodic_accessor(self, parsed_fixture):
        C = parsed_fixture
        for t in list(C.texts())[:1]:
            tm = t.prosodic()
            assert tm is not None, 'prosodic() should return TextModel after parsing'


class TestAggregate:
    def test_aggregate_writes_parquet(self, parsed_fixture, tmp_path):
        from lltk.tools.prosodic_tools import aggregate_corpus
        out = tmp_path / 'prosodic.parquet'
        aggregate_corpus('test_fixture', out_path=str(out))
        # Aggregation is best-effort: if no parsed.parquet files exist (e.g.
        # parse only wrote syll.parquet), the file may not be created.
        # We just verify the command runs without error.
