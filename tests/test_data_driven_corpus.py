"""Data-driven corpora: BaseCorpus fallback (no module) + manifest genre keys.

These let a simple corpus that only needs a constant genre drop its boilerplate
class module — the manifest stanza + data drive everything.
"""
import lltk


def test_corpus_without_module_loads_as_basecorpus():
    """A corpus with a manifest stanza but no python module loads as BaseCorpus.

    (clmet's module was removed; it is now manifest-only.)
    """
    from lltk.corpus.corpus import BaseCorpus
    C = lltk.load('clmet')
    assert isinstance(C, BaseCorpus)
    assert type(C).__name__ == 'BaseCorpus'


def test_apply_manifest_genre_stamps_and_noops():
    """_apply_manifest_genre stamps genre/genre_raw from corpus attrs; no-op when unset."""
    import pandas as pd
    C = lltk.load('clmet')  # any BaseCorpus instance
    df = pd.DataFrame({'title': ['x', 'y']})

    # no genre attrs -> columns untouched
    for a in ('genre', 'genre_raw'):
        if hasattr(C, a):
            delattr(C, a)
    out = C._apply_manifest_genre(df.copy())
    assert 'genre' not in out.columns and 'genre_raw' not in out.columns

    # genre attrs set (as a manifest key would) -> stamped
    C.genre, C.genre_raw = 'Fiction', 'Novel'
    out2 = C._apply_manifest_genre(df.copy())
    assert (out2['genre'] == 'Fiction').all()
    assert (out2['genre_raw'] == 'Novel').all()

    # cleanup so the cached instance doesn't leak into other tests
    for a in ('genre', 'genre_raw'):
        if hasattr(C, a):
            delattr(C, a)


def test_apply_manifest_genre_empty_df():
    """No crash on an empty/None frame."""
    import pandas as pd
    C = lltk.load('clmet')
    assert C._apply_manifest_genre(pd.DataFrame()) is not None
    assert C._apply_manifest_genre(None) is None
