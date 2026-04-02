"""
SyntheticCorpus: a virtual corpus backed by DuckDB queries.

Pulls texts from multiple source corpora, optionally deduplicated.
Text objects retain their original corpus for file access (.txt, .freqs).

Usage — declarative:

    class BigFiction(SyntheticCorpus):
        ID = 'big_fiction'
        NAME = 'BigFiction'
        SOURCES = {
            'canon_fiction': {'genre': 'Fiction'},
            'chadwyck': {'genre': 'Fiction'},
            'gildedage': {},
            'hathi_englit': {'genre': 'Fiction'},
        }
        DEDUP = True
        DEDUP_BY = 'oldest'

    C = BigFiction()
    C.meta          # DataFrame
    C.texts()       # text objects from source corpora

Usage — ad-hoc:

    C = SyntheticCorpus(id='my_query', _query_kwargs={'genre': 'Fiction', 'dedup': True})
    C.meta
"""

import pandas as pd
from lltk.corpus.corpus import BaseCorpus
from lltk.text.text import BaseText


class SyntheticCorpus(BaseCorpus):
    TEXT_CLASS = BaseText

    # Subclasses override these
    SOURCES = None       # dict of {corpus_id: {filter_key: value}}
    DEDUP = True
    DEDUP_BY = 'rank'    # 'rank' or 'oldest'

    def __init__(self, id=None, _query_kwargs=None, **kwargs):
        self._query_kwargs = _query_kwargs or {}
        if id is None:
            id = self.ID if hasattr(self, 'ID') and self.ID else '_synthetic'
        super().__init__(id=id, **kwargs)

    def _get_query_kwargs(self):
        """Build query kwargs from either SOURCES class attr or _query_kwargs."""
        if self._query_kwargs:
            return self._query_kwargs
        kw = {}
        if self.SOURCES:
            kw['sources'] = self.SOURCES
        kw['dedup'] = self.DEDUP
        kw['dedup_by'] = self.DEDUP_BY
        return kw

    def load_metadata(self, **kwargs):
        from lltk.tools.metadb import metadb
        qkw = self._get_query_kwargs()
        df = metadb.texts_df(**qkw)
        if df is not None and len(df):
            # Drop the meta JSON column for clean display
            if 'meta' in df.columns:
                df = df.drop(columns=['meta'])
            if '_id' in df.columns and 'id' in df.columns:
                df = df.set_index('id') if 'id' in df.columns else df
        return df if df is not None else pd.DataFrame()

    def init(self, force=False):
        if not force and self._init:
            return self
        self._init = True
        return self

    def iter_init(self, progress=True, **kwargs):
        from lltk.tools.metadb import metadb
        qkw = self._get_query_kwargs()
        yield from metadb.texts(**qkw, progress=progress)

    def texts(self, progress=False, **kwargs):
        return self.iter_init(progress=progress, **kwargs)

    @property
    def textd(self):
        # Materialize on first access
        if not self._textd or all(v is None for v in self._textd.values()):
            for t in self.iter_init(progress=False):
                self._textd[t.id] = t
            self._init = True
        return self._textd

    @property
    def num_texts(self):
        from lltk.tools.metadb import metadb
        qkw = self._get_query_kwargs()
        df = metadb.texts_df(**qkw)
        return len(df) if df is not None else 0
