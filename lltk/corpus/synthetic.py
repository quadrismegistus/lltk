"""
SyntheticCorpus: a virtual corpus backed by DuckDB queries.
CuratedCorpus: extends SyntheticCorpus with an editable XLSX spreadsheet.

Usage — SyntheticCorpus (virtual, DB-only):

    class BigFiction(SyntheticCorpus):
        SOURCES = {'chadwyck': {'genre': 'Fiction'}, ...}
        DEDUP = True

    C = BigFiction()
    C.meta          # DataFrame from DB query

Usage — CuratedCorpus (DB → XLSX → editable):

    class ArcFiction(CuratedCorpus):
        SOURCES = {'chadwyck': {'genre': 'Fiction'}, ...}
        DEDUP = True

    C = ArcFiction()
    C.curate()      # writes metadata.xlsx with all columns unpacked
    # ... edit in Excel: add 'exclude' column, correct genres, etc. ...
    C = ArcFiction()
    C.meta          # reads from XLSX, filters excluded texts
"""

import json
import os
import pandas as pd
from lltk.corpus.corpus import BaseCorpus
from lltk.text.text import BaseText
from lltk.imports import PATH_CORPUS


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
            # Convert all DuckDB nullable types to standard pandas types
            # (avoids fillna('') TypeError on Int32, etc.)
            for col in df.columns:
                if hasattr(df[col], 'dtype') and 'Int' in str(df[col].dtype):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if '_id' in df.columns and 'id' in df.columns:
                df = df.set_index('id') if 'id' in df.columns else df
        return df if df is not None else pd.DataFrame()

    def metadata(self, **kwargs):
        """Override to skip fillna('') which chokes on DuckDB nullable int types."""
        cache_key = ('load_metadata', True)
        if cache_key not in self._metadfd:
            self._metadfd[cache_key] = self.load_metadata(**kwargs)
        return self._metadfd[cache_key]

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


class CuratedCorpus(SyntheticCorpus):
    """
    A SyntheticCorpus that can be materialized to an editable XLSX spreadsheet.

    Before curate(): behaves like SyntheticCorpus (reads from DB).
    After curate(): reads from XLSX. Manual edits persist.
    Texts still resolve through source corpora via _id.

    Columns:
    - All core DB columns + meta JSON unpacked into real columns
    - 'exclude': set to any truthy value to remove a text (e.g. TRUE, "duplicate", "not fiction")
    - Add any custom columns you want (genre_override, notes, etc.)
    """

    # Priority columns shown first in the spreadsheet
    PRIORITY_COLS = ['_id', 'corpus', 'title', 'author', 'year', 'genre', 'genre_raw',
                     'is_translated', 'exclude']

    @property
    def path_curated(self):
        return os.path.join(PATH_CORPUS, self.id, 'metadata.xlsx')

    @property
    def path_curated_cache(self):
        return os.path.join(PATH_CORPUS, self.id, 'metadata_cache.pkl')

    @property
    def is_curated(self):
        return os.path.exists(self.path_curated)

    def curate(self, force=False):
        """Write metadata to XLSX for editing. Unpacks meta JSON into real columns."""
        from lltk.tools.metadb import metadb

        if self.is_curated and not force:
            print(f'Already curated: {self.path_curated}')
            print(f'Use curate(force=True) to overwrite, or edit the existing file.')
            return self.path_curated

        # Get full data from DB including meta JSON
        qkw = self._get_query_kwargs()
        df = metadb.texts_df(**qkw)
        if df is None or not len(df):
            print('No texts found for this query.')
            return None

        # Unpack meta JSON into real columns
        if 'meta' in df.columns:
            meta_dicts = df['meta'].apply(
                lambda x: json.loads(x) if x and x != 'None' else {}
            )
            meta_df = pd.json_normalize(meta_dicts)
            meta_df.index = df.index
            # Drop columns that already exist as core columns
            meta_df = meta_df.drop(columns=[c for c in meta_df.columns if c in df.columns], errors='ignore')
            # Drop columns that are all null
            meta_df = meta_df.dropna(axis=1, how='all')
            df = pd.concat([df.drop(columns=['meta']), meta_df], axis=1)

        # Convert DuckDB nullable types
        for col in df.columns:
            if hasattr(df[col], 'dtype') and 'Int' in str(df[col].dtype):
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Add exclude column if not present
        if 'exclude' not in df.columns:
            df['exclude'] = ''

        # Reorder columns: priority first, then rest alphabetically
        priority = [c for c in self.PRIORITY_COLS if c in df.columns]
        rest = sorted(c for c in df.columns if c not in priority)
        df = df[priority + rest]

        # Sort by year
        if 'year' in df.columns:
            df = df.sort_values('year', na_position='last')

        # Ensure output directory exists
        os.makedirs(os.path.dirname(self.path_curated), exist_ok=True)

        # Write XLSX
        df.to_excel(self.path_curated, index=True, freeze_panes=(1, 3))
        # Also write pkl cache for fast loading
        df.to_pickle(self.path_curated_cache)

        n = len(df)
        print(f'Wrote {n} texts to {self.path_curated}')
        print(f'Edit in Excel, then reload with lltk.load("{self.id}")')
        return self.path_curated

    def load_metadata(self, **kwargs):
        """Read from curated XLSX if it exists, otherwise from DB."""
        if not self.is_curated:
            return super().load_metadata(**kwargs)

        # Fast path: read from pkl cache if newer than XLSX
        if os.path.exists(self.path_curated_cache):
            xlsx_mtime = os.path.getmtime(self.path_curated)
            cache_mtime = os.path.getmtime(self.path_curated_cache)
            if cache_mtime >= xlsx_mtime:
                df = pd.read_pickle(self.path_curated_cache)
                return self._filter_excluded(df)

        # Read XLSX
        df = pd.read_excel(self.path_curated, index_col=0)

        # Update pkl cache
        df.to_pickle(self.path_curated_cache)

        return self._filter_excluded(df)

    def _filter_excluded(self, df):
        """Remove rows where 'exclude' is truthy."""
        if 'exclude' in df.columns:
            # Keep rows where exclude is empty, NaN, False, or ''
            mask = df['exclude'].isna() | (df['exclude'] == '') | (df['exclude'] == False)
            df = df[mask]
        return df

    def texts(self, progress=False, include_excluded=False, **kwargs):
        """Iterate text objects. Uses curated metadata if available."""
        from lltk.corpus.corpus import Corpus

        df = self.load_metadata() if not include_excluded else self._load_unfiltered()

        if '_id' not in df.columns:
            # _id might be lost if id is the index
            if df.index.name == 'id':
                df = df.reset_index()

        for _, row in df.iterrows():
            _id = row.get('_id', '')
            if not _id:
                continue
            parts = _id.lstrip('_').split('/', 1)
            if len(parts) != 2:
                continue
            corpus_id, text_id = parts
            try:
                t = Corpus(corpus_id).text(text_id)
                yield t
            except Exception:
                continue

    def _load_unfiltered(self):
        """Load metadata without filtering excluded texts."""
        if not self.is_curated:
            return super().load_metadata()
        if os.path.exists(self.path_curated_cache):
            return pd.read_pickle(self.path_curated_cache)
        return pd.read_excel(self.path_curated, index_col=0)

    def refresh(self):
        """Re-generate XLSX from DB, merging with existing manual edits."""
        if not self.is_curated:
            return self.curate()

        # Load current edits
        old_df = pd.read_excel(self.path_curated, index_col=0)
        # Track which columns were manually added (not in DB)
        db_df = super().load_metadata()
        manual_cols = [c for c in old_df.columns if c not in db_df.columns and c != 'exclude']

        # Get fresh DB data
        fresh_path = self.curate(force=True)

        if manual_cols:
            # Merge manual columns back
            fresh_df = pd.read_excel(fresh_path, index_col=0)
            for col in manual_cols + ['exclude']:
                if col in old_df.columns:
                    fresh_df[col] = old_df[col]
            fresh_df.to_excel(self.path_curated, index=True, freeze_panes=(1, 3))
            fresh_df.to_pickle(self.path_curated_cache)
            print(f'Merged manual columns: {manual_cols + ["exclude"]}')

        return fresh_path
