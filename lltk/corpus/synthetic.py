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
    def path_annotations(self):
        return os.path.join(os.path.expanduser(PATH_CORPUS), self.id, 'annotations.json')

    @property
    def is_curated(self):
        return os.path.exists(self.path_annotations)

    def _load_annotations(self):
        """Load annotations from JSON file."""
        if os.path.exists(self.path_annotations):
            with open(self.path_annotations) as f:
                return json.load(f)
        return {}

    def annotate(self, port=8989):
        """Launch the annotation web app for this corpus."""
        from lltk.web.annotate import run_annotate
        run_annotate(self.id, port=port)

    def propagate_from(self, source_corpus_id, columns=None, dry_run=False):
        """Propagate metadata from a source corpus via match groups into annotations.

        Finds texts in this corpus's SOURCES that share a match group with a
        source_corpus_id text, and writes annotations for the specified columns.
        Skips texts that already have a direct annotation for those columns.

        Args:
            source_corpus_id: corpus to propagate from (e.g. 'fiction_biblio')
            columns: list of columns to propagate (default: ['genre'])
            dry_run: if True, print what would be written without saving
        Returns:
            dict of {_id: annotation_dict} that were written (or would be)
        """
        import lltk
        if columns is None:
            columns = ['genre']

        annotations = self._load_annotations()

        # Get all source corpus _ids and their metadata
        source_texts = lltk.db.query(f"""
            SELECT _id, {', '.join(columns)}
            FROM texts WHERE corpus = '{source_corpus_id}'
        """)
        if not len(source_texts):
            print(f'No texts found for corpus {source_corpus_id}')
            return {}

        # Get match groups for source texts
        source_ids = set(source_texts['_id'])
        mg = lltk.db.match_conn.execute(
            "SELECT _id, group_id FROM match_db.match_groups"
        ).fetchdf()
        if not len(mg):
            print('No match groups found')
            return {}

        id_to_group = dict(zip(mg['_id'], mg['group_id']))
        group_to_ids = {}
        for _id, gid in zip(mg['_id'], mg['group_id']):
            group_to_ids.setdefault(gid, []).append(_id)

        # Build source values per group (track which source _id provided them)
        source_vals = {}
        for _, row in source_texts.iterrows():
            gid = id_to_group.get(row['_id'])
            if gid is not None:
                vals = {col: row[col] for col in columns if pd.notna(row[col])}
                if vals:
                    source_vals[gid] = (row['_id'], vals)

        # Get valid target corpora from SOURCES
        target_corpora = set(self.SOURCES.keys()) if self.SOURCES else set()

        # Find targets to annotate
        written = {}
        for gid, (source_id, vals) in source_vals.items():
            for _id in group_to_ids.get(gid, []):
                if _id in source_ids:
                    continue
                # Check target is in our SOURCES corpora
                corpus_id = _id.lstrip('_').split('/')[0]
                if target_corpora and corpus_id not in target_corpora:
                    continue
                # Skip columns already in direct annotations; always add genre_source
                existing = annotations.get(_id, {})
                new_vals = {col: v for col, v in vals.items() if col not in existing}
                # Add provenance even if genre already annotated
                if new_vals or 'genre_source' not in existing:
                    new_vals['genre_source'] = source_id
                if not new_vals:
                    continue
                written[_id] = new_vals

        if dry_run:
            print(f'Would write {len(written)} annotations:')
            from collections import Counter
            corpora = Counter(_id.lstrip('_').split('/')[0] for _id in written)
            for c, n in corpora.most_common():
                print(f'  {c}: {n}')
            return written

        # Write to annotations
        for _id, vals in written.items():
            if _id not in annotations:
                annotations[_id] = {}
            annotations[_id].update(vals)

        if written:
            self._save_annotations(annotations)
            print(f'Wrote {len(written)} annotations from {source_corpus_id}')
            from collections import Counter
            corpora = Counter(_id.lstrip('_').split('/')[0] for _id in written)
            for c, n in corpora.most_common():
                print(f'  {c}: {n}')
        else:
            print('No new annotations to write')

        return written

    def _save_annotations(self, data):
        """Save annotations to JSON file."""
        os.makedirs(os.path.dirname(self.path_annotations), exist_ok=True)
        with open(self.path_annotations, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def load_metadata(self, **kwargs):
        """Load from DB, merge annotation overrides from JSON."""
        df = super().load_metadata(**kwargs)
        if not self.is_curated or df is None or not len(df):
            return df

        annotations = self._load_annotations()
        if not annotations:
            return df

        # Ensure _id is accessible as a column
        has_id_col = '_id' in df.columns
        if not has_id_col and df.index.name == 'id':
            # _id might not be in columns if index was set
            pass

        # Apply direct overrides
        for _id, overrides in annotations.items():
            if has_id_col:
                mask = df['_id'] == _id
            else:
                continue
            if not mask.any():
                continue
            for col, val in overrides.items():
                if col not in df.columns:
                    df[col] = None
                # __none__ means explicitly set to null
                if val == '__none__':
                    df.loc[mask, col] = None
                else:
                    # Cast value to match column dtype (e.g. str '1619' → Int32)
                    if col in df.columns:
                        try:
                            val = df[col].dtype.type(val)
                        except (TypeError, ValueError):
                            pass
                    df.loc[mask, col] = val

        # Propagate annotations across match groups:
        # if any member of a match group is annotated, apply to the representative
        if has_id_col:
            try:
                import lltk
                # Build lookup: _id → annotations (including propagated)
                annotated_ids = set(annotations.keys())
                if annotated_ids:
                    # Get match groups for annotated texts
                    mg = lltk.db.match_conn.execute(
                        "SELECT _id, group_id FROM match_db.match_groups"
                    ).fetchdf()
                    if len(mg):
                        # Map group_id → list of annotated _ids in that group
                        id_to_group = dict(zip(mg['_id'], mg['group_id']))
                        group_to_annotated = {}
                        for _id, ann in annotations.items():
                            gid = id_to_group.get(_id)
                            if gid is not None:
                                group_to_annotated[gid] = ann

                        # For each row in df, if its group has an annotation but it doesn't, propagate
                        df_ids = set(df['_id'])
                        for _, row in mg.iterrows():
                            _id = row['_id']
                            gid = row['group_id']
                            if _id in df_ids and _id not in annotated_ids and gid in group_to_annotated:
                                # Propagate — direct annotations already applied above take priority
                                propagated = group_to_annotated[gid]
                                mask = df['_id'] == _id
                                for col, val in propagated.items():
                                    if col not in df.columns:
                                        df[col] = None
                                    if val == '__none__':
                                        df.loc[mask, col] = None
                                    else:
                                        df.loc[mask, col] = val
            except Exception:
                pass  # match DB not available, skip propagation

        return self._filter_excluded(df)

    def _filter_excluded(self, df):
        """Remove rows where 'exclude' is truthy."""
        if 'exclude' in df.columns:
            # Keep rows where exclude is empty, NaN, False, or ''
            mask = df['exclude'].isna() | (df['exclude'] == '') | (df['exclude'] == False)
            df = df[mask]
        return df

    def texts(self, progress=False, **kwargs):
        """Iterate text objects, respecting annotations (exclusions)."""
        if self.is_curated:
            from lltk.corpus.corpus import Corpus
            df = self.load_metadata()
            if '_id' not in df.columns:
                yield from super().texts(progress=progress, **kwargs)
                return
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
        else:
            yield from super().texts(progress=progress, **kwargs)
