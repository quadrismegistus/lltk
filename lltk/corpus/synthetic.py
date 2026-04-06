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

    # Source hierarchy for flattening annotations (earlier = higher priority).
    # Sources not listed here are ranked after all listed sources.
    SOURCE_HIERARCHY = ['human']

    @property
    def path_annotations(self):
        return os.path.join(os.path.expanduser(PATH_CORPUS), self.id, 'annotations.json')

    @property
    def is_curated(self):
        return os.path.exists(self.path_annotations)

    def _load_annotations_raw(self):
        """Load raw annotations list from JSON file.

        annotations.json is a list of dicts, each with '_id' and 'genre_source':
            [{"_id": "...", "genre_source": "human", "genre": "Fiction", ...}, ...]

        Legacy format (dict of dicts keyed by _id) is auto-migrated on load.
        """
        if not os.path.exists(self.path_annotations):
            return []
        with open(self.path_annotations) as f:
            data = json.load(f)
        if isinstance(data, dict):
            # Migrate legacy format: dict-of-dicts → list-of-dicts
            entries = []
            for _id, vals in data.items():
                entry = {'_id': _id}
                entry['genre_source'] = vals.pop('genre_source', 'human')
                entry.update(vals)
                entries.append(entry)
            return entries
        return data

    def _load_annotations(self):
        """Load annotations and flatten by source hierarchy → dict of dicts.

        Returns the same {_id: {col: val, ...}} shape that all consumers expect.
        Higher-priority sources win per-column when multiple entries exist for one _id.
        """
        entries = self._load_annotations_raw()
        return self._flatten_annotations(entries)

    def _flatten_annotations(self, entries):
        """Flatten a list of annotation entries into {_id: {col: val}} by source hierarchy.

        For each _id, iterates entries from highest to lowest priority source.
        Each column is set by the first (highest priority) entry that provides it.
        The winning genre_source for the 'genre' column is preserved.
        """
        hierarchy = list(self.SOURCE_HIERARCHY)

        def _source_rank(source):
            try:
                return hierarchy.index(source)
            except ValueError:
                return len(hierarchy)  # unlisted sources rank last

        # Group entries by _id, sorted by source priority
        from collections import defaultdict
        by_id = defaultdict(list)
        for entry in entries:
            _id = entry.get('_id')
            if _id:
                by_id[_id].append(entry)

        result = {}
        for _id, id_entries in by_id.items():
            id_entries.sort(key=lambda e: _source_rank(e.get('genre_source', 'human')))
            merged = {}
            winning_source = None
            for entry in id_entries:
                source = entry.get('genre_source', 'human')
                for col, val in entry.items():
                    if col in ('_id', 'genre_source'):
                        continue
                    if col not in merged:
                        merged[col] = val
                        if col == 'genre' and winning_source is None:
                            winning_source = source
            if winning_source:
                merged['genre_source'] = winning_source
            result[_id] = merged
        return result

    def annotate(self, port=8989):
        """Launch the annotation web app for this corpus."""
        from lltk.web.annotate import run_annotate
        run_annotate(self.id, port=port)

    def propagate_from(self, source, columns=None, dry_run=False,
                       source_label=None):
        """Add annotation entries from a corpus, DataFrame, or list of dicts.

        Source can be:
            - str: corpus ID to query from the DB (e.g. 'fiction_biblio')
            - DataFrame: must have '_id' column (or as index) plus columns to write
            - list[dict]: each dict must have '_id' plus columns to write

        Entries are stored with their genre_source for provenance. Spreading to
        match group siblings happens at read time in load_metadata(), not here.

        Re-running is safe: existing entries from the same source are replaced.

        Args:
            source: corpus ID (str), DataFrame, or list of dicts
            columns: list of columns to write (default: ['genre'] for corpus,
                     all non-_id columns for DataFrame/list)
            dry_run: if True, print what would be written without saving
            source_label: provenance label for genre_source (default: corpus ID or 'external')
        Returns:
            list of annotation dicts that were written (or would be)
        """
        import lltk

        entries = self._load_annotations_raw()

        # Resolve source to a DataFrame with '_id' column
        if isinstance(source, str):
            source_corpus_id = source
            if source_label is None:
                source_label = source_corpus_id
            if columns is None:
                columns = ['genre']
            source_df = lltk.db.query(f"""
                SELECT _id, {', '.join(columns)}
                FROM texts WHERE corpus = '{source_corpus_id}'
            """)
            if not len(source_df):
                print(f'No texts found for corpus {source_corpus_id}')
                return []
        else:
            if source_label is None:
                source_label = 'external'
            if isinstance(source, list):
                source_df = pd.DataFrame(source)
            else:
                source_df = source.copy()
            if '_id' not in source_df.columns and source_df.index.name == '_id':
                source_df = source_df.reset_index()
            if '_id' not in source_df.columns:
                raise ValueError("Source must have '_id' column or '_id' as index")
            if columns is None:
                columns = [c for c in source_df.columns if c not in ('_id', 'genre_source')]

        # Build new entries
        new_entries = []
        for _, row in source_df.iterrows():
            _id = row['_id']
            vals = {col: row[col] for col in columns
                    if col in row.index and pd.notna(row[col])}
            if vals:
                label = row.get('genre_source', source_label) if 'genre_source' in row.index else source_label
                entry = {'_id': _id, 'genre_source': label}
                entry.update(vals)
                new_entries.append(entry)

        if dry_run:
            print(f'Would write {len(new_entries)} annotation entries:')
            from collections import Counter
            corpora = Counter(e['_id'].lstrip('_').split('/')[0] for e in new_entries)
            for c, n in corpora.most_common():
                print(f'  {c}: {n}')
            return new_entries

        # Remove old entries from the same source for _ids we're updating
        new_ids = {e['_id'] for e in new_entries}
        new_source_labels = {e.get('genre_source', source_label) for e in new_entries}
        entries = [
            e for e in entries
            if not (e.get('_id') in new_ids and e.get('genre_source', 'human') in new_source_labels)
        ]
        entries.extend(new_entries)

        if new_entries:
            self._save_annotations(entries)
            print(f'Wrote {len(new_entries)} annotation entries from {source_label}')
            from collections import Counter
            corpora = Counter(e['_id'].lstrip('_').split('/')[0] for e in new_entries)
            for c, n in corpora.most_common():
                print(f'  {c}: {n}')
        else:
            print('No new annotations to write')

        return new_entries

    def _save_annotations(self, data):
        """Save annotations list to JSON file."""
        os.makedirs(os.path.dirname(self.path_annotations), exist_ok=True)
        with open(self.path_annotations, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def load_metadata(self, **kwargs):
        """Load from DB, merge annotation overrides from JSON.
        Annotated _ids act as a whitelist: texts in annotations.json are
        included even if they don't match SOURCES genre filters."""
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

        # Whitelist: fetch annotated _ids not already in DataFrame
        if has_id_col:
            existing_ids = set(df['_id'])
            missing_ids = [
                _id for _id in annotations
                if _id not in existing_ids
                and not annotations[_id].get('exclude')
            ]
            if missing_ids:
                try:
                    import lltk
                    placeholders = ', '.join(f"'{_id}'" for _id in missing_ids)
                    extra = lltk.db.query(f"SELECT * FROM texts WHERE _id IN ({placeholders})")
                    if extra is not None and len(extra):
                        # Drop meta JSON column for consistency
                        if 'meta' in extra.columns:
                            extra = extra.drop(columns=['meta'])
                        # Align columns and index
                        if df.index.name == 'id' and 'id' in extra.columns:
                            extra = extra.set_index('id')
                        df = pd.concat([df, extra], ignore_index=False)
                except Exception:
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
