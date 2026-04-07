"""
Fiction bibliography corpus.

Metadata-only corpus from parsed scholarly bibliographies of early English
fiction (Mish 1475-1700, Odell 1475-1700, McBurney 1700-1739, etc.).

compile() reads Gemini-parsed CSVs from sources_parsed/, assigns IDs,
auto-matches to ESTC via STC/Wing IDs and shelfmarks, and writes metadata.csv.
"""
from lltk.imports import *
import json as _json


# McBurney library abbreviations → ESTC institution codes
SHELFMARK_LIBRARY_MAP = {
    'BM': 'bL',       # British Museum → British Library
    'BL': 'bL',       # British Library (Raven 2000+)
    'Bod': 'bO',      # Bodleian Library
    'H': 'nMH',       # Harvard University
    'Y': 'nCtY',      # Yale University
    'N': 'nICN',      # Newberry Library
    'Chi': 'nICU',    # University of Chicago
    'Col': 'nNNC',    # Columbia University
    'LC': 'nDLC',     # Library of Congress
    'NYPL': 'nNN',    # New York Public Library
    'BPL': 'nMB',     # Boston Public Library
    'Ill': 'nIU',     # University of Illinois
    'UP': 'nPU',      # University of Pennsylvania
    'Fon': 'nTxHR',   # Fondren Library, Rice
    'BN': 'ePBN',     # Bibliothèque Nationale
    'Br': 'nRPB',     # Brown University (John Carter Brown Library)
}


class TextFictionBiblio(BaseText):
    pass


class FictionBiblio(BaseCorpus):
    TEXT_CLASS = TextFictionBiblio
    LINKS = {'estc': ('id_estc', 'id_estc')}

    @property
    def path_sources_parsed(self):
        return os.path.join(self.path, 'sources_parsed')

    @property
    def path_matches_verified(self):
        return os.path.join(self.path, 'matches_verified.csv')

    @property
    def path_matches_to_verify(self):
        return os.path.join(self.path, 'matches_to_verify.csv')

    def compile(self):
        """
        Compile metadata.csv from parsed bibliography CSVs.

        Pipeline:
        1. Read Gemini-parsed CSVs from sources_parsed/
        2. Assign IDs ({biblio}_{NNNN}), clean STC/Wing values
        3. Auto-match to ESTC via STC/Wing IDs
        4. Auto-match to ESTC via shelfmarks (for McBurney etc.)
        5. Load manual match verifications if available
        6. Write metadata.csv
        """
        sources_dir = self.path_sources_parsed
        if not os.path.isdir(sources_dir):
            raise FileNotFoundError(f'No sources_parsed directory at {sources_dir}')

        csv_files = sorted(
            os.path.join(sources_dir, f)
            for f in os.listdir(sources_dir)
            if f.endswith('.csv')
        )
        if not csv_files:
            raise FileNotFoundError(f'No CSV files in {sources_dir}')

        # ── Step 1: Read and assign IDs ──────────────────────────────
        frames = []
        for csv_path in csv_files:
            biblio = os.path.splitext(os.path.basename(csv_path))[0]
            df = pd.read_csv(csv_path).fillna('')
            df['biblio'] = biblio
            # Assign sequential IDs
            df['id'] = [f'{biblio}_{str(i+1).zfill(4)}' for i in range(len(df))]
            if log: log(f'Read {len(df)} entries from {biblio}')
            frames.append(df)

        meta = pd.concat(frames, ignore_index=True)
        if log: log(f'Total: {len(meta)} entries from {len(csv_files)} sources')

        # Infer is_dubious from entry_num starting with X (McBurney's dubious titles)
        if 'entry_num' in meta.columns:
            meta['is_dubious'] = meta['entry_num'].astype(str).str.upper().str.startswith('X')

        # Drop LLM metadata columns
        for col in ('model', 'temperature', 'prompt', 'meta_source'):
            if col in meta.columns:
                meta = meta.drop(columns=[col])

        # Rename meta_page to biblio_page
        if 'meta_page' in meta.columns:
            meta['biblio_page'] = meta['biblio'] + '_' + meta['meta_page'].astype(str)
            meta = meta.drop(columns=['meta_page'])

        # ── Step 2: Clean STC/Wing values ────────────────────────────
        for col in ('id_stc', 'id_wing'):
            if col in meta.columns:
                meta[col] = (meta[col].astype(str).str.strip()
                             .str.replace('[Not in STC]', '', regex=False)
                             .str.replace('[No STC]', '', regex=False)
                             .str.replace('[Not in Wing]', '', regex=False)
                             .str.replace('[No Wing]', '', regex=False)
                             .str.strip())

        # Expand multi-valued STC/Wing IDs (comma-separated) into first value
        # Keep the first STC and first Wing for the primary match
        for col in ('id_stc', 'id_wing'):
            if col in meta.columns:
                meta[col] = meta[col].apply(lambda x: x.split(',')[0].strip() if isinstance(x, str) else '')

        # ── Step 2b: Normalize pre-existing ESTC IDs (e.g. Raven) ────
        if 'id_estc' not in meta.columns:
            meta['id_estc'] = ''
        if 'id_estc_all' not in meta.columns:
            meta['id_estc_all'] = ''

        def _normalize_estc_id(raw):
            """Normalize a single ESTC ID: strip prefix, uppercase, strip leading zeros."""
            s = str(raw).strip()
            s = re.sub(r'^ESTC\s*', '', s).strip()
            # Strip bracketed qualifiers like [vols. 1-4]
            s = re.sub(r'\s*\[.*?\]', '', s).strip()
            if not s:
                return ''
            # Uppercase prefix
            if s[0].islower():
                s = s[0].upper() + s[1:]
            # Strip leading zeros: T068056 → T68056
            s = re.sub(r'^([A-Z])0+', r'\1', s)
            # Validate: must be letter + digits
            if re.match(r'^[A-Z]\d+$', s):
                return s
            return ''

        def _parse_estc_ids(raw, raw_all=''):
            """Parse potentially multi-value ESTC ID fields into a list of normalized IDs."""
            ids = []
            # Combine id_estc and id_estc_all
            for field in [raw, raw_all]:
                s = str(field).strip() if pd.notna(field) else ''
                if not s:
                    continue
                # Split on commas or semicolons
                for part in re.split(r'[;,]', s):
                    norm = _normalize_estc_id(part)
                    if norm and norm not in ids:
                        ids.append(norm)
            return ids

        # Parse and normalize all ESTC IDs
        for idx in meta.index:
            raw = meta.at[idx, 'id_estc']
            raw_all = meta.at[idx, 'id_estc_all'] if 'id_estc_all' in meta.columns else ''
            ids = _parse_estc_ids(raw, raw_all)
            meta.at[idx, 'id_estc'] = ids[0] if ids else ''
            meta.at[idx, 'id_estc_all'] = '|'.join(ids) if len(ids) > 1 else ''

        if 'estc_match_source' not in meta.columns:
            meta['estc_match_source'] = ''
        # Mark pre-existing ESTC IDs
        has_estc = meta['id_estc'] != ''
        meta.loc[has_estc & (meta['estc_match_source'] == ''), 'estc_match_source'] = 'direct'
        if has_estc.sum():
            if log: log(f'Pre-existing ESTC IDs: {has_estc.sum()}')

        # ── Step 3: Auto-match to ESTC via STC/Wing ─────────────────
        estc_meta = self._load_estc_metadata()
        if estc_meta is not None:
            meta = self._match_stc_wing(meta, estc_meta)
        elif not has_estc.any():
            if log: log('ESTC metadata not available — skipping STC/Wing matching')

        # ── Step 4: Auto-match to ESTC via shelfmarks ────────────────
        if estc_meta is not None and 'references' in meta.columns:
            meta = self._match_shelfmarks(meta, estc_meta)

        # ── Step 4b: Cross-link via McBurney references ──────────────
        meta = self._match_mcburney_crossrefs(meta)

        # ── Step 5: Load manual match verifications ──────────────────
        if os.path.exists(self.path_matches_verified):
            meta = self._apply_verified_matches(meta)

        # ── Step 6: Write matches_to_verify.csv for new matches ──────
        if estc_meta is not None:
            self._write_matches_to_verify(meta, estc_meta)

        # ── Step 7: Write metadata.csv ───────────────────────────────
        # Store original ID before any ESTC-linked dedup
        if 'id_orig' not in meta.columns:
            meta['id_orig'] = meta['id']

        meta = meta.set_index('id')

        # Report match stats
        has_estc = (meta['id_estc'].replace('', pd.NA).notna()).sum()
        if log: log(f'ESTC matched: {has_estc}/{len(meta)} ({has_estc/len(meta)*100:.1f}%)')

        out_path = os.path.join(self.path, 'metadata.csv')
        meta.to_csv(out_path)
        if log: log(f'Wrote {out_path}')

        # Clear parquet cache
        parquet_path = os.path.join(self.path, 'metadata.parquet')
        if os.path.exists(parquet_path):
            os.remove(parquet_path)

        return meta

    def _load_estc_metadata(self):
        """Load ESTC metadata.csv for matching."""
        try:
            from lltk.corpus.utils import load
            estc = load('estc')
            df = estc.load_metadata()
            if df is not None and len(df):
                return df.reset_index().fillna('')
        except Exception as e:
            if log: log(f'Could not load ESTC: {e}')
        return None

    def _match_stc_wing(self, meta, estc_meta):
        """Match bibliography entries to ESTC via STC/Wing IDs."""
        # Build ESTC lookups
        estc_by_stc = {}
        estc_by_wing = {}
        for _, row in estc_meta.iterrows():
            stc = str(row.get('id_stc', '')).strip()
            wing = str(row.get('id_wing', '')).strip()
            eid = str(row.get('id_estc', row.get('id', ''))).strip()
            if stc and eid:
                estc_by_stc[stc] = eid
            if wing and eid:
                estc_by_wing[wing] = eid

        if log: log(f'ESTC lookup: {len(estc_by_stc)} STC, {len(estc_by_wing)} Wing entries')

        # Match (skip entries that already have an ESTC ID)
        matched_stc = 0
        matched_wing = 0
        for idx in meta.index:
            if meta.at[idx, 'id_estc']:
                continue  # already has ESTC ID (e.g. from Raven)
            stc = str(meta.at[idx, 'id_stc'] if 'id_stc' in meta.columns else '').strip()
            wing = str(meta.at[idx, 'id_wing'] if 'id_wing' in meta.columns else '').strip()
            if stc and stc in estc_by_stc:
                meta.at[idx, 'id_estc'] = estc_by_stc[stc]
                meta.at[idx, 'estc_match_source'] = 'stc'
                matched_stc += 1
            elif wing and wing in estc_by_wing:
                meta.at[idx, 'id_estc'] = estc_by_wing[wing]
                meta.at[idx, 'estc_match_source'] = 'wing'
                matched_wing += 1
        if log: log(f'STC/Wing matching: {matched_stc} STC, {matched_wing} Wing, {matched_stc+matched_wing} total')
        return meta

    def _match_shelfmarks(self, meta, estc_meta):
        """Match bibliography entries to ESTC via library shelfmarks.

        Parses the 'references' field (e.g. 'BM 12511.c.16; Bod 270.f.49'),
        maps library abbreviations to ESTC institution codes, and looks up
        shelfmarks in ESTC's holdings JSON column.
        """
        if 'holdings' not in estc_meta.columns:
            if log: log('ESTC has no holdings column — run lltk compile estc first')
            return meta

        # Build shelfmark index: (institution_code, shelfmark) → estc_id
        shelfmark_index = {}
        n_holdings = 0
        for _, row in estc_meta[estc_meta['holdings'].replace('', pd.NA).notna()].iterrows():
            try:
                holdings = _json.loads(row['holdings'])
            except (TypeError, ValueError):
                continue
            eid = str(row.get('id_estc', row.get('id', ''))).strip()
            for h in holdings:
                code = h.get('code', '')
                sm = h.get('shelfmark', '')
                if code and sm and eid:
                    shelfmark_index[(code, sm)] = eid
                    n_holdings += 1

        if not shelfmark_index:
            if log: log('No shelfmark index built — skipping shelfmark matching')
            return meta

        if log: log(f'Shelfmark index: {n_holdings} entries from {len(estc_meta[estc_meta.holdings.replace("", pd.NA).notna()])} ESTC records')

        # Sort library map by abbreviation length (longest first) to avoid
        # 'N' matching before 'NYPL'
        sorted_libs = sorted(SHELFMARK_LIBRARY_MAP.items(), key=lambda x: len(x[0]), reverse=True)

        # Match entries that don't already have an ESTC ID
        matched = 0
        for idx in meta.index:
            if meta.at[idx, 'id_estc']:
                continue  # already matched via STC/Wing
            refs = str(meta.at[idx, 'references']).strip()
            if not refs:
                continue
            for ref_part in refs.split(';'):
                ref_part = ref_part.strip()
                if not ref_part:
                    continue
                for abbr, estc_code in sorted_libs:
                    if ref_part.startswith(abbr + ' '):
                        shelfmark = ref_part[len(abbr)+1:].strip()
                        # Strip trailing parentheticals like "(missing)"
                        shelfmark = re.sub(r'\s*\(.*?\)\s*$', '', shelfmark).strip()
                        eid = shelfmark_index.get((estc_code, shelfmark))
                        if eid:
                            meta.at[idx, 'id_estc'] = eid
                            meta.at[idx, 'estc_match_source'] = f'shelfmark:{abbr}'
                            matched += 1
                            break
                        # Try with sublocation variants (e.g. nMH vs nMH-H)
                        for full_code in shelfmark_index:
                            if full_code[0].startswith(estc_code) and full_code[1] == shelfmark:
                                meta.at[idx, 'id_estc'] = shelfmark_index[full_code]
                                meta.at[idx, 'estc_match_source'] = f'shelfmark:{abbr}'
                                matched += 1
                                break
                        break
                if meta.at[idx, 'id_estc']:
                    break  # found a match, stop trying other refs

        if log: log(f'Shelfmark matching: {matched} new matches')
        return meta

    def _match_mcburney_crossrefs(self, meta):
        """Inherit ESTC IDs from McBurney entries via cross-references.

        Beasley entries have id_mcburney (e.g. '129') pointing to McBurney entry numbers.
        If the corresponding McBurney entry has an ESTC ID, inherit it.
        """
        if 'id_mcburney' not in meta.columns:
            return meta

        # Build lookup: McBurney entry_num → ESTC ID (from mcburney entries in this DataFrame)
        mcb_lookup = {}
        mcb_entries = meta[(meta['biblio'] == 'mcburney1960') & (meta['id_estc'].replace('', pd.NA).notna())]
        if 'entry_num' in meta.columns:
            for _, row in mcb_entries.iterrows():
                enum = str(row.get('entry_num', '')).strip()
                eid = str(row['id_estc']).strip()
                if enum and eid:
                    mcb_lookup[enum] = eid

        if not mcb_lookup:
            return meta

        matched = 0
        for idx in meta.index:
            if meta.at[idx, 'id_estc']:
                continue
            mcb_ref = str(meta.at[idx, 'id_mcburney']).strip()
            if mcb_ref and mcb_ref in mcb_lookup:
                meta.at[idx, 'id_estc'] = mcb_lookup[mcb_ref]
                meta.at[idx, 'estc_match_source'] = f'mcburney_xref:{mcb_ref}'
                matched += 1

        if log: log(f'McBurney cross-ref matching: {matched} new matches (from {len(mcb_lookup)} McBurney entries with ESTC IDs)')
        return meta

    def _apply_verified_matches(self, meta):
        """Apply manually verified ESTC matches from matches_verified.csv.

        Format: CSV with columns 'id' and 'id_estc' (and optionally 'is_match').
        Rows with is_match='n' clear the ESTC match; rows with is_match='y'
        (or no is_match column) set it.
        """
        verified = pd.read_csv(self.path_matches_verified).fillna('')
        if 'id' not in verified.columns or 'id_estc' not in verified.columns:
            if log: log(f'matches_verified.csv missing id/id_estc columns')
            return meta

        applied = 0
        cleared = 0
        for _, row in verified.iterrows():
            bid = str(row['id']).strip()
            eid = str(row['id_estc']).strip()
            is_match = str(row.get('is_match', 'y')).strip().lower()

            mask = meta['id'] == bid
            if not mask.any():
                continue

            if is_match == 'n':
                meta.loc[mask, 'id_estc'] = ''
                meta.loc[mask, 'estc_match_source'] = ''
                cleared += 1
            elif eid:
                meta.loc[mask, 'id_estc'] = eid
                if not meta.loc[mask, 'estc_match_source'].values[0]:
                    meta.loc[mask, 'estc_match_source'] = 'verified'
                applied += 1

        if log: log(f'Manual verifications: {applied} applied, {cleared} cleared')
        return meta

    def _write_matches_to_verify(self, meta, estc_meta):
        """Write matches_to_verify.csv with fuzzy scores for human review.

        Includes all auto-matched entries (STC/Wing + shelfmark) that are NOT
        already in matches_verified.csv. Shows both sides of the match plus
        a fuzzy similarity score so you can quickly spot bad matches.

        Columns: id, biblio, biblio_page, id_estc, estc_match_source,
                 biblio_key, estc_key, fuzzy_score, is_match (blank for you to fill)
        """
        matched = meta[meta['id_estc'].replace('', pd.NA).notna()].copy()
        if not len(matched):
            return

        # Skip entries already verified
        already_verified = set()
        if os.path.exists(self.path_matches_verified):
            verified = pd.read_csv(self.path_matches_verified).fillna('')
            already_verified = set(zip(verified['id'].astype(str), verified['id_estc'].astype(str)))

        # Build ESTC lookup for title/author
        estc_lookup = {}
        for _, row in estc_meta.iterrows():
            eid = str(row.get('id_estc', row.get('id', ''))).strip()
            if eid:
                title = str(row.get('title', '')).strip()
                title_sub = str(row.get('title_sub', '')).strip()
                author = str(row.get('author', '')).strip()
                estc_lookup[eid] = {
                    'title': title,
                    'title_sub': title_sub,
                    'author': author,
                }

        # Fuzzy matching
        has_fuzz = False
        fuzz = None
        try:
            from rapidfuzz import fuzz
            has_fuzz = True
        except ImportError:
            try:
                from fuzzywuzzy import fuzz
                has_fuzz = True
            except ImportError:
                pass

        def _clean(s):
            """Remove punctuation and lowercase for fuzzy comparison."""
            import string
            return ''.join(c for c in str(s) if c not in string.punctuation).lower().strip()

        rows = []
        for _, row in matched.iterrows():
            bid = str(row['id'])
            eid = str(row['id_estc'])
            if (bid, eid) in already_verified:
                continue

            # Build comparison keys
            biblio_key = f"{row.get('author', '')} {row.get('title', '')}".strip()
            estc_info = estc_lookup.get(eid, {})
            estc_key = f"{estc_info.get('author', '')} {estc_info.get('title', '')} {estc_info.get('title_sub', '')}".strip()

            fuzzy_score = None
            if has_fuzz and biblio_key and estc_key:
                fuzzy_score = fuzz.partial_token_set_ratio(_clean(biblio_key), _clean(estc_key))

            rows.append({
                'id': bid,
                'biblio': row.get('biblio', ''),
                'biblio_page': row.get('biblio_page', ''),
                'id_stc': row.get('id_stc', ''),
                'id_wing': row.get('id_wing', ''),
                'id_estc': eid,
                'estc_match_source': row.get('estc_match_source', ''),
                'biblio_key': biblio_key,
                'estc_key': estc_key,
                'fuzzy_score': fuzzy_score,
                'is_match': '',
            })

        if rows:
            verify_df = pd.DataFrame(rows).sort_values('fuzzy_score')
            verify_df.to_csv(self.path_matches_to_verify, index=False)
            if log: log(f'Wrote {len(verify_df)} matches to verify → {self.path_matches_to_verify}')
        else:
            if log: log('No new matches to verify')

    def load_metadata(self, *x, **y):
        df = super().load_metadata(*x, **y)
        if not len(df):
            return df
        meta = self.merge_linked_metadata(df)
        for c in ['author', 'title', 'year']:
            c2 = 'estc_'+c
            if c2 in meta.columns:
                meta[c+'_biblio'] = meta[c]
                meta[c] = meta[c2].where(meta[c2].notna() & (meta[c2] != ''), meta[c])
        meta['genre'] = 'Fiction'

        # Enrich genre_raw: bibliography categories first, ESTC fills gaps
        # Raven 1987 category codes: N=Novel, E=Epistolary, M=Miscellaneous, C=Collection
        if 'category' in meta.columns:
            cat = meta['category'].fillna('')
            is_epistolary = cat.str.startswith('E')
            is_novel = cat.str.startswith('N')
            meta.loc[is_epistolary, 'genre_raw'] = 'Novel, epistolary'
            meta.loc[is_novel, 'genre_raw'] = 'Novel'

        # Raven 2000: extract epistolary from notes, default to Novel
        if 'notes' in meta.columns:
            is_raven2000 = meta['biblio'] == 'raven2000' if 'biblio' in meta.columns else pd.Series(False, index=meta.index)
            needs_genre = is_raven2000 & (meta['genre_raw'].isna() | (meta['genre_raw'] == ''))
            notes = meta['notes'].fillna('')
            meta.loc[needs_genre & notes.str.contains(r'[Ee]pistolary', regex=True), 'genre_raw'] = 'Novel, epistolary'
            needs_genre = is_raven2000 & (meta['genre_raw'].isna() | (meta['genre_raw'] == ''))
            meta.loc[needs_genre, 'genre_raw'] = 'Novel'

        # Fall back to ESTC genre_raw only where bibliography has nothing,
        # and only accept fiction-relevant values (ESTC often misclassifies
        # novels as Biography, Letter, etc. based on title conventions)
        FICTION_RAW = {
            'Fiction', 'Novel', 'Novel, epistolary', 'Romance', 'Tale', 'Fable',
            'Novella', 'Picaresque', 'Gothic', 'Imaginary voyage',
            'Novel, sentimental', 'Novel, Gothic', 'Novel, satire',
            'Novel, historical', 'Novel, didactic', 'Novel, oriental',
            'Epistolary fiction', 'Satire', 'Dialogue', 'Allegory',
        }
        if 'estc_genre_raw' in meta.columns:
            needs_genre = meta['genre_raw'].isna() | (meta['genre_raw'] == '')
            estc_raw = meta['estc_genre_raw']
            is_fiction_raw = estc_raw.isin(FICTION_RAW)
            meta.loc[needs_genre & is_fiction_raw, 'genre_raw'] = estc_raw

        return meta
