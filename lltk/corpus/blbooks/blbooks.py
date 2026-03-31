from lltk.imports import *


class TextBLBooks(BaseText):
    pass


class BLBooks(BaseCorpus):
    TEXT_CLASS = TextBLBooks

    def compile(self, force=False):
        """Download BL Books from HuggingFace and build metadata + txt files."""
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            if log: log('Already compiled. Use force=True to recompile.')
            return

        try:
            from datasets import load_dataset
        except ImportError:
            raise ImportError(
                'The datasets library is required to download BL Books. '
                'Install it with: pip install "datasets<4"'
            )

        if log: log('Downloading BL Books from HuggingFace (this may take a while)...')
        ds = load_dataset(
            'TheBritishLibrary/blbooks',
            split='train',
            trust_remote_code=True,
        )

        # Filter: English only, non-empty pages
        if log: log(f'Loaded {len(ds)} pages. Filtering...')
        ds = ds.filter(
            lambda row: row.get('Language_1') == 'English' and not row.get('empty_pg', True)
        )
        if log: log(f'{len(ds)} English non-empty pages after filtering.')

        # Group pages by record_id
        if log: log('Grouping pages by record...')
        from collections import defaultdict
        records = defaultdict(lambda: {'pages': [], 'meta': {}})
        for row in get_tqdm(ds, desc='[BLBooks] Grouping pages'):
            rid = str(row['record_id'])
            records[rid]['pages'].append((row.get('pg', 0), row.get('text', '')))
            if not records[rid]['meta']:
                records[rid]['meta'] = {
                    'record_id': rid,
                    'title': row.get('Title', ''),
                    'author': _join_authors(
                        row.get('All_names', ''),
                        row.get('Publisher', ''),
                    ),
                    'year': _parse_year(row.get('Date of publication', '')),
                    'date_raw': str(row.get('Date of publication', '')),
                    'publisher': row.get('Publisher', ''),
                    'place': row.get('Place of publication', ''),
                    'language': row.get('Language_1', ''),
                    'all_names': row.get('All_names', ''),
                    'blrecord_id': rid,
                    'physical_description': row.get('Physical description', ''),
                    'edition': row.get('Edition', ''),
                    'issuance_type': row.get('Type of issuance', ''),
                    'shelfmarks': row.get('Shelfmarks', ''),
                }

        # Write txt files and collect metadata
        if log: log(f'Writing {len(records)} texts...')
        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []
        for rid, data in get_tqdm(records.items(), desc='[BLBooks] Writing texts'):
            # Sort pages by page number, concatenate
            pages = sorted(data['pages'], key=lambda x: x[0])
            full_text = '\n\n'.join(text for _, text in pages if text)
            if not full_text.strip():
                continue

            # Write text file grouped by year
            meta = data['meta']
            year = meta.get('year', '')
            year_dir = str(year) if year and str(year).isdigit() else 'unknown'
            txt_dir = os.path.join(self.path_txt, year_dir)
            os.makedirs(txt_dir, exist_ok=True)
            txt_path = os.path.join(txt_dir, f'{rid}.txt')
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(full_text)

            # Collect metadata
            meta['id'] = f'{year_dir}/{rid}'
            meta['num_pages'] = len(pages)
            meta_rows.append(meta)

        # Write metadata
        df = pd.DataFrame(meta_rows)
        df.to_csv(self.path_metadata, index=False)
        if log: log(f'Saved {len(df)} records to {self.path_metadata}')

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        return meta


def _parse_year(date_str):
    """Extract a 4-digit year from a date string."""
    if not date_str or (isinstance(date_str, float) and date_str != date_str):
        return ''
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else ''


def _join_authors(all_names, publisher):
    """Extract author from All_names, falling back to publisher."""
    if all_names and str(all_names).strip():
        return str(all_names).strip()
    if publisher and str(publisher).strip():
        return str(publisher).strip()
    return ''
