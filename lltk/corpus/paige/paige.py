from lltk.imports import *


ZENODO_API_URL = 'https://zenodo.org/api/records/3939066'

CSV_SOURCES = {
    'en': {'filename': 'Data England web post.csv', 'lang': 'eng'},
    'fr': {'filename': 'Data France web post.csv', 'lang': 'fre'},
    'ep': {'filename': 'Epistolary data web post.csv', 'lang': 'fre'},
}


class TextPaige(BaseText):
    pass


class Paige(BaseCorpus):
    TEXT_CLASS = TextPaige

    def compile(self, force=False):
        """Download Paige referential-mode datasets from Zenodo and build merged metadata."""
        if not force and os.path.exists(self.path_metadata):
            if log: log('Already compiled. Use force=True to recompile.')
            return

        import requests

        raw_dir = os.path.join(self.path_root, 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        if log: log(f'Fetching Zenodo record listing from {ZENODO_API_URL}...')
        record = requests.get(ZENODO_API_URL).json()
        for f in record.get('files', []):
            key = f.get('key')
            url = f.get('links', {}).get('self')
            if not key or not url:
                continue
            out_path = os.path.join(raw_dir, key)
            if os.path.exists(out_path) and not force:
                continue
            if log: log(f'Downloading {key}...')
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(out_path, 'wb') as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)

        merged = []
        for short, info in CSV_SOURCES.items():
            path = os.path.join(raw_dir, info['filename'])
            if not os.path.exists(path):
                if log: log(f'Missing CSV: {path}')
                continue
            df = pd.read_csv(path, encoding='mac_roman')
            df.columns = [str(c).strip() for c in df.columns]
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
            # Normalize year/author/title column names
            rename = {}
            for c in df.columns:
                lc = c.lower().strip()
                if lc == 'year':
                    rename[c] = 'year'
                elif lc == 'author':
                    rename[c] = 'author'
                elif lc == 'title':
                    rename[c] = 'title'
                elif lc == 'decade':
                    rename[c] = 'decade'
            df = df.rename(columns=rename)

            df = df.reset_index(drop=True)
            df['id'] = [f'{short}{i+1:04d}' for i in range(len(df))]
            df['source_csv'] = short
            df['lang'] = info['lang']
            df['genre'] = 'Fiction'
            df['genre_raw'] = 'Epistolary novel' if short == 'ep' else 'Novel'

            for col in ('year', 'author', 'title'):
                if col not in df.columns:
                    df[col] = ''
            merged.append(df)

        out = pd.concat(merged, ignore_index=True, sort=False)
        # Put canonical cols first
        front = ['id', 'title', 'author', 'year', 'genre', 'genre_raw', 'lang', 'source_csv']
        cols = front + [c for c in out.columns if c not in front]
        out = out[cols]
        out.to_csv(self.path_metadata, index=False)
        if log: log(f'Saved {len(out)} records to {self.path_metadata}')

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        return meta
