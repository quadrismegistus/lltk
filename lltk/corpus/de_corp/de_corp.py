import os
import re

import pandas as pd
from lltk.imports import BaseCorpus, BaseText, log


ZENODO_API_URL = 'https://zenodo.org/api/records/15714828'

# Map non-fiction German genre labels → lltk GENRE_VOCAB.
# Fiction is always 'Fiction'; genre_raw preserves the original German label.
NONFIC_GENRE_MAP = {
    'Geschichte': 'History',
    'Philosophie': 'Treatise',
    'Religion': 'Nonfiction',
    'Reiseberichte, Reiseerzählungen': 'Nonfiction',
    'Natur': 'Nonfiction',
    'Praktisches': 'Nonfiction',
}


class TextDeCorp(BaseText):
    pass


class DeCorp(BaseCorpus):
    TEXT_CLASS = TextDeCorp

    def compile(self, force=False):
        """Download de-Corp (German fiction + non-fiction, 1780–1940) from Zenodo.

        Zenodo record 15714828: ~5,000 texts from Project Gutenberg (DE + US).
        Two CSVs (fiction, non-fiction) + one zip of .txt files keyed by
        `filename` column. Output layout: txt/{year}/{safe_filename}.txt,
        metadata.csv with id, title, author, year, genre, genre_raw,
        author_gender, n_tokens, n_sents, source, work_type, lang.
        """
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            log('Already compiled. Use force=True to recompile.')
            return

        import zipfile, tempfile, shutil, requests

        raw_dir = os.path.join(self.path_root, 'raw')
        os.makedirs(raw_dir, exist_ok=True)

        log(f'Fetching Zenodo record listing from {ZENODO_API_URL}...')
        record = requests.get(ZENODO_API_URL, timeout=30).json()
        file_map = {f['key']: f['links']['self'] for f in record.get('files', [])
                    if f.get('key') and f.get('links', {}).get('self')}

        expected = ['de-corp_fiction.csv', 'de-corp_non_fiction.csv', 'de-corp_txt.zip']
        for key in expected:
            if key not in file_map:
                raise RuntimeError(f'Missing file on Zenodo record: {key}')
            out_path = os.path.join(raw_dir, key)
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0 and not force:
                continue
            log(f'Downloading {key}...')
            r = requests.get(file_map[key], stream=True, timeout=600, allow_redirects=True)
            r.raise_for_status()
            with open(out_path, 'wb') as fh:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        fh.write(chunk)

        # Extract zip into temp dir (flat or nested — we scan for filenames)
        zip_path = os.path.join(raw_dir, 'de-corp_txt.zip')
        tmp_dir = tempfile.mkdtemp(prefix='de_corp_')
        log('Extracting zip...')
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)

        # Index extracted .txt files by basename for filename-based lookup
        txt_index = {}
        for dp, _, files in os.walk(tmp_dir):
            for fn in files:
                if fn.endswith('.txt'):
                    txt_index.setdefault(fn, os.path.join(dp, fn))

        os.makedirs(self.path_txt, exist_ok=True)
        rows = []

        for work_type, csv_key in (('fiction', 'de-corp_fiction.csv'),
                                   ('nonfiction', 'de-corp_non_fiction.csv')):
            df = pd.read_csv(os.path.join(raw_dir, csv_key))
            df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

            # Unify gender column name across the two CSVs
            if 'author_gender' not in df.columns and 'gender' in df.columns:
                df = df.rename(columns={'gender': 'author_gender'})

            for _, r in df.iterrows():
                fn = str(r.get('filename') or '').strip()
                if not fn:
                    continue
                src = txt_index.get(fn)
                if not src:
                    log(f'Missing txt for {fn}')
                    continue

                year = r.get('year')
                try:
                    year_int = int(year) if pd.notna(year) else None
                except (TypeError, ValueError):
                    year_int = None
                year_dir = str(year_int) if year_int else 'unknown'

                fid_base = re.sub(r'\.txt$', '', fn)
                fid_safe = re.sub(r'[^a-zA-Z0-9_\-.]', '_', fid_base)
                fid = f'{year_dir}/{fid_safe}'

                dst_dir = os.path.join(self.path_txt, year_dir)
                os.makedirs(dst_dir, exist_ok=True)
                dst = os.path.join(dst_dir, f'{fid_safe}.txt')
                if not os.path.exists(dst):
                    shutil.copy2(src, dst)

                genre_raw = str(r.get('genre') or '').strip() or None
                if work_type == 'fiction':
                    genre = 'Fiction'
                else:
                    genre = NONFIC_GENRE_MAP.get(genre_raw, 'Nonfiction')

                author_last = str(r.get('author_last') or '').strip()
                author_first = str(r.get('author_first') or '').strip()
                if author_last and author_first:
                    author = f'{author_last}, {author_first}'
                elif author_last:
                    author = author_last
                else:
                    author = author_first

                rows.append({
                    'id': fid,
                    'title': str(r.get('title') or '').strip(),
                    'author': author,
                    'author_last': author_last,
                    'author_first': author_first,
                    'year': year_int,
                    'genre': genre,
                    'genre_raw': genre_raw,
                    'author_gender': str(r.get('author_gender') or '').strip() or None,
                    'n_sents': r.get('num_sents'),
                    'n_tokens': r.get('num_tokens'),
                    'source': str(r.get('source') or '').strip() or None,
                    'work_type': work_type,
                    'lang': 'de',
                    'filename_orig': fn,
                })

        out = pd.DataFrame(rows)
        out.to_csv(self.path_metadata, index=False)
        log(f'Saved {len(out)} records to {self.path_metadata}')

        shutil.rmtree(tmp_dir, ignore_errors=True)

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        if 'genre' not in meta.columns:
            meta['genre'] = None
        if 'genre_raw' not in meta.columns:
            meta['genre_raw'] = None
        if 'lang' not in meta.columns:
            meta['lang'] = 'de'
        return meta
