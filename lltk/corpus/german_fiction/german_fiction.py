from lltk.imports import *


FIGSHARE_ARTICLE_ID = '4524680'
FIGSHARE_API_URL = f'https://api.figshare.com/v2/articles/{FIGSHARE_ARTICLE_ID}/files'


class TextGermanFiction(BaseText):
    pass


class GermanFiction(BaseCorpus):
    TEXT_CLASS = TextGermanFiction

    def compile(self, force=False):
        """Download German-Language Fiction corpus from Figshare and build metadata + txt files.

        3,219 texts (2,735 German originals + 484 translations) from Gutenberg-DE.
        Filenames encode author, title, and year:
            Author_Name_-_Title_(1837).txt
        Translated texts (no year in filename) are in a separate subfolder.
        """
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            if log: log('Already compiled. Use force=True to recompile.')
            return

        import zipfile
        import tempfile
        import shutil
        import requests

        raw_dir = os.path.join(self.path_root, 'raw')
        os.makedirs(raw_dir, exist_ok=True)
        zip_path = os.path.join(raw_dir, 'german_fiction.zip')

        if not os.path.exists(zip_path) or os.path.getsize(zip_path) == 0 or force:
            if log: log(f'Fetching download URL from Figshare API...')
            files = requests.get(FIGSHARE_API_URL, timeout=30).json()
            download_url = files[0]['download_url']
            if log: log(f'Downloading {files[0]["name"]} ({files[0]["size"] / 1e6:.0f} MB)...')
            r = requests.get(download_url, stream=True, timeout=300, allow_redirects=True)
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)
            if log: log(f'Downloaded {os.path.getsize(zip_path) / 1e6:.0f} MB')

        tmp_dir = tempfile.mkdtemp(prefix='german_fiction_')
        if log: log('Extracting zip...')
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)

        TRANSLATED_DIR = 'corpus-of-translated-foreign-language-fiction-txt'
        ORIGINAL_DIR = 'corpus-of-german-fiction-txt'

        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []

        for dp, _, files in os.walk(tmp_dir):
            for fn in sorted(files):
                if not fn.endswith('.txt'):
                    continue
                src_path = os.path.join(dp, fn)
                is_translated = TRANSLATED_DIR in dp

                author, title, year = _parse_filename(fn)
                year_dir = str(year) if year else 'unknown'
                fid = re.sub(r'\.txt$', '', fn)
                fid = re.sub(r'[^a-zA-Z0-9_\-.]', '_', fid)

                txt_dir = os.path.join(self.path_txt, year_dir)
                os.makedirs(txt_dir, exist_ok=True)
                dst_path = os.path.join(txt_dir, f'{fid}.txt')
                shutil.copy2(src_path, dst_path)

                meta_rows.append({
                    'id': f'{year_dir}/{fid}',
                    'title': title,
                    'author': author,
                    'year': year,
                    'genre': 'Fiction',
                    'is_translated': is_translated,
                    'lang': 'deu',
                    'filename_orig': fn,
                })

        df = pd.DataFrame(meta_rows)
        df.to_csv(self.path_metadata, index=False)
        if log: log(f'Saved {len(df)} records to {self.path_metadata}')

        shutil.rmtree(tmp_dir, ignore_errors=True)

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        if 'genre' not in meta.columns:
            meta['genre'] = 'Fiction'
        if 'genre_raw' not in meta.columns:
            meta['genre_raw'] = None
        return meta


def _parse_filename(fn):
    """Parse 'Author_Name_-_Title_(1837).txt' into (author, title, year).

    Translated texts have no year: 'Author_Name_-_Title.txt'.
    Returns (author_str, title_str, year_int_or_empty).
    """
    name = re.sub(r'\.txt$', '', fn)

    year = None
    m = re.search(r'\((\d{4})\)\s*$', name)
    if m:
        year = int(m.group(1))
        name = name[:m.start()].strip().rstrip('_').strip()

    parts = name.split('_-_', 1)
    if len(parts) == 2:
        author_raw = parts[0].replace('_', ' ').strip()
        title = parts[1].replace('_', ' ').strip()
    else:
        author_raw = ''
        title = name.replace('_', ' ').strip()

    # Convert "First Middle Last" → "Last, First Middle"
    if author_raw and ',' not in author_raw:
        words = author_raw.split()
        if len(words) >= 2:
            author_raw = f'{words[-1]}, {" ".join(words[:-1])}'

    return author_raw, title, year or ''
