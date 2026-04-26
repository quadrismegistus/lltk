import os
import re

import pandas as pd
from lltk.imports import BaseCorpus, BaseText, get_tqdm, log


ZENODO_API_URL = 'https://zenodo.org/api/records/4751204'


class TextGallicaLiteraryFictions(BaseText):
    pass


class GallicaLiteraryFictions(BaseCorpus):
    TEXT_CLASS = TextGallicaLiteraryFictions

    def compile(self, force=False):
        """Download Gallica Literary Fictions from Zenodo and build metadata + txt files.

        Source: https://zenodo.org/record/4751204
        372 zip files (one per publication year 1600-1996, skipping gaps),
        each containing a TSV of page-level rows. We group by main_id and
        concatenate pages into one txt per book.
        """
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            log('Already compiled. Use force=True to recompile.')
            return

        import csv
        import time
        import zipfile
        import tempfile
        import requests
        from collections import defaultdict

        log(f'Fetching Zenodo record listing from {ZENODO_API_URL}...')
        record = requests.get(ZENODO_API_URL).json()
        urls = sorted(
            f['links']['self']
            for f in record['files']
            if (f.get('key') or '').lower().endswith('.zip')
        )
        log(f'{len(urls)} year-zip files to download.')

        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []
        tmp_root = tempfile.mkdtemp(prefix='gallica_lf_')

        for url in get_tqdm(urls, desc='[GallicaLiteraryFictions] Years'):
            try:
                r = requests.get(url, stream=True, timeout=120)
                r.raise_for_status()
            except Exception as e:
                log(f'Failed {url}: {e}')
                time.sleep(2)
                continue

            zip_path = os.path.join(tmp_root, os.path.basename(url))
            with open(zip_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1 << 20):
                    if chunk:
                        f.write(chunk)

            extract_dir = os.path.join(tmp_root, os.path.basename(url) + '_x')
            os.makedirs(extract_dir, exist_ok=True)
            try:
                with zipfile.ZipFile(zip_path) as zf:
                    zf.extractall(extract_dir)
            except zipfile.BadZipFile:
                log(f'Bad zip: {url}')
                os.remove(zip_path)
                time.sleep(1)
                continue
            os.remove(zip_path)

            for dp, _, files in os.walk(extract_dir):
                for fn in files:
                    if not fn.lower().endswith('.tsv'):
                        continue
                    tsv_path = os.path.join(dp, fn)
                    books = defaultdict(lambda: {'pages': [], 'meta': {}})
                    with open(tsv_path, encoding='utf-8', newline='') as tsv_file:
                        reader = csv.DictReader(tsv_file, delimiter='\t')
                        for row in reader:
                            mid = (row.get('main_id') or '').strip()
                            if not mid:
                                continue
                            ark = re.sub(r'_p\d+$', '', mid)
                            cat_id = (row.get('catalogue_id') or '').strip()
                            book_id = cat_id or ark
                            try:
                                pg = int(row.get('page') or 0)
                            except (TypeError, ValueError):
                                pg = 0
                            txt = row.get('texte', '') or ''
                            if txt.strip() == 'NA':
                                txt = ''
                            # Replace "_p_" paragraph markers with paragraph breaks
                            txt = txt.replace(' _p_ ', '\n\n').replace('_p_ ', '').replace(' _p_', '')
                            # Sort by (ark, page) so multiple digital copies
                            # sharing a catalogue_id stay contiguous.
                            books[book_id]['pages'].append((ark, pg, txt))
                            if not books[book_id]['meta']:
                                books[book_id]['meta'] = {
                                    'main_id': ark,
                                    'catalogue_id': cat_id,
                                    'title': row.get('titre', ''),
                                    'author_last': row.get('nom_auteur', ''),
                                    'author_first': row.get('prenom_auteur', ''),
                                    'year': _parse_year(row.get('date', '')),
                                    'document_ocr': row.get('document_ocr', ''),
                                    'date_enligne': row.get('date_enligne', ''),
                                    'gallica': row.get('gallica', ''),
                                }

                    for book_id, data in books.items():
                        pages = sorted(data['pages'], key=lambda x: (x[0], x[1]))
                        full_text = '\n\n\n'.join(t for _, _, t in pages if t)
                        if not full_text.strip():
                            continue
                        meta = data['meta']
                        year = meta.get('year', '')
                        year_dir = str(year) if year and str(year).isdigit() else 'unknown'
                        txt_dir = os.path.join(self.path_txt, year_dir)
                        os.makedirs(txt_dir, exist_ok=True)
                        txt_path = os.path.join(txt_dir, f'{book_id}.txt')
                        with open(txt_path, 'w', encoding='utf-8') as f:
                            f.write(full_text)

                        meta['id'] = f'{year_dir}/{book_id}'
                        meta['num_pages'] = len(pages)
                        meta['author'] = _join_author(
                            meta.get('author_first', ''),
                            meta.get('author_last', ''),
                        )
                        meta_rows.append(meta)

            import shutil
            shutil.rmtree(extract_dir, ignore_errors=True)
            time.sleep(1)  # avoid 429

        df = pd.DataFrame(meta_rows)
        df.to_csv(self.path_metadata, index=False)
        log(f'Saved {len(df)} records to {self.path_metadata}')

        import shutil
        shutil.rmtree(tmp_root, ignore_errors=True)

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')
        # Entire corpus is literary fiction (Y2/Ybis classification)
        meta['genre'] = 'Fiction'
        meta['genre_raw'] = 'Novel'
        meta['lang'] = 'fre'
        return meta


def _parse_year(date_str):
    if date_str is None:
        return ''
    if isinstance(date_str, float) and date_str != date_str:
        return ''
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else ''


def _join_author(first, last):
    first = (first or '').strip()
    last = (last or '').strip()
    if last and first:
        return f'{last}, {first}'
    return last or first or ''
