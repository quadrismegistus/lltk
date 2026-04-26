import os
import re

import pandas as pd
from lltk.imports import BaseCorpus, BaseText, get_tqdm, log


class TextSpanishPDBooks(BaseText):
    pass


class SpanishPDBooks(BaseCorpus):
    TEXT_CLASS = TextSpanishPDBooks
    HF_DATASET = 'PleIAs/Spanish-PD-Books'

    def compile(self, force=False):
        """Download Spanish-PD-Books from HuggingFace and build metadata + txt files.

        ~302K books. Reads parquet files directly via HfFileSystem — the datasets
        library fails on schema differences across shards (publication_date int/str).

        Fields: identifier, creator, title, publication_date, word_count, text,
                language, lang (original lang label), real_lang (detected lang)
        """
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            log('Already compiled. Use force=True to recompile.')
            return

        from huggingface_hub import HfFileSystem
        import pyarrow.parquet as pq

        fs = HfFileSystem()
        parquet_files = sorted(fs.glob(f'datasets/{self.HF_DATASET}/**/*.parquet'))
        log(f'{len(parquet_files)} parquet shards found.')

        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []
        pbar = get_tqdm(total=302640, desc='[SpanishPDBooks] Writing texts')

        for pf in parquet_files:
            try:
                with fs.open(pf, 'rb') as f:
                    table = pq.read_table(f)
            except Exception as e:
                log(f'Skipping {pf}: {e}')
                continue
            for row in table.to_pylist():
                pbar.update(1)
                ident = str(row.get('identifier', '')).strip()
                text = row.get('text', '')
                if not ident or not text or not text.strip():
                    continue

                fid = ident.rstrip('/').rsplit('/', 1)[-1] if '/' in ident else ident
                fid = re.sub(r'[^a-zA-Z0-9_\-.]', '_', fid)
                if not fid:
                    continue

                pub_date = row.get('publication_date', '')
                year = int(pub_date) if isinstance(pub_date, (int, float)) and pub_date == pub_date else _parse_year(pub_date)
                if not year:
                    year = _parse_year(row.get('identifier', ''))
                year_dir = str(year) if year else 'unknown'
                txt_dir = os.path.join(self.path_txt, year_dir)
                os.makedirs(txt_dir, exist_ok=True)
                txt_path = os.path.join(txt_dir, f'{fid}.txt.gz')
                if not os.path.exists(txt_path) or force:
                    import gzip as _gzip
                    with _gzip.open(txt_path, 'wt', encoding='utf-8') as f:
                        f.write(text)

                creator = row.get('creator', '') or ''
                author = _clean_creator(creator)

                meta_rows.append({
                    'id': f'{year_dir}/{fid}',
                    'identifier': ident,
                    'title': row.get('title', ''),
                    'author': author,
                    'creator_raw': creator,
                    'year': year,
                    'date_raw': str(row.get('publication_date', '')),
                    'word_count': row.get('word_count', ''),
                    'lang': row.get('real_lang', '') or '',
                    'lang_orig': row.get('lang', '') or '',
                })

        pbar.close()
        df = pd.DataFrame(meta_rows)
        df = df.drop_duplicates(subset='id', keep='first')
        df.to_csv(self.path_metadata, index=False)
        log(f'Saved {len(df)} records to {self.path_metadata}')

    # Conservative genre keywords — precision over recall.
    # Excludes: historia (ambiguous), cuentos (too broad), memorias/crónica (memoir/journalism),
    # verso/versos (common non-Poetry uses), carta/cartas (letters appear in fiction too).
    SPANISH_GENRE_KEYWORDS = {
        'Fiction': {
            'novela': 'Novela', 'novelas': 'Novela',
        },
        'Poetry': {
            'poema': 'Poema', 'poemas': 'Poema',
            'poesía': 'Poesía', 'poesías': 'Poesía',
            'poesias': 'Poesía',
        },
        'Drama': {
            'comedia': 'Comedia', 'comedias': 'Comedia',
            'tragedia': 'Tragedia', 'tragedias': 'Tragedia',
            'ópera': 'Ópera', 'opera': 'Ópera',
            'drama': 'Drama', 'dramas': 'Drama',
        },
    }

    def load_metadata(self):
        import re as _re
        meta = super().load_metadata()
        if not len(meta):
            return meta
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')

        def _classify_title(title):
            if not title or str(title) == 'nan':
                return None, None
            words = set(_re.findall(r"[a-záéíóúüñ']+", str(title).lower()))
            for genre, kw_map in self.SPANISH_GENRE_KEYWORDS.items():
                for kw, raw in kw_map.items():
                    if kw in words:
                        return genre, raw
            return None, None

        classified = meta['title'].apply(_classify_title)
        meta['genre'] = classified.apply(lambda x: x[0])
        meta['genre_raw'] = classified.apply(lambda x: x[1])
        return meta


def _parse_year(date_str):
    if not date_str or (isinstance(date_str, float) and date_str != date_str):
        return ''
    m = re.search(r'(\d{4})', str(date_str))
    return int(m.group(1)) if m else ''


def _clean_creator(creator):
    """Extract author name, stripping GND/Wikipedia links if present."""
    if not creator or str(creator) == 'nan':
        return ''
    name = str(creator).split('\n')[0].strip()
    name = re.sub(r'https?://\S+', '', name)
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'\s+', ' ', name).strip(' ,;|')
    if not name:
        return ''
    if ',' in name:
        return name
    parts = name.split()
    if len(parts) >= 2:
        return f'{parts[-1]}, {" ".join(parts[:-1])}'
    return name
