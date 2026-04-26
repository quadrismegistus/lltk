import os
import re

import pandas as pd
from lltk.imports import BaseCorpus, BaseText, get_tqdm, log


class TextGermanPD(BaseText):
    pass


class GermanPD(BaseCorpus):
    TEXT_CLASS = TextGermanPD
    HF_DATASET = 'PleIAs/German-PD'

    def compile(self, force=False):
        """Download German-PD from HuggingFace and build metadata + txt files.

        ~385K books, 187GB on HuggingFace. Streams row-by-row so no full
        download is needed upfront. Each row is one book with full text.

        Fields: identifier, creator, title, publication_date, word_count, text
        """
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            if log: log('Already compiled. Use force=True to recompile.')
            return

        if log: log(f'Streaming {self.HF_DATASET} from HuggingFace (~385K books, 187GB)...')
        # Read parquet files directly via HfFileSystem — the datasets library
        # fails on schema differences across shards.
        from huggingface_hub import HfFileSystem
        import pyarrow.parquet as pq

        fs = HfFileSystem()
        parquet_files = sorted(fs.glob(f'datasets/{self.HF_DATASET}/**/*.parquet'))
        if log: log(f'{len(parquet_files)} parquet shards found.')

        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []
        pbar = get_tqdm(total=292000, desc='[GermanPD] Writing texts')

        for pf in parquet_files:
            try:
                with fs.open(pf, 'rb') as f:
                    table = pq.read_table(f)
            except Exception as e:
                if log: log(f'Skipping {pf}: {e}')
                continue
            for row in table.to_pylist():
                pbar.update(1)
                ident = str(row.get('identifier', '')).strip()
                text = row.get('text', '')
                if not ident or not text or not text.strip():
                    continue

                # Use last path component of identifier URL as file ID
                fid = ident.rstrip('/').rsplit('/', 1)[-1] if '/' in ident else ident
                # Sanitize for filesystem
                fid = re.sub(r'[^a-zA-Z0-9_\-.]', '_', fid)
                if not fid:
                    continue

                pub_date = row.get('publication_date', '')
                # publication_date can be string or int across different shards
                year = int(pub_date) if isinstance(pub_date, (int, float)) and pub_date == pub_date else _parse_year(pub_date)
                # Fallback: extract year from identifier URL
                if not year:
                    year = _parse_year(row.get('identifier', ''))
                year_dir = str(year) if year else 'unknown'
                txt_dir = os.path.join(self.path_txt, year_dir)
                os.makedirs(txt_dir, exist_ok=True)
                txt_path = os.path.join(txt_dir, f'{fid}.txt')
                if os.path.exists(txt_path) and not force:
                    pass
                else:
                    with open(txt_path, 'w', encoding='utf-8') as f:
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
                    'lang': 'deu',
                })

        pbar.close()
        df = pd.DataFrame(meta_rows)
        df = df.drop_duplicates(subset='id', keep='first')
        df.to_csv(self.path_metadata, index=False)
        if log: log(f'Saved {len(df)} records to {self.path_metadata}')

    # Conservative genre keywords — precision over recall.
    # Excludes: erzählung (ambiguous), märchen (fairy tale compilations),
    # lied/dichtung (too broad), brief/briefe (common in fiction),
    # geschichte (often fiction), drama (figurative use).
    GERMAN_GENRE_KEYWORDS = {
        'Fiction': {
            'roman': 'Roman', 'romane': 'Roman',
            'novelle': 'Novelle', 'novellen': 'Novellen',
        },
        'Poetry': {
            'gedicht': 'Gedicht', 'gedichte': 'Gedichte',
            'lyrik': 'Lyrik',
        },
        'Drama': {
            'komödie': 'Komödie', 'tragödie': 'Tragödie',
            'trauerspiel': 'Trauerspiel', 'lustspiel': 'Lustspiel',
            'schauspiel': 'Schauspiel',
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
            words = set(_re.findall(r"[a-zäöüß']+", str(title).lower()))
            for genre, kw_map in self.GERMAN_GENRE_KEYWORDS.items():
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
    """Extract author name from creator field, stripping GND/Wikipedia links.

    Returns 'Last, First' format for normalize_author compatibility.
    Input is 'First Middle Last\\n  \\n    (GND, Wikipedia, ADB/NDB)'.
    """
    if not creator or str(creator) == 'nan':
        return ''
    # Take only the first line (name), discard GND/Wikipedia labels
    name = str(creator).split('\n')[0].strip()
    # Remove URLs
    name = re.sub(r'https?://\S+', '', name)
    # Remove parenthetical dates/info
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'\s+', ' ', name).strip(' ,;|')
    if not name:
        return ''
    # Already in "Last, First" format — leave as-is
    if ',' in name:
        return name
    # Convert "First Middle Last" → "Last, First Middle"
    parts = name.split()
    if len(parts) >= 2:
        return f'{parts[-1]}, {" ".join(parts[:-1])}'
    return name
