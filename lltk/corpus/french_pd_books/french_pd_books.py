from lltk.imports import *


class TextFrenchPDBooks(BaseText):
    pass


class FrenchPDBooks(BaseCorpus):
    TEXT_CLASS = TextFrenchPDBooks
    HF_DATASET = 'PleIAs/French-PD-Books'

    def compile(self, force=False):
        """Download French-PD-Books from HuggingFace and build metadata + txt files."""
        if not force and os.path.exists(self.path_metadata) and os.path.isdir(self.path_txt):
            if log: log('Already compiled. Use force=True to recompile.')
            return

        try:
            from datasets import load_dataset
        except ImportError:
            raise ImportError(
                'The datasets library is required. '
                'Install it with: pip install "datasets<4"'
            )

        if log: log(f'Streaming {self.HF_DATASET} from HuggingFace...')
        ds = load_dataset(self.HF_DATASET, split='train', streaming=True)

        os.makedirs(self.path_txt, exist_ok=True)
        meta_rows = []

        for row in get_tqdm(ds, desc='[FrenchPDBooks] Writing texts', total=261116):
            fid = str(row.get('file_id', '')).strip()
            text = row.get('complete_text', '')
            if not fid or not text or not text.strip():
                continue

            year = _parse_year(row.get('date', ''))
            year_dir = str(year) if year else 'unknown'
            txt_dir = os.path.join(self.path_txt, year_dir)
            os.makedirs(txt_dir, exist_ok=True)
            txt_path = os.path.join(txt_dir, f'{fid}.txt')
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(text)

            meta_rows.append({
                'id': f'{year_dir}/{fid}',
                'file_id': fid,
                'title': row.get('title', ''),
                'author': row.get('author', ''),
                'year': year,
                'date_raw': str(row.get('date', '')),
                'page_count': row.get('page_count', ''),
                'word_count': row.get('word_count', ''),
                'character_count': row.get('character_count', ''),
                'ocr': row.get('ocr', ''),
                'lang': 'fre',
            })

        df = pd.DataFrame(meta_rows)
        df.to_csv(self.path_metadata, index=False)
        if log: log(f'Saved {len(df)} records to {self.path_metadata}')

    # Conservative genre keywords — precision over recall.
    # Excludes: histoire (often fiction), mémoires (ambiguous), conte (compte rendu),
    # vers/discours (too broad), lettre/lettres (common in fiction), chronique (journalism).
    FRENCH_GENRE_KEYWORDS = {
        'Fiction': {
            'roman': 'Roman', 'romans': 'Roman',
        },
        'Poetry': {
            'poème': 'Poème', 'poèmes': 'Poèmes',
            'poésie': 'Poésie', 'poésies': 'Poésies',
        },
        'Drama': {
            'comédie': 'Comédie', 'tragédie': 'Tragédie',
            'opéra': 'Opéra',
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
            words = set(_re.findall(r"[a-zàâäéèêëîïôöùûüç']+", str(title).lower()))
            for genre, kw_map in self.FRENCH_GENRE_KEYWORDS.items():
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
