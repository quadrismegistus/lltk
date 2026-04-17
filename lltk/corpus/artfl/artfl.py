from lltk.imports import *


# Manual overrides for keyword false positives
ARTFL_TITLE_OVERRIDES = {
    'Lettres sur les romans': 'Criticism',
    'Réflexions critiques sur la poésie et sur la peinture': 'Criticism',
}

ARTFL_GENRE_MAP = {
    'Fiction': ('Fiction', None),
    'Non-Fiction': ('Nonfiction', None),
    'Drama': ('Drama', None),
    'Poetry': ('Poetry', None),
    'Memoir': ('Biography', 'Memoir'),
    'Letter': ('Letters', None),
    'Travel': ('Nonfiction', 'Travel'),
}


class TextARTFL(BaseText):
    pass


class ARTFL(BaseCorpus):
    TEXT_CLASS = TextARTFL

    def load_metadata(self):
        meta = super().load_metadata()
        if not len(meta):
            return meta

        # Parse year: handles 'n.d.' and other non-numeric
        if 'year' in meta.columns:
            meta['year'] = pd.to_numeric(meta['year'], errors='coerce')

        # Map genre to GENRE_VOCAB
        if 'genre' in meta.columns:
            meta['genre_raw'] = meta['genre']
            mapped = meta['genre'].map(ARTFL_GENRE_MAP)
            meta['genre'] = mapped.apply(lambda x: x[0] if isinstance(x, tuple) else None)
            meta['genre_raw'] = mapped.apply(
                lambda x: x[1] if isinstance(x, tuple) and x[1] else None
            ).fillna(meta['genre_raw'])

        # Apply manual title overrides
        if 'title' in meta.columns:
            for title, genre in ARTFL_TITLE_OVERRIDES.items():
                mask = meta['title'].fillna('').str.strip() == title
                meta.loc[mask, 'genre'] = genre

        # Keyword fallback for ungenred texts (precision over recall)
        if 'genre' in meta.columns:
            import re as _re
            KEYWORDS = {
                'Fiction': {'roman', 'romans'},
                'Poetry': {'poème', 'poèmes', 'poésie', 'poésies'},
                'Drama': {'comédie', 'tragédie', 'opéra'},
            }
            mask = meta['genre'].isna() | (meta['genre'] == '')
            def _kw_classify(title):
                if not title or str(title) == 'nan':
                    return None
                words = set(_re.findall(r"[a-zàâäéèêëîïôöùûüç']+", str(title).lower()))
                for genre, kws in KEYWORDS.items():
                    if words & kws:
                        return genre
                return None
            meta.loc[mask, 'genre'] = meta.loc[mask, 'title'].apply(_kw_classify)

        meta['lang'] = 'fr'
        return meta
