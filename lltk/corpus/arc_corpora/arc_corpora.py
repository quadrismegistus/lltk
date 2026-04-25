"""
Synthetic corpora for the abstraction project's arc analysis.

Three genre arcs: Fiction, Poetry, Periodical.
Each pulls from multiple source corpora, deduplicated by oldest edition.
Text objects retain their source corpus for file access and corpus adjustment.
"""

from lltk.corpus.synthetic import CuratedCorpus


class ArcFiction(CuratedCorpus):
    ID = 'arc_fiction'
    NAME = 'ArcFiction'
    SOURCES = {
        'bpo': {'genre': 'Fiction', 'year_max': 2009},
        'chadwyck': {'year_max': 2009},
        'chicago': {'year_max': 2009},
        'clmet': {'genre': 'Fiction', 'year_max': 2009},
        'gale_amfic': {'year_max': 2009},
        'gildedage': {'year_max': 2009},
        'hathi_englit': {'genre': 'Fiction', 'year_max': 2009},
        'internet_archive': {'year_max': 2009},
        'litlab': {'genre': 'Fiction', 'year_max': 2009},
        'long_arc_prestige': {'genre': 'Fiction', 'year_max': 2009},
        'markmark': {'year_max': 2009},
        'ecco': {'genre': 'Fiction', 'year_max': 2009},
        'ecco_tcp': {'genre': 'Fiction', 'year_max': 2009},
        'eebo_tcp': {'genre': 'Fiction', 'year_max': 2009},
        'earlyprint': {'genre': 'Fiction', 'year_max': 2009},
        'evans_tcp': {'genre': 'Fiction', 'year_max': 2009},
        'coha': {'genre': 'Fiction', 'year_max': 2009},
        # 'dialnarr': {'genre': 'Fiction'},
        # 'sellers': {'genre': 'Fiction'},
        'tedjdh': {'genre': 'Fiction', 'year_max': 2009},
        'blbooks': {'genre': 'Fiction', 'year_max': 2009},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

    # Conservative mode: pre-CONSERVATIVE_BEFORE, only include texts whose
    # genre was confirmed by an approved authority (bibliography or ESTC form).
    # Set to None to disable.
    CONSERVATIVE_BEFORE = 1801
    CONSERVATIVE_SOURCES = {
        'form',
        'bibliography:fiction_biblio',
        'bibliography:end',
        'bibliography:ravengarside',
    }
    # Corpora whose self-assignment is trusted pre-CONSERVATIVE_BEFORE
    CONSERVATIVE_CORPORA = {'chadwyck'}

    _UNSET = object()

    def load_metadata(self, conservative=_UNSET, **kwargs):
        meta = super().load_metadata(**kwargs)
        if meta is None or not len(meta):
            return meta
        if conservative is self._UNSET:
            conservative = self.CONSERVATIVE_BEFORE
        if conservative and 'genre_enriched_source' in meta.columns:
            pre = meta.year < conservative
            approved = meta.genre_enriched_source.isin(self.CONSERVATIVE_SOURCES)
            trusted_corpus = meta.corpus.isin(self.CONSERVATIVE_CORPORA) if self.CONSERVATIVE_CORPORA else False
            meta = meta[~pre | approved | trusted_corpus]
        return meta


class ArcFictionFr(CuratedCorpus):
    ID = 'arc_fiction_fr'
    NAME = 'ArcFictionFr'
    SOURCES = {
        # Temporarily excluded: Fiction label derives from title-keyword
        # heuristics (roman/poème/etc.) rather than an authority. Re-enable
        # once we have bibliography- or LLM-backed genre classification.
        # 'french_pd_books': {'genre': 'Fiction', 'lang': 'fr', 'year_max': 1950},
        'artfl': {'genre': 'Fiction', 'lang': 'fr', 'year_max': 1950},
        'gallica_literary_fictions': {'genre': 'Fiction', 'lang': 'fr', 'year_max': 1950},
        'txtlab': {'genre': 'Fiction', 'lang': 'fr', 'year_max': 1950},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

    def load_metadata(self, **kwargs):
        meta = super().load_metadata(**kwargs)
        if meta is None or not len(meta):
            return meta
        if 'is_translated' in meta.columns:
            meta = meta[meta.is_translated.fillna(False) == False]
        return meta


class ArcFictionDe(CuratedCorpus):
    ID = 'arc_fiction_de'
    NAME = 'ArcFictionDe'
    # internet_archive is excluded: its corpus-provided `lang='de'` flag is
    # wrong for its fiction subset — all 17 Fiction+lang=de candidates are
    # confidently English per db-detect-langs (16 'en' at conf >5, 1
    # 'unknown'). ecco + gale_amfic kept — their lang='de' Fiction is
    # genuine German content (18th-c German imprints / 19th-c German-
    # American fiction) and matches lang_detected.
    SOURCES = {
        'german_fiction':   {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        # Temporarily excluded: Fiction label derives from title-keyword
        # heuristics (roman/novelle/etc.) rather than an authority. Re-enable
        # once we have bibliography- or LLM-backed genre classification.
        # 'german_pd':        {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        'de_corp':          {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        'dta':              {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        'txtlab':           {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        'ecco':             {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
        'gale_amfic':       {'genre': 'Fiction', 'lang': 'de', 'year_max': 1950},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

    def load_metadata(self, **kwargs):
        meta = super().load_metadata(**kwargs)
        if meta is None or not len(meta):
            return meta
        if 'is_translated' in meta.columns:
            meta = meta[meta.is_translated.fillna(False) == False]
        return meta


class ArcPoetry(CuratedCorpus):
    ID = 'arc_poetry'
    NAME = 'ArcPoetry'
    SOURCES = {
        'chadwyck_poetry': {},
        'hathi_englit': {'genre': 'Poetry'},
        'eebo_tcp': {'genre': 'Poetry'},
        'ecco': {'genre': 'Poetry'},
        'ecco_tcp': {'genre': 'Poetry'},
        'evans_tcp': {'genre': 'Poetry'},
        'eebo_tcp': {'genre': 'Poetry'},
        'bpo': {'genre': 'Poetry'},
        'long_arc_prestige': {'genre': 'Poetry'},
        'sellers': {'genre': 'Poetry'},
        'tedjdh': {'genre': 'Poetry'},
        'blbooks': {'genre': 'Poetry'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'


class ArcPeriodical(CuratedCorpus):
    ID = 'arc_periodical'
    NAME = 'ArcPeriodical'
    SOURCES = {
        'bpo': {'genre': 'Periodical'},
        'coha': {'genre': 'Periodical'},
        'coca': {'genre': 'Periodical'},
        'ecco': {'genre': 'Periodical'},
        'eebo_tcp': {'genre': 'Periodical'},
        'evans_tcp': {'genre': 'Periodical'},
        'ecco_tcp': {'genre': 'Periodical'},
        'new_yorker': {},
        'spectator': {},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

class ArcEssays(CuratedCorpus):
    ID = 'arc_essays'
    NAME = 'ArcEssays'
    SOURCES = {
        'hathi_essays': {},
        'ecco': {'genre': 'Essay'},
        'eebo_tcp': {'genre': 'Essay'},
        'ecco_tcp': {'genre': 'Essay'},
        'evans_tcp': {'genre': 'Essay'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

class ArcSermons(CuratedCorpus):
    ID = 'arc_sermons'
    NAME = 'ArcSermons'
    SOURCES = {
        'hathi_sermons': {},
        'ecco': {'genre': 'Sermon'},
        'eebo_tcp': {'genre': 'Sermon'},
        'ecco_tcp': {'genre': 'Sermon'},
        'evans_tcp': {'genre': 'Sermon'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'

class ArcBiography(CuratedCorpus):
    ID = 'arc_biography'
    NAME = 'ArcBiography'
    SOURCES = {
        'hathi_bio': {'genre': 'Biography'},
        'ecco': {'genre': 'Biography'},
        'eebo_tcp': {'genre': 'Biography'},
        'ecco_tcp': {'genre': 'Biography'},
        'evans_tcp': {'genre': 'Biography'},
        'tedjdh': {'genre': 'Biography'},
        'sellers': {'genre': 'Biography'},
        'clmet': {'genre': 'Biography'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'