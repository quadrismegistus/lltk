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
        'bpo': {'genre': 'Fiction'},
        'chadwyck': {},
        'canon_fiction': {'genre': 'Fiction'},
        'chicago': {},
        'clmet': {'genre': 'Fiction'},
        'gale_amfic': {},
        'gildedage': {},
        'hathi_englit': {'genre': 'Fiction'},
        'internet_archive': {},
        'litlab': {'genre': 'Fiction'},
        'long_arc_prestige': {'genre': 'Fiction'},
        'markmark': {},
        'ecco': {'genre': 'Fiction'},
        'ecco_tcp': {'genre': 'Fiction'},
        'eebo_tcp': {'genre': 'Fiction'},
        'earlyprint': {'genre': 'Fiction'},
        'evans_tcp': {'genre': 'Fiction'},
        'coha': {'genre': 'Fiction'},
        'clmet': {'genre': 'Fiction'},
        # 'dialnarr': {'genre': 'Fiction'},
        # 'sellers': {'genre': 'Fiction'},
        'tedjdh': {'genre': 'Fiction'},
        'blbooks': {'genre': 'Fiction'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'


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