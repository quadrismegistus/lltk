"""
Synthetic corpora for the abstraction project's arc analysis.

Three genre arcs: Fiction, Poetry, Periodical.
Each pulls from multiple source corpora, deduplicated by oldest edition.
Text objects retain their source corpus for file access and corpus adjustment.
"""

from lltk.corpus.synthetic import SyntheticCorpus


class ArcFiction(SyntheticCorpus):
    ID = 'arc_fiction'
    NAME = 'ArcFiction'
    SOURCES = {
        'chadwyck': {'genre': 'Fiction'},
        'chicago': {'genre': 'Fiction'},
        'gale_amfic': {'genre': 'Fiction'},
        'gildedage': {},
        'hathi_englit': {'genre': 'Fiction'},
        'internet_archive': {'genre': 'Fiction'},
        'litlab': {'genre': 'Fiction'},
        'long_arc_prestige': {'genre': 'Fiction'},
        'markmark': {'genre': 'Fiction'},
        'ecco': {'genre': 'Fiction'},
        'eebo_tcp': {'genre': 'Fiction'},
        'coha': {'genre': 'Fiction'},
        'clmet': {'genre': 'Fiction'},
        'dialnarr': {'genre': 'Fiction'},
        'sellers': {'genre': 'Fiction'},
        'tedjdh': {'genre': 'Fiction'},
        'blbooks': {'genre': 'Fiction'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'


class ArcPoetry(SyntheticCorpus):
    ID = 'arc_poetry'
    NAME = 'ArcPoetry'
    SOURCES = {
        'chadwyck_poetry': {},
        'hathi_englit': {'genre': 'Poetry'},
        'eebo_tcp': {'genre': 'Poetry'},
        'ecco': {'genre': 'Poetry'},
        'long_arc_prestige': {'genre': 'Poetry'},
        'sellers': {'genre': 'Poetry'},
        'tedjdh': {'genre': 'Poetry'},
        'blbooks': {'genre': 'Poetry'},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'


class ArcPeriodical(SyntheticCorpus):
    ID = 'arc_periodical'
    NAME = 'ArcPeriodical'
    SOURCES = {
        'bpo': {'genre': 'Periodical'},
        'coha': {'genre': 'Periodical'},
        'new_yorker': {},
        'spectator': {},
    }
    DEDUP = True
    DEDUP_BY = 'oldest'
