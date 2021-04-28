from lltk.imports import *


class BigHist(MetaCorpus):
    CORPORA=[
        #'ARTFL',
        #'BPO',
        'CLMET',
        'COCA',
        'COHA',
        #'CanonFiction',
        'Chadwyck',
        'ChadwyckDrama',
        'ChadwyckPoetry',
        'Chicago',
        #'DTA',
        'DialNarr',
        'ECCO',
        'ECCO_TCP',
        'EEBO_TCP',
        #'ESTC',
        'EnglishDialogues',
        'EvansTCP',
        #'FanFic',
        'GaleAmericanFiction',
        'GildedAge',
        #'Hathi',
        'HathiAlmanacs',
        'HathiBio',
        'HathiEngLit',
        'HathiEssays',
        'HathiLetters',
        'HathiNovels',
        'HathiProclamations',
        'HathiSermons',
        'HathiStories',
        'HathiTales',
        'HathiTreatises',
        'InternetArchive',
        'LitLab',
        'MarkMark',
        #'NewYorker',
        'OldBailey',
        'PMLA',
        'RavenGarside',
        'SOTU',
        'Sellers',
        'SemanticCohort',
        'Spectator',
        'TedJDH',
        'TxtLab'
    ]
    name='BigHist'

    def __init__(self,**attrs):
        super().__init__(corpora=self.CORPORA)

