# -*- coding: utf-8 -*-
from __future__ import absolute_import

from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
import os

class TextBPO(Text):
    pass

class BPO(Corpus):
    TEXT_CLASS=TextBPO
    PATH_TXT = 'bpo/_txt_bpo'
    PATH_XML = 'bpo/_xml_bpo'
    PATH_METADATA = 'bpo/corpus-metadata.BPO.txt'

    def __init__(self,**attrs):
        super(BPO,self).__init__(**attrs)
        self.year_start=1680
        self.year_end=1900
