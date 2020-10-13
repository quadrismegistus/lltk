from __future__ import absolute_import
# -*- coding: utf-8 -*-

import codecs

from lltk.text import Text

class TextLitLab(Text):

	def text_plain_from_xml(self):
		doc=[]
		for sentence in self.dom('sentence'):
			sent=[]
			for token in sentence('word'):
				tok=token.text
				tok=tok.replace('-LSB-','[').replace('-RSB-',']').replace('-LRB-','(').replace('-RRB-',')').replace('``','"').replace("''",'"')
				sent+=[tok]
			doc+=[sent]
		return '\n'.join(' '.join([w for w in sent]) for sent in doc)

	def lines_txt(self):
		sentence=[]
		for line in self.lines_xml():
			if '<sentence ' in line and sentence:
				yield ' '.join(sentence)
				sentence=[]
			if '<word>' in line:
				word=line.split('<word>')[1].split('</word>')[0]
				sentence+=[word]

		if sentence:
			yield ' '.join(sentence)



# -*- coding: utf-8 -*-

import os

from lltk.corpus import Corpus
from lltk.corpus import CorpusMeta,name2corpus


class LitLab(Corpus):
	PATH_XML = 'litlab/_xml_litlab'
	EXT_XML = '.xml.gz'

	PATH_TXT = 'litlab/_txt_litlab'
	EXT_TXT = '.txt.gz'

	PATH_METADATA='litlab/corpus-metadata.LitLab.xlsx'

	TEXT_CLASS=TextLitLab

	def __init__(self):
		super(LitLab,self).__init__('LitLab',path_txt=self.PATH_TXT,ext_txt=self.EXT_TXT,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)



class LitLabCanon(CorpusMeta):
	def __init__(self, name='LitLabCanon'):
		from lltk.corpus.chadwyck import Chadwyck
		from lltk.corpus.markmark import MarkMark
		from lltk.corpus.gildedage import GildedAge
		corpora = [Chadwyck(), GildedAge(), MarkMark()]
		super(LitLabCanon,self).__init__(name=name,corpora=corpora)
		self.path = os.path.dirname(__file__)
