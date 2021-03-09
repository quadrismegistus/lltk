from __future__ import absolute_import
# -*- coding: utf-8 -*-

from lltk.corpus.corpus import CorpusMeta,name2corpus
from lltk.corpus.litlab import LitLab
from lltk.corpus.tedjdh import TedJDH
from lltk.corpus.chadwyck import Chadwyck
from lltk.corpus.gildedage import GildedAge
from lltk.corpus.eebo import EEBO_TCP
import os

class Fiction1700_1900(CorpusMeta):
	def __init__(self, name='Fiction1700_1900'):
		litlab=LitLab()
		tedjdh=TedJDH()
		chadwyck=Chadwyck()
		gildedage=GildedAge()

		corpora = [litlab, tedjdh, chadwyck, gildedage]

		for c in corpora:
			c._texts = [t for t in c.texts() if t.year>=1700 and t.year<1900]

		tedjdh._texts = [t for t in tedjdh.texts() if t.medium in ['Non-Fiction','Fiction']]

		super(Fiction1700_1900,self).__init__(name=name,corpora=corpora)
		self.path=os.path.join(self.path,'fictio')

class FictionPoetry1700_1900(CorpusMeta):
	def __init__(self, name='FictionPoetry1700_1900'):
		litlab=LitLab()
		tedjdh=TedJDH()
		chadwyck=Chadwyck()
		gildedage=GildedAge()

		corpora = [litlab, tedjdh, chadwyck, gildedage]

		for c in corpora:
			c._texts = [t for t in c.texts() if t.year>=1700 and t.year<1900]

		tedjdh._texts = [t for t in tedjdh.texts() if t.medium in ['Non-Fiction','Fiction','Poetry']]

		super(FictionPoetry1700_1900,self).__init__(name=name,corpora=corpora)
		self.path=os.path.join(self.path,'fictio')

class FictionPoetry1600_1900(CorpusMeta):
	def __init__(self, name='FictionPoetry1600_1900'):
		litlab=LitLab()
		tedjdh=TedJDH()
		chadwyck=Chadwyck()
		gildedage=GildedAge()
		eebo_tcp=EEBO_TCP()

		corpora = [litlab, tedjdh, chadwyck, gildedage, eebo_tcp]

		for c in corpora:
			c._texts = [t for t in c.texts() if t.medium in {'Non-Fiction','Fiction','Poetry'} and t.year>=1600 and t.year<1900]

		super(FictionPoetry1600_1900,self).__init__(name=name,corpora=corpora)
		self.path=os.path.join(self.path,'fictio')

class FictionPoetry1600_1900_Strict(CorpusMeta):
	def __init__(self, name='FictionPoetry1600_1900'):
		#litlab=LitLab()
		tedjdh=TedJDH()
		chadwyck=Chadwyck()
		#gildedage=GildedAge()
		eebo_tcp=EEBO_TCP()

		corpora = [tedjdh, chadwyck, eebo_tcp]

		for c in corpora:
			c._texts = [t for t in c.texts() if t.medium in {'Non-Fiction','Fiction','Poetry'} and t.year>=1600 and t.year<1900]

		super(FictionPoetry1600_1900_Strict,self).__init__(name=name,corpora=corpora)
		self.path=os.path.join(self.path,'fictio')
