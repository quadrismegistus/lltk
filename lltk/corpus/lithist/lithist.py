from __future__ import absolute_import

from lltk.corpus.corpus import Corpus,load_corpus
from lltk.text.text import Text
import os

"""
class LitHist(Corpus):
	TEXT_CLASS=Text
	PATH_TXT = 'lithist/_txt_lithist'
	PATH_XML = 'lithist/_xml_lithist'
	PATH_METADATA = 'lithist/corpus-metadata.LitHist.txt'

	def __init__(self):
		super(LitHist,self).__init__('LitHist',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
"""


from lltk.corpus.corpus import CorpusMeta,name2corpus
import os

def lithist_load_corpus(name,medium={},genre={}):
	if name=='Chadwyck':
		c=load_corpus(name)
		c._texts = [t for t in c.texts() if t.year>1500 and t.year<1900]
	elif name=='ChadwyckPoetry':
		c=load_corpus(name)
		#c._texts = [t for t in c.texts() if t.meta['posthumous']=='False' and t.year>1500 and t.year<2000]
	elif name=='ChadwyckDrama':
		c=load_corpus(name)
		#c._texts = [t for t in c.texts() if t.meta['posthumous']=='False' and t.year>1500 and t.year<2000]
	elif name=='ECCO_TCP_in_Sections':
		c=load_corpus('ECCO_TCP').sections
		c._texts = [t for t in c.texts() if t.year>=1700 and t.year<1800]
	elif name=='ECCO_TCP':
		c=load_corpus('ECCO_TCP')
		c._texts = [t for t in c.texts() if t.year>=1700 and t.year<1800]
	elif name=='EEBO_TCP_in_Sections':
		c=load_corpus('EEBO_TCP').sections
		c._texts = [t for t in c.texts() if t.year>=1500 and t.year<1700]
	elif name=='EEBO_TCP':
		c=load_corpus('EEBO_TCP')
		c._texts = [t for t in c.texts() if t.year>=1500 and t.year<1700]
	else:
		c=load_corpus(name)

	if medium:
		c._texts=[t for t in c.texts() if t.medium in medium]

	if genre:
		c._texts=[t for t in c.texts() if t.genre in genre]

	return c

class LitHist(CorpusMeta):
	CORPORA=[
		'Chadwyck','ChadwyckPoetry','ChadwyckDrama',
		'ECCO_TCP','EEBO_TCP', #,'ECCO_TCP_in_Sections','EEBO_TCP_in_Sections' (too many files)
		'Sellers',   # 'TedJDH' (replicated in Sellers + ECCO_TCP)
		'DialNarr', #LitLab (too noisy),
		'MarkMark','Chicago','GildedAge',
		'COHA','COCA','CLMET','OldBailey','EnglishDialogues',
		'Spectator']

	def __init__(self, name_meta='LitHist',corpora=None):
		if not corpora:
			corpora=[lithist_load_corpus(c) for c in self.CORPORA]
			corpora=[x for x in corpora if x is not None]
			#print(corpora)

		super(LitHist,self).__init__(name=name_meta,corpora=corpora)
		self.path=os.path.join(self.path,'lithist')









class LitHistProse(LitHist):

	#CORPORA = [x for x in LitHist.CORPORA if not x in {'ChadwyckPoetry','ChadwyckDrama'}]
	CORPORA=[
		'Chadwyck',
		#'ECCO_TCP','EEBO_TCP', #,
		'ECCO_TCP_in_Sections','EEBO_TCP_in_Sections', # (too many files)
		'Sellers',   # 'TedJDH' (replicated in Sellers + ECCO_TCP)
		'MarkMark','Chicago',
		'COHA','Spectator']

	def __init__(self,name='LitHistProse',corpora=None):
		#super(LitHist,self).__init__(name=name,corpora=None)
		if not corpora:
			corpora=[lithist_load_corpus(c,medium='Prose') for c in self.CORPORA]
			corpora=[x for x in corpora if x is not None] # and len(x.texts())>0]
			#print(corpora)

		super(LitHist,self).__init__(name=name,corpora=corpora)

		# filter for prose
		#print(len(self.texts()))
		self._texts = [t for t in self.texts() if t.medium=='Prose']
		#print(len(self.texts()))
		self.path=os.path.join(self.path,'lithist')




class LitHistAuthors(CorpusMeta):
	CORPORA={'Chadwyck','ChadwyckPoetry','ChadwyckDrama','COHA','ECCO_TCP','EEBO_TCP'} #,'Sellers','CLMET','Spectator','Chicago','MarkMark'}
	YEAR_AUTHOR_30_MIN=1500

	def __init__(self, name='LitHistAuthors',corpora=None):
		if not corpora: corpora=[]

		for name in self.CORPORA:
			c=load_corpus(name)
			if c is None: continue
			c._texts = [t for t in c.texts() if t.year_author_is_30>self.YEAR_AUTHOR_30_MIN]
			corpora.append(c)

		super(LitHistAuthors,self).__init__(name=name,corpora=corpora)
		self.path=os.path.join(self.path,'lithist')




class LitHistHathi(CorpusMeta):
	CORPORA=LitHist.CORPORA + ['HathiEngLit','HathiBio']

	def __init__(self, name_meta='LitHistHathi',corpora=None):
		if not corpora:
			corpora=[lithist_load_corpus(c) for c in self.CORPORA]
			corpora=[x for x in corpora if x is not None]
			#print(corpora)
		super().__init__(name=name_meta,corpora=corpora)
