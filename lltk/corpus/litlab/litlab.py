from lltk.imports import *

class TextLitLab(Text): pass

class LitLab(Corpus):
	TEXT_CLASS=TextLitLab


# class LitLabCanon(CorpusMeta):
# 	def __init__(self, name='LitLabCanon'):
# 		from lltk.corpus.chadwyck import Chadwyck
# 		from lltk.corpus.markmark import MarkMark
# 		from lltk.corpus.gildedage import GildedAge
# 		corpora = [Chadwyck(), GildedAge(), MarkMark()]
# 		super(LitLabCanon,self).__init__(name=name,corpora=corpora)
# 		self.path = os.path.dirname(__file__)
