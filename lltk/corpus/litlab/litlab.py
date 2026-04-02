from lltk.imports import *

class TextLitLab(BaseText): pass

class LitLab(BaseCorpus):
	TEXT_CLASS=TextLitLab

	def load_metadata(self, *x, **y):
		meta = super().load_metadata(*x, **y)
		if 'genre' in meta.columns:
			meta['genre_raw'] = meta['genre']
		meta['genre'] = 'Fiction'
		return meta


# class LitLabCanon(CorpusMeta):
# 	def __init__(self, name='LitLabCanon'):
# 		from lltk.corpus.chadwyck import Chadwyck
# 		from lltk.corpus.markmark import MarkMark
# 		from lltk.corpus.gildedage import GildedAge
# 		corpora = [Chadwyck(), GildedAge(), MarkMark()]
# 		super(LitLabCanon,self).__init__(name=name,corpora=corpora)
# 		self.path = os.path.dirname(__file__)
