from lltk.imports import *

class TextGildedAge(BaseText): pass

class GildedAge(BaseCorpus):
	TEXT_CLASS = TextGildedAge

	def load_metadata(self, *x, **y):
		df = super().load_metadata(*x, **y)
		df['genre_raw'] = 'Novel'
		df['genre'] = 'Fiction'
		return df