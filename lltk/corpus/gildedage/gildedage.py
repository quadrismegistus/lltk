from lltk.imports import *

class TextGildedAge(BaseText): pass

class GildedAge(BaseCorpus):
	TEXT_CLASS = TextGildedAge

	def load_metadata(self, *x, **y):
		df = super().load_metadata(*x, **y)
		df['genre_raw'] = 'Novel'
		df['genre'] = 'Fiction'
		# Normalize IDs: underscores → spaces to match freqs filenames
		df.index = df.index.str.replace('_', ' ')
		df.index.name = 'id'
		return df