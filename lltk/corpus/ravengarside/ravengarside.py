from lltk.imports import BaseCorpus, BaseText
class TextRavenGarside(BaseText): pass
class RavenGarside(BaseCorpus):
	TEXT_CLASS=TextRavenGarside

	def load_metadata(self, *x, **y):
		df = super().load_metadata(*x, **y)
		df['genre_raw'] = 'Novel'
		df['genre'] = 'Fiction'
		return df