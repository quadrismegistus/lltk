from lltk.corpus.corpus import Corpus

class MarkMark(Corpus):
	@property
	def metadata(self):
		meta=super().metadata
		meta['genre']='Fiction'
		return meta
