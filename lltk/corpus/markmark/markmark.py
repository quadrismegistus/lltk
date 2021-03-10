from lltk.corpus.corpus import Corpus

class MarkMark(Corpus):
	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']='Fiction'
		return meta
