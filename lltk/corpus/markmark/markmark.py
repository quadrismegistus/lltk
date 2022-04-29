from lltk.corpus.corpus import BaseCorpus
from lltk.imports import *

class TextMarkMark(BaseText): pass

class MarkMark(BaseCorpus):
	ID='markmark'
	NAME='MarkMark'
	TEXT_CLASS=TextMarkMark

	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']='Fiction'
		for bc in {'author_id'}:
			if bc in meta.columns:
				meta=meta.drop(bc,1)
		return meta
