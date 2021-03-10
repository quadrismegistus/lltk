import os
import lltk
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus



class TextSemanticCohort(Text):
	pass



class SemanticCohort(Corpus):
	TEXT_CLASS=TextSemanticCohort

	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		return self.download(**attrs)

	def load_metadata(self,*x,**y):
		"""
		Magic attribute loading metadata, and doing any last minute customizing
		"""
		meta=super().load_metadata()
		# ?
		return meta

