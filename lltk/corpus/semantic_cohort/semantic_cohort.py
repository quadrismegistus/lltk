import os
import lltk
from lltk.text.text import BaseText
from lltk.corpus.corpus import BaseCorpus



class TextSemanticCohort(BaseText):
	pass



class SemanticCohort(BaseCorpus):
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

