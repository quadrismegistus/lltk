from __future__ import absolute_import
import os
from lltk.corpus.corpus import Corpus
from lltk.text.text import Text

class TextSellers(Text):
	@property
	def medium(self):
		if self.genre in {'Drama'}:
			return 'Unknown'
		elif self.genre in {'Poetry'}:
			return 'Verse'
		else:
			return 'Prose'


class Sellers(Corpus):
	TEXT_CLASS=TextSellers
