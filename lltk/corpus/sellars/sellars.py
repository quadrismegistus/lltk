from __future__ import absolute_import
import os
from lltk.corpus import Corpus
from lltk.text import Text

class TextSellars(Text):
	@property
	def medium(self):
		if self.genre in {'Drama'}:
			return 'Unknown'
		elif self.genre in {'Poetry'}:
			return 'Verse'
		else:
			return 'Prose'


class Sellars(Corpus):
	TEXT_CLASS=TextSellars
