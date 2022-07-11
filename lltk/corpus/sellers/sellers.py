from __future__ import absolute_import
import os
from lltk.corpus.corpus import BaseCorpus
from lltk.text.text import BaseText

class TextSellers(BaseText):
	@property
	def medium(self):
		if self.genre in {'Drama'}:
			return 'Unknown'
		elif self.genre in {'Poetry'}:
			return 'Verse'
		else:
			return 'Prose'


class Sellers(BaseCorpus):
	TEXT_CLASS=TextSellers
