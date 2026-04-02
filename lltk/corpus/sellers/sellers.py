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


SELLERS_GENRE_MAP = {
	'Fiction': 'Fiction',
	'Poetry': 'Poetry',
	'Drama': 'Drama',
	'Biography': 'Biography',
	'Non-Fiction': 'Nonfiction',
	'Oratory': 'Speech',
	'Letters': 'Letters',
}

class Sellers(BaseCorpus):
	TEXT_CLASS=TextSellers

	def load_metadata(self, *x, **y):
		meta = super().load_metadata(*x, **y)
		if 'genre' in meta.columns:
			meta['genre_raw'] = meta['genre']
			meta['genre'] = meta['genre_raw'].map(SELLERS_GENRE_MAP)
		return meta
