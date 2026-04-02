from lltk.imports import *

TEDJDH_GENRE_MAP = {
	'Fiction': 'Fiction',
	'Poetry': 'Poetry',
	'Drama': 'Drama',
	'Biography': 'Biography',
	'Non-Fiction': 'Nonfiction',
	'Oratory': 'Speech',
	'Letters': 'Letters',
	# Juvenilia: too mixed to assign
}

class TedJDH(BaseCorpus):
	def load_metadata(self, *x, **y):
		meta = super().load_metadata(*x, **y)
		if 'genre' in meta.columns:
			meta['genre_raw'] = meta['genre']
			meta['genre'] = meta['genre_raw'].map(TEDJDH_GENRE_MAP)
		return meta