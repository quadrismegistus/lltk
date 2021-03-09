from lltk.imports import *
class Spectator(Corpus):
	def load_metadata(self):
		meta=super().load_metadata()
		meta['genre']='Periodical'
		return meta