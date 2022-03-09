from lltk.imports import *
class Spectator(BaseCorpus):
	def load_metadata(self):
		meta=super().load_metadata()
		meta['genre']='Periodical'
		return meta