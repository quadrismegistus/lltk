from lltk.imports import *
from lltk.corpus.tcp import *

class TextEEBO_TCP(TextTCP):
	@property
	def meta_by_file(self):
		if not hasattr(self,'_meta'):
			mtxt=''
			for line in self.lines_xml():
				mtxt+=line
				if '</HEADER>' in line:
					break
			md=self.extract_metadata(mtxt)
			md['fnfn_xml']=self.fnfn
			md['id']=self.id
			md['genre'],md['medium']=self.return_genre()
			del md['notes']
			self._meta=md
		return self._meta

class EEBO_TCP(TCP):
	TEXT_CLASS=TextEEBO_TCP
	EXT_XML = '.headed.xml.gz'

	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		#meta['genre']=[fix_genre(genre,title) for genre,title in zip(meta.genre, meta.title)]
		return meta#.query(f'{self.MIN_YEAR}<=year<{self.MAX_YEAR}')	