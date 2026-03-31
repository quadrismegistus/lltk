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

def _normalize_estc_id(raw):
	"""Normalize EEBO id_stc format: 'ESTC S115782' → 'S115782' (zero-padded)."""
	if not raw or (isinstance(raw, float) and raw != raw):
		return ''
	raw = str(raw).strip()
	if raw.startswith('ESTC '):
		raw = raw[5:]
	if not raw:
		return ''
	letter = raw[0]
	number = raw[1:]
	return f'{letter}{number.zfill(6)}'


class EEBO_TCP(TCP):
	TEXT_CLASS=TextEEBO_TCP
	EXT_XML = '.headed.xml.gz'
	LINKS = {'estc': ('id_stc', 'id_estc')}
	LINK_TRANSFORMS = {'id_stc': _normalize_estc_id}

	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		if not len(meta):
			return meta
		return self.merge_linked_metadata(meta)