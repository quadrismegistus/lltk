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
		meta = self.merge_linked_metadata(meta)
		# EEBO's own 'genre' column is really a medium (Prose/Verse/Drama) — rename it
		if 'genre' in meta.columns:
			meta['medium'] = meta['genre']
		# Genre comes from linked ESTC only
		if 'estc_genre' in meta.columns:
			meta['genre'] = meta['estc_genre']
		else:
			meta['genre'] = None
		if 'estc_genre_raw' in meta.columns:
			meta['genre_raw'] = meta['estc_genre_raw']
		else:
			meta['genre_raw'] = None
		# Medium overrides genre
		if 'medium' in meta.columns:
			meta.loc[meta['medium'] == 'Verse', 'genre'] = 'Poetry'
			meta.loc[meta['medium'] == 'Drama', 'genre'] = 'Drama'
		return meta