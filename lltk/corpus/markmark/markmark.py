from __future__ import absolute_import
# -*- coding: utf-8 -*-

from lltk.corpus import Corpus
from lltk.text import Text
import os,codecs,re
from lltk import tools

class MarkMark(Corpus):
	"""
	Steps taking in bringing this corpus from raw to refined.

	from lltk.corpus.markmark import MarkMark
	corpus = MarkMark()
	corpus.gen()
	corpus.tokenize_texts()
	corpus.save_metadata()
	corpus.refine_metadata()
	corpus.gen_mfw(yearbin=50,year_min=1600,year_max=2000)
	corpus.gen_freq_table()
	"""

	TEXT_CLASS=Text
	PATH_TXT = 'markmark/_txt_markmark'
	PATH_RAW = '/Users/ryan/DH/corpora/litlab-20c/texts/'
	PATH_METADATA = 'markmark/corpus-metadata.MarkMark.txt'
	#EXT_XML='.xml'
	EXT_TXT='.txt'

	def __init__(self):
		super(MarkMark,self).__init__('MarkMark',path_txt=self.PATH_TXT,path_metadata=self.PATH_METADATA,ext_txt=self.EXT_TXT)
		self.path = os.path.dirname(__file__)

	def gen(self,rootdir=None,metadata_fn='metadata.txt',text_fn='text.txt',save_texts=True):
		"""
		Mapping from raw to this corpus's data.
		"""
		if not rootdir: rootdir=self.PATH_RAW

		#for root, subdirs, files in os.walk(rootdir):
		#	if 'text.txt' in files:
		#		print root,subdirs,files

		meta_ld=[]
		for author_folder in os.listdir(rootdir):
			author_path=os.path.join(rootdir,author_folder)
			if not os.path.isdir(author_path): continue
			author_meta_path = os.path.join(author_path,metadata_fn)
			author_meta = get_meta(author_meta_path) if os.path.exists(author_meta_path) else {}

			for text_folder in os.listdir(author_path):
				text_path=os.path.join(author_path,text_folder)
				if not os.path.isdir(text_path): continue

				text_meta_path = os.path.join(text_path,metadata_fn)
				text_text_path = os.path.join(text_path,text_fn)

				text_meta = get_meta(text_meta_path) if os.path.exists(text_meta_path) else {}
				metadx = dict(list(author_meta.items()) + list(text_meta.items()))
				metadx ['id'] = text_id = text_path.replace(rootdir,'').replace('/','.').replace(' ','_')
				meta_ld+=[metadx]

				if save_texts:
					text_text = ''
					if os.path.exists(text_text_path):
						with codecs.open(text_text_path,encoding='utf-8',errors='ignore') as f:
							text_text=f.read()
					text_ofnfn = os.path.join(self.path_txt,text_id+self.EXT_TXT)
					tools.write2(text_ofnfn,text_text)




		tools.write2(self.path_metadata, meta_ld)









def get_meta(fn,rename_d = {'name_full':'author'}):
	odx={}
	with codecs.open(fn,encoding='utf-8') as f:
		for ln in f:
			if '=' in ln:
				a,b=ln.strip().split('=',1)
				odx[a]=b
	for k,v in list(rename_d.items()):
		if k in odx:
			odx[v]=odx[k]
			del odx[k]
	return odx
