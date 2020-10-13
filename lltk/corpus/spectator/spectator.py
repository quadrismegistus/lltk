from __future__ import absolute_import

from lltk.corpus import Corpus
from lltk.text import Text
from lltk import tools
import os

class Spectator(Corpus):
	TEXT_CLASS=Text
	PATH_TXT = 'spectator/_txt_spectator'
	PATH_XML = 'spectator/_xml_spectator'
	PATH_METADATA = 'spectator/corpus-metadata.Spectator.txt'

	def __init__(self):
		super(Spectator,self).__init__('Spectator',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)

	def gen(self):
		## gen metadata
		meta_ld=[]
		for idx in sorted(self.get_text_ids(from_files=True)):
			odx={}
			odx['id']=idx
			odx['periodical'],odx['number'],odx['date'],odx['author']=idx.split('.')
			odx['year']=odx['date'][:4]
			odx['title']=''
			meta_ld+=[odx]
		tools.write(self.path_metadata, meta_ld)
