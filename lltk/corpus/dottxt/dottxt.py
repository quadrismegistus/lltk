from __future__ import absolute_import

from lltk.corpus.corpus import Corpus
from lltk.text.text import Text
# from lltk.corpus.default import TextPlainTextCorpus,PlainTextCorpus
import os

# class DotTXT(PlainTextCorpus):
# 	TEXT_CLASS=TextPlainTextCorpus
#     PATH
# 	def __init__(self, PATH_TXT, PATH_METADATA, PATH_ROOT=None, name=None):
# 		self.PATH_TXT=PATH_TXT
# 		self.PATH_METADATA=PATH_METADATA
# 		self.PATH_ROOT=os.path.split(PATH_METADATA)[0]
# 		super(PlainTextCorpus,self).__init__(
# 						'PlainTextCorpus' if not name else name,
# 						path_txt=self.PATH_TXT,
# 						path_metadata=self.PATH_METADATA,
# 						ext_txt='.txt')
# 		self.path = self.PATH_ROOT

class DotTXT(Corpus):
	TEXT_CLASS=Text
	PATH_TXT = 'dottxt/_txt_dottxt'
	PATH_METADATA = 'dottxt/corpus-metadata.DotTXT.xlsx'
	EXT_TXT='.txt'

	def __init__(self):
		super(DotTXT,self).__init__('DotTXT',path_txt=self.PATH_TXT,path_metadata=self.PATH_METADATA,ext_txt=self.EXT_TXT)
		self.path = os.path.dirname(__file__)
