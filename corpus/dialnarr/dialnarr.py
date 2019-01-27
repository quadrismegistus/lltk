import os
from lit.corpus import Corpus
from lit.text import Text



class TextDialNarr(Text):
	@property
	def meta_by_file(self):
		# 1825.Child.Am.F.The_Rebels_Or_Boston_Before_The_Revolution.dialogue
		md={}
		fn=os.path.basename(self.fnfn_txt)
		fn=fn.replace('.ascii','')
		md['year'],md['author'],md['nation'],md['gender'],md['title'],md['genre'],txt=fn.split('.')
		md['title']=md['title'].replace('_',' ')
		md['genre']='Fictional '+md['genre'].title()
		md['medium']='Prose'
		return md




class DialNarr(Corpus):
	TEXT_CLASS=TextDialNarr
	PATH_TXT = 'dialnarr/_txt_dialnarr'
	PATH_XML = None
	PATH_METADATA = 'dialnarr/corpus-metadata.DialNarr.txt'

	def __init__(self):
		super(DialNarr,self).__init__('DialNarr',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
		self.ext_txt = '.txt'
