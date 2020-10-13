from __future__ import absolute_import
### TEXT CLASS
from lltk.text import Text

class TextCLMET(Text):
	genre2Genre={u'Drama':'Drama',
	 u'LET':'Letter',
	 u'Narrative fiction':'Fiction',
	 u'Narrative non-fiction':'Biography',
	 u'Other':'Other',
	 u'Treatise':'Treatise'}

	@property
	def meta_by_file(self,bad_tags={'html','body'}):
		import codecs,bs4,os
		md={}
		md['medium']='Prose'

		txt=self.text_xml
		meta = txt.split('<text>')[0]
		dom=bs4.BeautifulSoup(meta,'lxml')
		for tag in dom():
			if tag.name in bad_tags: continue
			md[tag.name]=tag.text

		md['id_orig']=md.get('id','')
		md['id']=self.id
		md['genre']=self.genre2Genre.get(md.get('genre'),'')
		return md





### CORPUS CLASS
from lltk.corpus import Corpus
import os

class CLMET(Corpus):
	"""
	Steps taken for creation:
	1) Set PATH_XMl to root
	2) Set self.ext_xml = '.txt'
	3) Wrote TextCLMET.meta_by_file()
	4) Wrote TextCLMET.text_plain()
	5) Executed: corpus.gen_freqs()
	6)			 corpus.save_metadata()
	7)			 corpus.gen_mfw(yearbin=50,year_min=1700,year_max=1899)
	8)			 corpus.gen_freq_table()

	"""
	TEXT_CLASS=TextCLMET
	PATH_TXT = 'clmet/_txt_clmet'
	PATH_XML = '/Volumes/Present/DH/corpora/clmet/xml'
	PATH_METADATA = 'clmet/corpus-metadata.CLMET.txt'

	def __init__(self):
		super(CLMET,self).__init__('CLMET',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
		self.ext_xml='.txt'
