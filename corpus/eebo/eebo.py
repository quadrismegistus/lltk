# -*- coding: utf-8 -*-
import codecs
from lit.text import Text
from lit.text.tcp import TextTCP

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

import os
from lit.corpus.tcp import TCP
from lit.text.tcp import TextSectionTCP

class EEBO_TCP(TCP):
	"""
	Steps for corpus generation
	1) Hook up path to TCP XML
	2) This is built on TextTCP class which can read metadata and text from TCP xml files
	3) gen_freqs()
	4) save_metadata()
	5) gen_mfw(year_min=1500,year_max=1699,yearbin=50)
	6) gen_freq_table()
	"""


	TEXT_CLASS=TextEEBO_TCP
	PATH_XML = '/Volumes/Present/DH/corpora/eebo/_xml_eebo_tcp'
	PATH_INDEX = 'eebo/_index_eebo_tcp'
	EXT_XML = '.headed.xml.gz'
	PATH_METADATA = 'eebo/corpus-metadata.EEBO-TCP.txt'
	TEXT_SECTION_CLASS=TextSectionTCP

	def __init__(self):
		super(EEBO_TCP,self).__init__('EEBO-TCP',self.PATH_XML,self.PATH_INDEX,self.EXT_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)