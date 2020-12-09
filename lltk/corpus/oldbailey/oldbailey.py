# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function


import codecs,os

from lltk.text import Text

class TextOldBailey(Text):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']

	@property
	def meta_by_file(self,bad_tags={'div1'}):
		print('>>',self.id,'...')
		md={}
		md['id']=self.id
		md['medium']='Dialogue'

		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in bad_tags: [x.extract() for x in dom.findAll(tag)]

		for tag in dom('interp'):
			try:
				md[tag.attrs['type']]=tag.attrs['value']
			except AttributeError:
				pass

		md['year']=int(md['date'][0:4]) # format is: "5: 1737/1744" or "2: 1631" or etc
		return md


	def text_plain(self, dialogue_only=True, BAD=[''], force_xml=None):
		txt=[]
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in BAD: [x.extract() for x in dom.findAll(tag)]
		if dialogue_only or True: # @TODO other option hasn't been implemented yet
			for dtag in dom('u'):
				utt=dtag.text
				utt=utt.replace('\r\n',' ').replace('\r',' ').replace('\n',' ')
				while '  ' in utt: utt=utt.replace('  ',' ')
				utt=utt.strip()
				txt+=[utt]
		txt='\n\n'.join(txt).strip()
		return txt






from lltk.corpus import Corpus
import os,codecs,re
from lltk import tools

class OldBailey(Corpus):
	"""
    [This is the PARSED Old Bailey. Full one not implemented yet.]

	Steps taking in bringing this corpus from raw to refined.
	>> wrote meta_by_file() and text_plain() for TextEnglishDialogues

	from lltk.corpus.oldbailey import OldBailey
	corpus=OldBailey()
	corpus.tokenize_texts()
	corpus.save_metadata()
	corpus.gen_mfw(yearbin=50)
	corpus.gen_freq_table()
	"""

	TEXT_CLASS=TextOldBailey
	PATH_TXT = 'oldbailey/_txt_parsed_old_bailey'
	PATH_XML = 'oldbailey/_xml_parsed_old_bailey'
	PATH_METADATA = 'oldbailey/corpus-metadata.OldBailey.xlsx'
	EXT_XML='.xml'

	def __init__(self):
		super(OldBailey,self).__init__('OldBailey',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML)
		self.path = os.path.dirname(__file__)
	@property
	def metadata(self):
		meta=super().metadata
		meta['genre']='Trial'
		meta['title']=''
		return meta