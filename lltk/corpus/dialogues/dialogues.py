# -*- coding: utf-8 -*-
from __future__ import absolute_import



#### TEXT CLASS
import codecs,os
from lltk.text.text import Text

class TextEnglishDialogues(Text):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']

	@property
	def meta_by_file(self,bad_tags={'textbibliography'}):
		md={}
		md['id']=self.id
		md['medium']='Dialogue'
		#num_lines=0
		#num_stanzas=0
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		header=dom('dialogueheader')[0]
		for tag in header:
			try:
				"""
				print tag
				print tag.name
				print tag.attrs
				print tag.text
				print '##'*100
				print
				"""
				if tag.name in bad_tags: continue
				md[tag.name]=tag.text
				for k,v in list(tag.attrs.items()):
					md[tag.name+'_'+k]=v

			except AttributeError:
				pass

		md['year']=int(md['speechpubdate'][3:7]) # format is: "5: 1737/1744" or "2: 1631" or etc
		return md


	def text_plain(self, dialogue_only=True, BAD=['comment']):
		txt=[]
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in BAD: [x.extract() for x in dom.findAll(tag)]
		if dialogue_only or True: # @TODO other option hasn't been implemented yet
			#txt='\n\n'.join([x.text.replace('\r\n',' ').replace('\r',' ').replace('\n',' ').replace('  ',' ').replace('  ',' ').strip() for x in dom('dialogue')])
			for dtag in dom('dialogue'):
				utt=dtag.text
				utt=utt.replace('\r\n',' ').replace('\r',' ').replace('\n',' ')
				while '  ' in utt: utt=utt.replace('  ',' ')
				utt=utt.strip()
				txt+=[utt]
		txt='\n\n'.join(txt).strip()
		return txt






### CORPUS CLASS

from lltk.corpus.corpus import Corpus
import os,codecs,re
from lltk import tools

class EnglishDialogues(Corpus):
	"""
	Steps taking in bringing this corpus from raw to refined.
	>> wrote meta_by_file() and text_plain() for TextEnglishDialogues

	from lltk.corpus.dialogues import EnglishDialogues
	corpus=EnglishDialogues()
	corpus.tokenize_texts()
	corpus.save_metadata()
	corpus.gen_mfw(yearbin=50)
	corpus.gen_freq_table(n=10000)
	"""

	TEXT_CLASS=TextEnglishDialogues
	PATH_TXT = 'dialogues/_txt_corpus_english_dialogues'
	PATH_XML = 'dialogues/_xml_corpus_english_dialogues'
	#PATH_RAW = '/Volumes/Present/DH/corpora/chadwyck_poetry/raw'
	PATH_METADATA = 'dialogues/corpus-metadata.EnglishDialogues.txt'
	EXT_XML='.xml'

	def __init__(self):
		super(EnglishDialogues,self).__init__('EnglishDialogues',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML)
		self.path = os.path.dirname(__file__)
