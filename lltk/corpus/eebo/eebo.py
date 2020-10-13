from __future__ import absolute_import
# -*- coding: utf-8 -*-
import codecs
from lltk.text import Text
from lltk.corpus.tcp import TextTCP
import tools
spelling_d={}

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

	def text_plain_from_xml(self, xml=None, OK=['p','l'], BAD=[], body_tag='text', force_xml=False, text_only_within_medium=True,modernize_spelling=True):
		global spelling_d

		txt=super().text_plain_from_xml(xml=xml, OK=OK, BAD=BAD, body_tag=body_tag, force_xml=force_xml, text_only_within_medium=text_only_within_medium)

		if modernize_spelling:
			if not spelling_d: spelling_d=tools.get_spelling_modernizer()
			txt = tools.modernize_spelling_in_txt(txt,spelling_d)

		return txt




import os
from lltk.corpus.tcp import TCP,TextSectionTCP

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
	EXT_XML = '.headed.xml.gz'
	EXT_TXT='.txt'
	TEXT_SECTION_CLASS=TextSectionTCP
