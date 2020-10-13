from __future__ import absolute_import
# -*- coding: utf-8 -*-

from lltk.corpus import Corpus
from lltk.text import Text
import os

class RavenGarside(Corpus):
	TEXT_CLASS=Text
	PATH_TXT = 'ravengarside/_txt_ravengarside'
	PATH_XML = 'ravengarside/_xml_ravengarside'
	PATH_METADATA = 'ravengarside/corpus-metadata.RavenGarside.xlsx'

	def __init__(self):
		super(RavenGarside,self).__init__('RavenGarside',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
