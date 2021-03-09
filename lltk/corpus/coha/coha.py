from __future__ import absolute_import

from lltk.corpus.corpus import Corpus
from lltk.text.text import Text
from lltk import tools
import os



class TextCOHA(Text):
	def text_plain(self):
		txt=self.text_plain_from_txt()
		txt = txt.replace('@ @ @ @ @ @ @ @ @ @','\n\n')
		return txt



class COHA(Corpus):
	"""
	Steps taken for creation:
	1) Set PATH_TXT to root
	2) Set self.ext_xml = '.txt'
	3) Set self.PATH_ORIG_SOURCE_METADATA='/Volumes/Present/DH/corpora/coha/sources_coha.xlsx'
	4) Wrote custom self.save_metadata() that takes (3) and saves a suitable corpus file (i.e. one with an "id" column)
	5) Executed: corpus.save_metadata()
	6)			 corpus.gen_freqs()
	7) 			 corpus.update_metadata() # adds {num_words, ocr_accuracy} columns
	8)			 corpus.gen_mfw(yearbin=10)

	"""
	TEXT_CLASS=TextCOHA
	PATH_TXT = '/Volumes/Present/DH/corpora/coha/txt'
	PATH_ORIG_SOURCE_METADATA='/Volumes/Present/DH/corpora/coha/sources_coha.xlsx'
	PATH_METADATA = 'coha/corpus-metadata.COHA.txt'

	def __init__(self):
		super(COHA,self).__init__('COHA',path_txt=self.PATH_TXT,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
		self.ext_txt='.txt'

	def update_metadata(self):
		super(COHA,self).save_metadata(num_words=True,ocr_accuracy=True)

	def save_metadata(self):
		ld=tools.read_ld(self.PATH_ORIG_SOURCE_METADATA)

		# fix empty column and other things
		genre2Genre = {u'FIC':'Fiction', u'MAG':'Magazine', u'NEWS':'News', u'NF':'Non-Fiction'}
		for d in ld:
			d['note']=d['']
			del d['']
			d['medium']='Prose'

			# do genre assignments
			d['genre']=genre2Genre[d['genre']] # clean genre
			if d['note']=='[Movie script]':
				d['genre']='Film'
			elif d['note']=='[Play script]':
				d['genre']='Drama'


		# Align new and original ID's
		dd=tools.ld2dd(ld,'textID')
		for t in self.texts(from_files=True):
			textID=t.id.split('_')[-1]
			if textID in dd:
				dd[textID]['id']=t.id

		# Filter out any metadata rows that didn't correspond to files
		ld = [d for d in ld if 'id' in d and d['id']]

		timestamp=tools.now().split('.')[0]
		tools.write2(os.path.join(self.path,'corpus-metadata.%s.%s.txt' % (self.name,timestamp)), ld)
