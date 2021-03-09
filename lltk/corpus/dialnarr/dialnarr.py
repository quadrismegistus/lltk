from __future__ import absolute_import
import os
from lltk.corpus.corpus import Corpus
from lltk.text.text import Text



class TextDialNarr(Text):
	
	def get_meta_from_file(self):
		# 1825.Child.Am.F.The_Rebels_Or_Boston_Before_The_Revolution.dialogue
		md={}
		#idx=os.path.splitext(self.path_txt)[0].replace(self.corpus.path_txt,'')
		#if idx.startswith(os.path.sep): idx=idx[1:]
		#md['id']=idx.replace('.ascii','')
		md['id']=self.id

		fn=os.path.basename(self.path_txt)
		fn=fn.replace('.ascii','')
		md['year'],md['author'],md['nation'],md['gender'],md['title'],md['genre'],txt=fn.split('.')
		md['title']=md['title'].replace('_',' ')
		md['genre']='Fictional '+md['genre'].title()
		md['medium']='Prose'
		return md

	@property
	def path_txt(self):
		return self.fnfn_txt.replace('.ascii.','.ascii.txt')


class DialNarr(Corpus):
	TEXT_CLASS=TextDialNarr
