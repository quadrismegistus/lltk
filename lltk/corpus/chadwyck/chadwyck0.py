from __future__ import absolute_import
# -*- coding: utf-8 -*-

#### TEXT CLASS
import codecs,os
from lltk import tools
from lltk.text import Text,text_plain_from_xml,clean_text
BAD={'figdesc','head','edit','note','l'}
OK={'p'}

class TextChadwyck(Text):

	@property
	def genre(self): return 'Fiction'

	@property
	def meta_by_file(self):
		md={}
		for line in self.lines_xml():
			#if '<doc>' in line: break

			if '<T1>' in line and not 'title' in md:
				md['title']=line.split('<T1>')[1].split('</T1>')[0]
			if '<A1>' in line and not 'author' in md:
				md['author']=line.split('<A1>')[1].split('</A1>')[0]
			if '<Y1>' in line and not 'year' in md:
				md['year']=line.split('<Y1>')[1].split('</Y1>')[0]

			if 'title' in md and 'author' in md and 'year' in md:
				break

		if self.id.startswith('Early_American_Fiction/'):
			md['nation']='American'
		else:
			md['nation']='British'
		md['medium']='Fiction'
		md['id']=self.id
		md['subcorpus']=self.id.split('/')[0]
		return md


	@property
	def nation(self):
		return 'American' if 'America' in self.id else 'British'


	def text_plain_from_xml(self,txt=None,**attrs):
		if not txt: txt=self.text_xml
		return text_plain_from_xml(txt,body_tag='doc',BAD=BAD,OK=OK)

	def chapters(self,chapter_tags=['div5','div4','div3','div2','div1','div0'],para_tag='p',verse_tag='l',keep_verse=True, metadata=False):
		import bs4,tools
		from collections import defaultdict
		txt=self.text_xml
		done=0
		parent_tags={}
		chapter_tag=None
		for ctag in chapter_tags:
			if '</'+ctag+'>' in txt:
				if not chapter_tag:
					chapter_tag=ctag
				parent_tags[len(parent_tags)]=ctag
		#print chapter_tag,done,parent_tags
		dom=bs4.BeautifulSoup(txt,'lxml')
		# numretu
		for ctag in list(parent_tags.values()):
			for i,ctagx in enumerate(dom(ctag)):
				ctagx['num']=i+1
		for child_i,child in enumerate(chapter_tags):
			immediate_parents=chapter_tags[child_i+1:]
			#print 'xx',child_i,child,immediate_parents
			for ptagi,ptag in enumerate(immediate_parents):
				for pi,parentx in enumerate(dom(ptag)):
					#print 'parent',pi,ptag,child,len(parentx)
					for ci,ctagx in enumerate(parentx(child)):
						#print 'chap',ci,len(ctagx)
						#ctagx['num_in_parent%s' % str(ptagi+1)]=ci+1
						ctagx['num_%s_in_%s' % (child,ptag)]=ci+1
						#print ctagx.attrs


		if keep_verse:
			for tag in dom(verse_tag):
				tag.name=para_tag

		for tag in BAD: [x.extract() for x in dom.findAll(tag)]

		chapters=[]
		for chap_i,xml_chapter in enumerate(dom(chapter_tag)):
			chapter=[]
			if metadata:
				chapter_meta = {'num_chap_in_doc':chap_i+1}

				for level,ptag in sorted(parent_tags.items()):
					parent=xml_chapter.find_parent(ptag)

					if parent:
						for k,v in list(parent.attrs.items()):
							#print '>>',level,ptag,k,v
							chapter_meta[k+'_'+ptag+'_in_doc' if '_in_' not in k else k]=v

			for xml_para in xml_chapter(para_tag):
				if not xml_para: continue
				#print xml_para.attrs
				para=clean_text(xml_para.text).replace('\n',' ')
				while '  ' in para: para=para.replace('  ',' ')
				if para: chapter+=[para]
			if chapter:
				chapter_txt='\n\n'.join(chapter)
				if metadata:
					#chapter_meta['txt']=chapter_txt
					yield (chapter_meta,chapter_txt)
				else:
					yield chapter_txt





### CORPUS CLASS
from lltk.corpus import Corpus
class Chadwyck(Corpus):
	TEXT_CLASS=TextChadwyck
	PATH_TXT = 'chadwyck/_txt_chadwyck'
	PATH_XML = 'chadwyck/_xml_chadwyck'
	PATH_METADATA = 'chadwyck/corpus-metadata.Chadwyck.xlsx'
	EXT_XML='.new'
	EXT_TXT='.txt'
	MIN_YEAR=1500
	MAX_YEAR=1900

	# def __init__(self):
	# 	super(Chadwyck,self).__init__('Chadwyck',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML,ext_txt=self.EXT_TXT)
	# 	self.path = os.path.dirname(__file__)

	@property
	def metadata(self):
		meta=super().metadata
		meta['genre']='Fiction'
		meta=meta.query(f'{self.MIN_YEAR} <= year < {self.MAX_YEAR}')
		return meta

def sens_sens():
	return Chadwyck().textd['Nineteenth-Century_Fiction/ncf0204.08']
