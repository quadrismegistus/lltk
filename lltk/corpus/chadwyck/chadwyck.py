from __future__ import absolute_import
# -*- coding: utf-8 -*-

#### TEXT CLASS
import codecs,os
from lltk import tools
from lltk.text import Text,text_plain_from_xml,clean_text
from lltk.corpus import PATH_CORPUS

BAD={'figdesc','head','edit','note','l'}
OK={'p'}

class TextChadwyck(Text):

	@property
	def genre(self): return 'Fiction'


	@property
	def nation(self):
		return 'American' if 'America' in self.id else 'British'

	def text_plain_from_xml(self,txt=None,**attrs):
		if not txt: txt=self.text_xml
		return text_plain_from_xml(txt,body_tag='doc',BAD=BAD,OK=OK)



def split_raw_xml_into_chapters(xml_str,chapter_tags=['div5','div4','div3','div2','div1','div0'],para_tag='p',verse_tag='l',keep_verse=True):
	import bs4,tools
	from collections import defaultdict
	txt=xml_str
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

	for chap_i,xml_chapter in enumerate(dom(chapter_tag)):
		if xml_chapter is None: continue
		xml_str=str(xml_chapter)
		name=xml_str.split('</collection>')[1].split('<attbytes')[0].strip()
		chapter_meta = {'chap_num':chap_i+1}
		chapter_meta['chap_idref']=xml_chapter('idref')[0].text
		chapter_meta['chap_name']=name

		for level,ptag in sorted(parent_tags.items()):
			parent=xml_chapter.find_parent(ptag)

			if parent:
				for k,v in list(parent.attrs.items()):
					#print '>>',level,ptag,k,v
					chapter_meta[ptag+'_num' if '_num' not in k else k]=v

	
		## make txt
		chapter=[]
		for xml_para in xml_chapter(para_tag):
			if not xml_para: continue
			para=clean_text(xml_para.text).replace('\n',' ')
			while '  ' in para: para=para.replace('  ',' ')
			if para: chapter+=[para]
		chapter_txt='\n\n'.join(chapter)

		# yield meta, xml, txt
		yield(chapter_meta,str(xml_chapter),chapter_txt)



def get_meta_from_raw_xml(xml_str,xml_fn):
	md={}
	for line in xml_str.split('\n'):
		#if '<doc>' in line: break

		if '<T1>' in line and not 'title' in md:
			md['title']=line.split('<T1>')[1].split('</T1>')[0]
		if '<ID>' in line and not 'idref' in md:
			md['idref']=line.split('<ID>')[1].split('</ID>')[0]
		if '<A1>' in line and not 'author' in md:
			md['author']=line.split('<A1>')[1].split('</A1>')[0]
		if '<Y1>' in line and not 'year' in md:
			md['year']=line.split('<Y1>')[1].split('</Y1>')[0]
		if '<PBL>' in line and not 'pub' in md:
			md['pub']=line.split('<PBL>')[1].split('</PBL>')[0]
		if '<TY>' in line and not 'type' in md:
			md['type']=line.split('<TY>')[1].split('</TY>')[0]
		if '<body>' in line: break

	if 'America' in xml_fn:
		md['nation']='American'
	else:
		md['nation']='British'
	md['medium']='Fiction'
	md['subcorpus']=xml_fn.split(os.path.sep)[-2]
	md['fn_raw']=os.path.sep.join(xml_fn.split(os.path.sep)[-2:])
	return md

def compile_text(fnfn):
	import json
	with open(fnfn) as f:
		xml_str=f.read()

		# overall meta
		book_meta = get_meta_from_raw_xml(xml_str,fnfn)
		# print(book_meta)

		for chapter_meta,chapter_xml,chapter_txt in split_raw_xml_into_chapters(xml_str):
			# print(chapter_meta)
			all_meta = {**book_meta, **chapter_meta}

			# get id
			idhash = tools.hash(all_meta['chap_idref'])[:10]
			all_meta['id']=idhash
			path_l=tools.slice(idhash,slice_length=2)
			all_meta['path']='/'.join(path_l)

			# write meta, xml, txt
			path_text = os.path.join(PATH_CORPUS,Chadwyck.ID,'texts',*path_l)
			if not os.path.exists(path_text): os.makedirs(path_text)
			path_meta = os.path.join(path_text, 'meta.json')
			path_txt = os.path.join(path_text, 'text.txt')
			path_xml = os.path.join(path_text, 'text.xml')

			with open(path_meta,'w') as of: json.dump(all_meta, of)
			with open(path_txt,'w') as of: of.write(chapter_txt)
			with open(path_xml,'w') as of: of.write(chapter_xml)


### CORPUS CLASS
from lltk.corpus import Corpus
class Chadwyck(Corpus):
	ID='chadwyck'
	TEXT_CLASS=TextChadwyck
	PATH_TXT = 'chadwyck/_txt_chadwyck'
	PATH_XML = 'chadwyck/_xml_chadwyck'
	PATH_METADATA = 'chadwyck/corpus-metadata.Chadwyck.xlsx'
	EXT_XML='.new'
	EXT_TXT='.txt'

	def __init__(self):
		super(Chadwyck,self).__init__('Chadwyck',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML,ext_txt=self.EXT_TXT)
		self.path = os.path.dirname(__file__)

	# compile from raw
	def compile(self,raw_ext='.new',**y):
		# make sure I have it
		self.compile_get_raw()

		# walk
		fnfns=[]
		for root, dirs, files in os.walk(self.path_raw, topdown=False):
			for fn in files:
				# print(fn)
				if not fn.endswith(raw_ext): continue
				fnfn=os.path.join(root,fn)
				fnfns+=[fnfn]
		
		from tqdm import tqdm
		import json
		from p_tqdm import p_map
		p_map(compile_text,fnfns,num_cpus=2)
		






def sens_sens():
	return Chadwyck().textd['Nineteenth-Century_Fiction/ncf0204.08']
