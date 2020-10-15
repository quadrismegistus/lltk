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



# def split_raw_xml_into_chapters(xml_str,chapter_tags=['div5','div4','div3','div2','div1','div0'],para_tag='p',verse_tag='l',keep_verse=True,modernize_spelling=True):
# 	import bs4,tools
# 	from collections import defaultdict
# 	txt=xml_str
# 	done=0
# 	parent_tags={}
# 	chapter_tag=None

# 	for ctag in chapter_tags:
# 		txt=txt.replace(f'<{ctag}>','<chapter>').replace('</{ctag}>','</chapter>')

# 	chapter_tags=['chapter']
# 	chapter_tag='chapter'
# 	# for ctag in chapter_tags:
# 	# 	if '</'+ctag+'>' in txt:
# 	# 		if not chapter_tag:
# 	# 			chapter_tag=ctag
# 	# 		parent_tags[len(parent_tags)]=ctag
# 	# #print chapter_tag,done,parent_tags
# 	dom=bs4.BeautifulSoup(txt,'lxml')
# 	# # numretu
# 	# for ctag in list(parent_tags.values()):
# 	# 	for i,ctagx in enumerate(dom(ctag)):
# 	# 		ctagx['num']=i+1
			
# 	# for child_i,child in enumerate(chapter_tags):
# 	# 	immediate_parents=chapter_tags[child_i+1:]
# 	# 	#print 'xx',child_i,child,immediate_parents
# 	# 	for ptagi,ptag in enumerate(immediate_parents):
# 	# 		for pi,parentx in enumerate(dom(ptag)):
# 	# 			#print 'parent',pi,ptag,child,len(parentx)
# 	# 			for ci,ctagx in enumerate(parentx(child)):
# 	# 				#print 'chap',ci,len(ctagx)
# 	# 				#ctagx['num_in_parent%s' % str(ptagi+1)]=ci+1
# 	# 				ctagx['num_%s_in_%s' % (child,ptag)]=ci+1
# 	# 				#print ctagx.attrs


# 	if keep_verse:
# 		for tag in dom(verse_tag):
# 			tag.name=para_tag

# 	for tag in BAD: [x.extract() for x in dom.findAll(tag)]

# 	# if modernize_spelling:
# 		# spelling_d=tools.get_spelling_modernizer()

# 	# print(chapter_tag)
# 	for chap_i,xml_chapter in enumerate(dom(chapter_tag)):
# 		if xml_chapter is None: continue
# 		xml_str=str(xml_chapter)
# 		try:
# 			name=xml_str.split('<attbytes>')[0].split('\n')[1].strip()
# 		except IndexError:
# 			name='?'
# 			print(xml_str,'??????')
# 		chapter_meta = {'num':chap_i+1}
# 		chapter_meta['idref']=xml_chapter('idref')[0].text
# 		chapter_meta['name']=name

# 		for level,ptag in sorted(parent_tags.items()):
# 			parent=xml_chapter.find_parent(ptag)

# 			if parent:
# 				for k,v in list(parent.attrs.items()):
# 					#print '>>',level,ptag,k,v
# 					chapter_meta[ptag+'_num' if '_num' not in k else k]=v

	
# 		## make txt
# 		chapter=[]
# 		for xml_para in xml_chapter(para_tag):
# 			if not xml_para: continue
# 			para=clean_text(xml_para.text).replace('\n',' ')
# 			while '  ' in para: para=para.replace('  ',' ')
# 			if para: chapter+=[para]
# 		chapter_txt='\n\n'.join(chapter)

# 		# modernize?
# 		# if modernize_spelling:
# 			# speling_
# 			# chapter_txt=tools.modernize_spelling_in_txt(chapter_txt,spelling_d)

# 		# yield meta, xml, txt
# 		yield(chapter_meta,str(xml_chapter),chapter_txt)

def test_split():
	raw=open(os.path.join(PATH_CORPUS,Chadwyck.ID,'raw','Eighteenth-Century_Fiction','sterne.01.new')).read()
	for x in split_raw_xml_into_chapters(raw):
		print(x)
		print('\n\n')


def split_raw_xml_into_chapters(xml_str,chapter_tags=['div5','div4','div3','div2','div1','div0'],para_tag='p',verse_tag='l',keep_verse=True,modernize_spelling=True):
	import bs4,tools
	from collections import defaultdict
	dom=bs4.BeautifulSoup(xml_str,'lxml')
	if keep_verse:
		for tag in dom(verse_tag):
			tag.name=para_tag
	for tag in BAD: [x.extract() for x in dom.findAll(tag)]
	was = dict((i,False) for i in range(7))


	xml_chapters=[]
	divs_0 = list(dom('div000'))
	if not divs_0: divs_0=[dom]
	else: was[0]=True

	for ctag in dom('div0'): ctag.name='div1'
	for ctag in dom('div1'): ctag.name='div1'
	for ctag in dom('div2'): ctag.name='div1'
	for ctag in dom('div3'): ctag.name='div1'
	for ctag in dom('div4'): ctag.name='div1'
	for ctag in dom('div5'): ctag.name='div1'
		

	for i_div0,div0 in enumerate(divs_0):
		divs_1 = list(div0('div1'))
		if not divs_1: divs_1=[div0]
		else: was[1]=True
		# print(divs_1,'!?!?!?!?')

		for i_div1,div1 in enumerate(divs_1):
			# print(i_div1,div1)
			# print('\n\n^^')

			divs_2 = list(div1('div2'))
			if not divs_2: divs_2=[div1]
			else: was[2]=True

			for i_div2,div2 in enumerate(divs_2):
				divs_3 = list(div2('div3'))
				if not divs_3: divs_3=[div2]
				else: was[3]=True

				for i_div3,div3 in enumerate(divs_3):
					divs_4 = list(div3('div4'))
					if not divs_4: divs_4=[div3]
					else: was[4]=True

					for i_div4,div4 in enumerate(divs_4):
						divs_5 = list(div4('div5'))
						if not divs_5: divs_5=[div4]
						else: was[5]=True

						for i_div5,div5 in enumerate(divs_5):
							# print(i_div0,i_div1,i_div2,i_div3,i_div4,i_div5)
							index_l=[i_div0,i_div1,i_div2,i_div3,i_div4,i_div5]
							index=tuple([ix for ii,ix in enumerate(index_l) if was[ii]])
							xml_chapters+=[(index,div5)]


				# print(div2)
				# print('\n\n')
	# stop


	for chap_i,(chap_index,xml_chapter) in enumerate(xml_chapters):
		if xml_chapter is None: continue
		xml_str=str(xml_chapter)
		try:
			name=xml_str.split('<attbytes>')[0].split('</collection>')[1].strip()
		except IndexError:
			name='?'
			# print(xml_str,'??????')
		chapter_meta = {'num':chap_i+1}
		try:
			chapter_meta['idref']=xml_chapter('idref')[0].text
		except IndexError:
			continue
		chapter_meta['name']=name
		chapter_meta['index']='.'.join(str(x+1) for x in chap_index)
	
		## make txt
		chapter=[]
		chapter_xml=[]
		for xml_para in xml_chapter(para_tag):
			if not xml_para: continue
			para=clean_text(xml_para.text).replace('\n',' ')
			while '  ' in para: para=para.replace('  ',' ')
			if para: chapter+=[para]

			xml_para.name='p'
			para_xml=clean_text(str(xml_para)).replace('\n',' ')
			while '  ' in para_xml: para_xml=para_xml.replace('  ',' ')
			if para_xml: chapter_xml+=[para_xml]

		chapter_txt='\n\n'.join(chapter)
		chapter_xml='\n\n'.join(chapter_xml)
		if chapter_xml:
			chapter_xml=f'''<TEI xmlns="http://www.tei-c.org/ns/1.0">
<text>
<body>
{chapter_xml}
</body>
</text>
</TEI>
'''
			# minidom=bs4.BeautifulSoup(chapter_xml, 'xml')
			# chapter_xml=minidom.prettify()
		
		chapter_raw=xml_str

		# modernize?
		# if modernize_spelling:
			# speling_
			# chapter_txt=tools.modernize_spelling_in_txt(chapter_txt,spelling_d)

		# yield meta, xml, txt
		yield(chapter_meta,chapter_raw,chapter_xml,chapter_txt)



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
		if '<attbytes>' in line and not 'name' in md:
			md['name']=line.split('<attbytes>')[0].strip()
		if '</comcitn>' in line: break


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
	chapters_meta=[]
	with open(fnfn) as f:
		xml_str=f.read()

		# overall meta
		book_meta = get_meta_from_raw_xml(xml_str,fnfn)
		if not book_meta.get('idref'):
			print('missing id:',book_meta)
			return []
		
		# print(book_meta)
		author=book_meta.get('author','Unknown')
		if ' ' in author:
			author_l = author.strip().split()
			author = tools.noPunc(author_l[0])
			# dob=author_l[-1].split('-')[0]
			# if dob.isdigit(): author+='_'+str(dob)
		else:
			author = tools.noPunc(author)

		title = book_meta.get('title','Unknown').split('.')[0].split(';')[0].split(':')[0].split('(')[0]
		title=title.strip()
		if title.startswith('A '): title=title[2:]
		if title.startswith('The '): title=title[4:]
		title=title.replace(' ','_')

		path_l_book=[
			# book_meta.get('subcorpus'),
			author,
			title[:50],
			book_meta.get('idref')
		]
		book_meta['id']='/'.join(path_l_book)

		# save book meta
		path_book=os.path.join(PATH_CORPUS,Chadwyck.ID,'texts',*path_l_book)
		if not os.path.exists(path_book): os.makedirs(path_book)
		
		path_meta_book = os.path.join(path_book,'meta.json')
		with open(path_meta_book,'w') as of: json.dump(book_meta,of,indent=4, sort_keys=True)

		for chapter_meta,chapter_raw,chapter_xml,chapter_txt in split_raw_xml_into_chapters(xml_str):
			# print(chapter_meta)
			all_meta = {**book_meta, **chapter_meta}

			# get id
			#idhash = tools.hash(Chadwyck.ID + '/' + all_meta['chap_idref'])[:10]
			#all_meta['id']=idhash
			#path_l=tools.slice(idhash,slice_length=2)
			#all_meta['path']='/'.join(path_l)
			path_l=[
				*path_l_book,
				# str(chapter_meta.get('num',0)).zfill(4)
				*[str(x).zfill(3) for xi,x in enumerate(chapter_meta.get('index').split('.'))],
				# chapter_meta.get('idref'),
				# str(chapter_meta.get('div2_num',0)).zfill(2),
				# str(chapter_meta.get('div3_num',0)).zfill(2),
			]
			chapter_meta['id']='/'.join(path_l)


			# write meta, xml, txt
			path_text = os.path.join(PATH_CORPUS,Chadwyck.ID,'texts',*path_l)
			if not os.path.exists(path_text): os.makedirs(path_text)
			path_meta = os.path.join(path_text, 'meta.json')
			path_txt = os.path.join(path_text, 'text.txt')
			path_raw = os.path.join(path_text, 'text.raw')
			path_xml = os.path.join(path_text, 'text.xml')

			if os.path.exists(path_meta):
				raise Exception('!! already exists!',path_meta,book_meta)
				

			# with open(path_meta,'w') as of: json.dump(all_meta, of, indent=4)
			with open(path_meta,'w') as of: json.dump(chapter_meta, of, indent=4, sort_keys=True)
			with open(path_txt,'w') as of: of.write(chapter_txt)
			with open(path_raw,'w') as of: of.write(chapter_raw)
			with open(path_xml,'w') as of: of.write(chapter_xml)

			chapters_meta+=[all_meta]
	
	return chapters_meta


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
		import sys
		sys.setrecursionlimit(1000000)

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
		
		# print(fnfns)
		# return
		from tqdm import tqdm
		import json
		from p_tqdm import p_umap
		res=p_umap(compile_text,fnfns,num_cpus=4)
		all_meta=[chap_meta for meta in res for chap_meta in meta]
		tools.write2(self.path_metadata,all_meta)




def sens_sens():
	return Chadwyck().textd['Nineteenth-Century_Fiction/ncf0204.08']
