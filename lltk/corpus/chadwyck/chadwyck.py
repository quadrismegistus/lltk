from lltk.imports import *

BAD={'figdesc','head','edit','note','l'}
OK={'p'}


def xml2txt_chadwyck(xml):
	return xml2txt(xml,
		OK=OK,
		BAD=BAD,
		body_tag='doc'
	)


class TextChadwyck(Text): pass


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
	
	for ctag in dom('div0'): ctag.name='div1'
	for ctag in dom('div1'): ctag.name='div1'
	for ctag in dom('div2'): ctag.name='div1'
	for ctag in dom('div3'): ctag.name='div1'
	for ctag in dom('div4'): ctag.name='div1'
	for ctag in dom('div5'): ctag.name='div1'

	divs = list(dom('div1'))
	if not divs: divs=[dom]
	for i_div,div in enumerate(divs):
		xml_chapters+=[(i_div,div)]

	for chap_i,(chap_index,xml_chapter) in enumerate(xml_chapters):
		if xml_chapter is None: continue
		xml_str=str(xml_chapter)
		try:
			name=xml_str.split('<attbytes>')[0].split('</collection>')[1].strip()
		except IndexError:
			name='?'
		chapter_meta = {} #'num':chap_i+1}
		try:
			chapter_meta['idref']=xml_chapter('idref')[0].text
		except IndexError:
			continue
		chapter_meta['name']=name
		chapter_meta['index']=chap_index#='.'.join(str(x+1) for x in chap_index)
	
		## make txt
		chapter=[]
		chapter_xml=[]
		for xi,xml_para in enumerate(xml_chapter(para_tag)):
			if not xml_para: continue
			para=clean_text(xml_para.text).replace('\n',' ')
			while '  ' in para: para=para.replace('  ',' ')
			if para: chapter+=[para]

			xml_para.name='p'
			xml_para.attrs['id']=xi
			para_xml=clean_text(str(xml_para)).replace('\n',' ')
			while '  ' in para_xml: para_xml=para_xml.replace('  ',' ')
			if para_xml: chapter_xml+=[para_xml]

		chapter_txt='\n\n'.join(chapter)
		chapter_xml='\n\n'.join(chapter_xml)
		# if chapter_xml:
# 			chapter_xml=f'''<TEI xmlns="http://www.tei-c.org/ns/1.0">
# <text>
# <body>
# {chapter_xml}
# </body>
# </text>
# </TEI>
# '''
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

# os.path.join(PATH_CORPUS,Chadwyck.ID,'texts',

def compile_text(obj):
	fnfn,idx,path_corpus = obj
	old=[]
	with open(fnfn) as f:
		xml_str=f.read()
		# overall meta
		book_meta = get_meta_from_raw_xml(xml_str,fnfn)
		if not book_meta.get('idref'):
			print('missing id:',book_meta)
			return []
		book_meta['id']=idx
		path_xml = os.path.join(path_corpus, 'xml', idx+'.xml')
		opath = os.path.dirname(path_xml)
		if not os.path.exists(opath):
			try:
				os.makedirs(opath)
			except FileExistsError:
				pass
		with open(path_xml,'w') as of: of.write(xml_str)
		old.append(book_meta)
	return old
	


### CORPUS CLASS
from lltk.corpus.corpus import Corpus
class Chadwyck(Corpus):
	TEXT_CLASS=TextChadwyck
	XML2TXT=xml2txt_chadwyck

	# compile from raw
	def compile(self,raw_ext='.new',**y):
		import pandas as pd
		# make sure I have it
		self.compile_download()
		# walk
		fnfns=[]
		ids=[]
		for (root,dirs,files) in os.walk(self.path_raw, topdown=False):
			for fn in files:
				if not fn.endswith(raw_ext): continue
				fnfn=os.path.join(root,fn)
				fnfns+=[fnfn]
				ids+=[os.path.splitext(fnfn.split('/raw/')[-1])[0]]
		objs = [
			(fnfn,idx,self.path_home)
			for fnfn,idx in zip(fnfns,ids)
		]
		res_lld=tools.pmap(compile_text,objs,num_proc=DEFAULT_NUM_PROC)
		res_ld = [d for ld in res_lld for d in ld]
		res_df = pd.DataFrame(res_ld).set_index('id')
		res_df = fix_meta(res_df)
		
		# Some adds/fixes
		res_df['nation']=res_df.reset_index().id.apply(lambda idx: 'American' if 'America' in idx else 'British')
		
		save_df(res_df, self.path_metadata)
		# res_df=pd.DataFrame(res_ld)
		return res_df

def sens_sens():
	return Chadwyck().textd['Nineteenth-Century_Fiction/ncf0204.08']
