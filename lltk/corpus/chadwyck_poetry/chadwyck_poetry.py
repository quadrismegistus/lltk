# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function
import six
from six.moves import zip
STONES = ['save_txt_from_xml'] # for slingshot

META_KEYS = ['_path', 'a1', 'alias', 'aliasinv', 'anote', 'argument', 'attauth', 'attautid', 'attbytes', 'attdbase', 'attgend', 'attgenre', 'attidref', 'attnatn', 'attperi', 'attpoet', 'attpubl', 'attpubn1', 'attpubn2', 'attrhyme', 'attsize', 'attview', 'audclip', 'audio', 'authdtls', 'author', 'author_dob', 'author_dod', 'author_gender', 'bnote', 'bo', 'break', 'bytes', 'caesura', 'caption', 'cell', 'chid', 'collection', 'conclude', 'dedicat', 'engcorp2', 'epigraph', 'epilogue', 'figure', 'firstl', 'gap', 'greek', 'hi', 'hideinft', 'id', 'idref', 'img', 'it', 'item', 'l', 'label', 'lacuna', 'lb', 'litpack', 'mainhead', 'note', 'num_lines', 'p', 'pb', 'pbl', 'pndfig', 'poemcopy', 'posthumous', 'preface', 'prologue', 'publish', 'reflink', 'removed', 'signed', 'sl', 'somauth', 'sombiog', 'sompoet', 'speaker', 'stage', 'sub', 'subhead', 'sup', 't1', 't2', 't3', 'target', 'title', 'title_volume', 'trailer', 'ty', 'u', 'usonly', 'video', 'volhead', 'xref', 'y1', 'yeayear_new', 'year_old', 'idz']

### TEXT CLASS
import codecs,os
from lltk import tools
from lltk.text.text import Text
from lltk.tools import get_spelling_modernizer,modernize_spelling_in_txt

STANZA_TAGS = ['stanza','versepara','pdiv']
LINE_TAGS = ['l','lb']

spelling_d = None


class TextChadwyckPoetry(Text):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']

	@property
	def genre(self): return 'Poetry'

	#def get_id(self):
	#	return self.meta['id'] if '/' in self.meta['id'] else self.path_xml.replace(self.corpus.path_xml,'').replace(self.corpus.ext_xml,'')

	@property
	def path(self):
		return self.path_xml

	@property
	def year(self):
		return self.year_author_is_30

	# @property
	# def meta(self,force_author_dob=True,plus_years=30):
	# 	meta=super(TextChadwyckPoetry,self).meta
	# 	meta['year']
	# 	try:
	# 		meta['year']=int(str(meta['author_dob'])[:4])+plus_years
	# 	except ValueError:
	# 		meta['year']=0
	# 	return meta

	@property
	def meta_by_file(self):
		return meta_by_file(self.text_xml)

	@property
	def meta_by_file_v1(self):
		md={}
		num_lines=0
		num_stanzas=0

		for line in self.lines_xml():
			#if '<doc>' in line: break
			if '<T1>' in line: md['title']=line.split('<T1>')[1].split('</T1>')[0].strip()
			if '<t1>' in line: md['title']=line.split('<t1>')[1].split('</t1>')[0].strip()
			if '<A1>' in line: md['author']=line.split('<A1>')[1].split('</A1>')[0].strip()
			if '<a1>' in line: md['author']=line.split('<a1>')[1].split('</a1>')[0].strip()
			if '<Y1>' in line: md['year']=line.split('<Y1>')[1].split('</Y1>')[0].strip()
			if '<y1>' in line: md['year']=line.split('<y1>')[1].split('</y1>')[0].strip()
			if '<T2>' in line: md['title_volume']=line.split('<T2>')[1].split('</T2>')[0].strip()
			if '<t2>' in line: md['title_volume']=line.split('<t2>')[1].split('</t2>')[0].strip()
			if '<ID>' in line: md['idz']=line.split('<ID>')[1].split('</ID>')[0].strip()
			if '<id>' in line: md['idz']=line.split('<id>')[1].split('</id>')[0].strip()
			for lntag in self.LINE_TAGS:
				if '<'+lntag+'>' in line or '</'+lntag+'>' in line:
					num_lines+=1
			for stanzatag in self.STANZA_TAGS:
				if stanzatag in line:
					num_stanzas+=1


		if not 'modern/' in self.id and not 'faber' in self.id:
			if 'american' in self.id:
				md['nation']='American'
			else:
				md['nation']='British'
		md['medium']='Verse'
		md['genre']='Poetry'
		md['id']=self.id
		md['num_lines']=num_lines
		md['num_stanzas']=num_stanzas
		md['subcorpus']=self.id.split('/')[0]
		md['author_id']=self.id.split('/')[1]
		md['text_id']=self.id.split('/')[1]

		return md


	# from /home/users/heuser/workspace/jupyter/prosodic_chadwyck/prosodic_parser.py
	def text_plain(self, OK=['l','lb'], BAD=['note','edit'], body_tag='poem', line_lim=None, modernize_spelling=True):
		#if not self.exists: return ''
		if os.path.exists(self.fnfn_txt):
			print('>> text_plain from stored text file:',self.fnfn_txt)
			return tools.read(self.fnfn_txt)

		if self.fnfn_xml and os.path.exists(self.fnfn_xml):
			return xml2txt(self.fnfn_xml, OK=OK,BAD=BAD,body_tag=body_tag,line_lim=line_lim,modernize_spelling=modernize_spelling)

		return ''

	#@property
	#def path_txt(self):
	#	# @HACK!!!
	#	return self.path_xml



### CORPUS CLASS
from lltk.corpus.corpus import Corpus
import os,codecs,re
from lltk import tools

class ChadwyckPoetry(Corpus):
	"""
	Steps taking in bringing this corpus from raw to refined.

	from lltk.corpus.chadwyck_poetry import ChadwyckPoetry
	corpus = ChadwyckPoetry()
	corpus.gen_xml()
	corpus.tokenize_texts()
	corpus.save_metadata()
	corpus.gen_mfw(yearbin=50,year_min=1600,year_max=2000)
	corpus.gen_freq_table()
	"""

	TEXT_CLASS=TextChadwyckPoetry
	EXT_XML='.xml'
	MIN_YEAR=1500
	MAX_YEAR=2020	

	# def __init__(self):
	# 	super(ChadwyckPoetry,self).__init__('ChadwyckPoetry',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML)
	# 	self.path = os.path.dirname(__file__)

	def gen_xml(self, split_start='<poem>', split_end='</poem>', id_split_start='<ID>', id_split_end='</ID>'):
		"""
		Mapping from raw to this corpus's data.
		Will generate XML snippets from raw data, one per poem, stored in author folders.
		"""
		import multiprocessing as mp

		objects = []

		for subcorpus in os.listdir(self.PATH_RAW):
			subpath = os.path.join(self.PATH_RAW, subcorpus)
			opath = os.path.join(self.path_xml, subcorpus)
			for author_fn in os.listdir(subpath):
				author_fnfn=os.path.join(subpath,author_fn)
				if not author_fnfn.endswith('.new'): continue

				objects+=[(author_fnfn,opath)]


		pool=mp.Pool()
		pool.map(save_poems_from_raw_author_folder, objects)

	# def save_metadata(self,save=True,nproc=1,
	# 						guess_gender=False,detect_posthumous=False,fudge_dates=False):
	# 	texts=self.texts()
	# 	if nproc>1:
	# 		paths=[t.path_xml for t in texts]
	# 		import multiprocessing as mp
	# 		pool=mp.Pool(nproc)
	# 		old=[]
	# 		num=0
	# 		for i,metadx in enumerate(pool.imap_unordered(meta_by_file, paths)):
	# 			if not i % 100: print('>>',i,'...')
	# 			old+=[metadx]
	# 	else:
	# 		for i,t in enumerate(texts):
	# 			if not i%10: print('>> processing text #',i,'...')
	# 			metad=meta_by_file(t)
	# 			old+=[metad]
	#
	# 	# add Id
	# 	for metad,text in zip(old,texts):
	# 		metad['idz']=metad['id']
	# 		metad['id']=text.id
	#
	# 	timestamp=tools.now().split('.')[0]
	# 	tools.write2(os.path.join(self.path,'corpus-metadata.%s.%s.txt' % (self.name,timestamp)), old)


	## word2vec
	def word2vec_by_period(self,year_min=1500,year_max=2000,**attrs):
		return super(ChadwyckPoetry,self).word2vec_by_period(year_min=year_min,year_max=year_max,**attrs)

	def load_metadata(self,*x,**y):
		import numpy as np
		meta=super().load_metadata()
		meta['genre']='Verse'
		#meta['year']=meta.author_dob.apply(lambda x: int(x)+30 if x.isdigit() else np.nan)
		meta['year']=meta.apply(decide_year_from_dob_and_dod,1)
		return meta#.query(f'{self.MIN_YEAR}<=year<{self.MAX_YEAR}')


def decide_year_from_dob_and_dod(row,addby=25):
	import numpy as np

	# has a dob?
	try:
		dob=int(row.author_dob)
	except ValueError:
		return np.nan
	
	# has a dod?
	try:
		dod=int(row.author_dod)
	except ValueError:
		return dob+addby
	
	# died before 25?
	lifetime = dod - dob
	if lifetime < addby:
		return dod

	# pub in lifetime?
	try:
		y1=int(row.y1)
		if dob<y1<dod:
			return y1
	except ValueError:
		pass
	# # otherwise mid point?
	# midpoint = lifetime // 2
	# return dob + midpoint
	return dob+addby




class ChadwyckPoetrySample(ChadwyckPoetry):
	def __init__(self):
		super(ChadwyckPoetrySample,self).__init__('ChadwyckPoetrySample',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA,ext_xml=self.EXT_XML)
		self.path = os.path.dirname(__file__)

	def gen(self,n_texts=100,per_n_years=10):
		"""
		Logic of this sample:
		Just get everything posthumous.
		"""
		pass









## FUNCTIONS

def save_poems_from_raw_author_folder(xxx_todo_changeme, use_bs4=False):
	(author_fnfn, opath) = xxx_todo_changeme
	import bs4
	author_fn=os.path.split(author_fnfn)[-1]
	author_id = author_fn.split('.')[0]
	opath = os.path.join(opath, author_id)
	if not os.path.exists(opath): os.makedirs(opath)

	print('>>',opath,'...')
	with codecs.open(author_fnfn,encoding='latin1') as f:
		txt=f.read()
		txt=six.text_type(txt.replace('\r\n','\n').replace('\r','\n'))
		if use_bs4:
			dom = bs4.BeautifulSoup(txt,'lxml')
			for poem_i,poem in enumerate(dom('poem')):
				poem_num=poem_i+1
				ids=poem('id')
				if not ids: continue
				idx=ids[0].text
				if not idx: continue
				poem_xml_str = six.text_type(poem)

				ofnfn = os.path.join(opath, idx+'.xml')
				with codecs.open(ofnfn,'w',encoding='utf-8') as of:
					of.write(poem_xml_str)
		else:
			txt=txt.replace('<poem ','<poem>')
			for poem in txt.split('<poem>')[1:]:
				idx=None
				try:
					poem = '<poem>'+ poem.split('</poem>')[0]+'</poem>'
					idx=poem.split('</ID>')[0].split('<ID>')[1]
				except IndexError as e:
					pass
				if not idx or not poem: continue
				poem_xml_str = poem

				ofnfn = os.path.join(opath, idx+'.xml')
				with codecs.open(ofnfn,'w',encoding='utf-8') as of:
					of.write(poem_xml_str)
			#print '>> saved:',ofnfn



####

# Functions



def save_txt_from_xml(xml_path,results_dir='./', modernize_spelling=True):
	txt = xml2txt(xml_path, modernize_spelling=modernize_spelling)
	ofn = xml_path if xml_path[0]!=os.path.sep else xml_path[1:]
	ofn = ofn.replace('.xml','.txt')
	ofnfn = os.path.join(results_dir,ofn)
	opath = os.path.dirname(ofnfn)
	try:
		if not os.path.exists(opath): os.makedirs(opath)
	except OSError:
		pass
	#with codecs.open(ofnfn,'w',encoding='utf-8') as of:
	with open(ofnfn,'w') as of:
		of.write(txt)
		print('>> saved:',ofnfn)

# from /home/users/heuser/workspace/jupyter/prosodic_chadwyck/prosodic_parser.py
def xml2txt(xml_path, xml_string=None, OK=['l','lb'], BAD=['note'], body_tag='poem', line_lim=None, modernize_spelling=False):
	global spelling_d
	import bs4 #,codecs
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']
	REPLACEMENTS={
		'&indent;':'    ',
		'&hyphen;':'-',
		u'\u2014':' -- ',
		u'\u2013':' -- ',
		u'\u2018':"'",
		u'\u2019':"'",
		u'\xc9':'E', #u'É',
		u'\xe9':'e', #u'é',
		u'\xc8':'E',#u'È',
		u'\u201d':'"',
		u'\u201c':'"',
	}
	txt=[]
	if not xml_string:
		#with codecs.open(xml_path,encoding='utf-8') as f:
		with open(xml_path,errors='ignore') as f:
			xml_string = f.read()
	dom = bs4.BeautifulSoup(xml_string,'lxml')

	for tag in BAD:
		[x.extract() for x in dom.findAll(tag)]

	# standardize lines:
	for tag in LINE_TAGS:
		if tag=='l': continue
		for ent in dom(tag):
			ent.name='l'

	## replace stanzas
	num_stanzas=0
	for tag in STANZA_TAGS:
		for ent in dom(tag):
			ent.name='stanza'
			num_stanzas+=1
	txt=[]
	num_lines=0
	if not num_stanzas:
		for line in dom('l'):
			txt+=[line.text.strip()]
			num_lines+=1
			if line_lim and num_lines>=line_lim: break
	else:
		for stanza in dom('stanza'):
			for line in stanza('l'):
				txt+=[line.text.strip()]
				num_lines+=1
			if line_lim and num_lines>=line_lim: break
			txt+=['']

	txt=txt[:line_lim]
	txt='\n'.join(txt).replace(u'∣','').strip()
	for k,v in list(REPLACEMENTS.items()):
		txt=txt.replace(k,v)

	if modernize_spelling:
		if not spelling_d: spelling_d=get_spelling_modernizer()
		txt = modernize_spelling_in_txt(txt,spelling_d)

	return txt

#def meta_by_file(path_xml,guess_gender=False,detect_posthumous=True,fudge_dates=True):
def meta_by_file(text_xml,guess_gender=False,detect_posthumous=False,fudge_dates=True):
	if guess_gender: import gender_guesser.detector as gender
	gender_d={}

	import bs4 #,codecs
	#with codecs.open(path_xml,encoding='utf-8',errors='ignore') as f: text_xml=f.read()
	#with open(path_xml,errors='ignore') as f:
	#	text_xml=f.read()

	dom=bs4.BeautifulSoup(text_xml,'lxml')

	metad={}
	for mk in META_KEYS: metad[mk]=''

	for tag in dom():
		#print tag.name,[tag.text]#[unicode(tag)]
		#tag_html=unicode(tag)
		tag_html=tag.decode_contents()
		if '</' in tag_html: continue # ignore nesting tags
		#print tag.name,[tag_html]
		if tag.name in metad and metad[tag.name]: continue
		metad[tag.name] = tag.text.strip()

	#metad['idz']=metad['id']
	#metad['id']=self.id

	# set standards
	metad['title']=metad.get('t1','')
	metad['author']=metad.get('a1','')
	metad['year']=metad.get('y1','')
	metad['title_volume']=metad.get('y2','')
	metad['num_lines']=sum([len(list(dom(linetag))) for linetag in LINE_TAGS])

	# metadata by induction
	if fudge_dates:
		try:
			metad['year']=int(metad['year'])
			dates_in_title = [int(x) for x in re.split('\W',metad['title']) if x.isdigit() and len(x)==4]
			for dt in dates_in_title:
				if dt<metad['year']:
					metad['year_old']=metad['year']
					metad['year']=metad['year_new']=dt

		except ValueError:
			pass

	if detect_posthumous:
		from lltk.text import get_author_dates
		metad['author_dob'],metad['author_dod']=get_author_dates(metad['author'])
		metad['posthumous']=''
		if metad['year'] and metad['author_dob'] and metad['year']>metad['author_dob']:
			if not metad['author_dod']:
				if metad['author_dob'] > 1900: # so it's plausible they're still alive
					metad['posthumous']=False
			else: # has death date
				if metad['year']>metad['author_dod']:
					metad['posthumous']=True
				else:
					metad['posthumous']=False

	if guess_gender:
		import gender_guesser.detector as gender

		if metad['author'] in gender_d:
			metad['author_gender']=gender_d[metad['author']]
		else:
			isnt_this_problematic_just_to=gender.Detector()
			genders = [isnt_this_problematic_just_to.get_gender(x) for x in re.split('\W',metad['author'])]
			for x in genders:
				if x!='unknown':
					metad['author_gender']=x
					gender_d[metad['author']]=x
					break
				else:
					metad['author_gender']='unknown'
					gender_d[metad['author']]='unknown'

	return metad
