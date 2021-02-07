# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import os,codecs,gzip,re
from collections import Counter
from lltk import tools
from six.moves import map
import six
from six.moves import zip
nlp=None
from smart_open import open
from collections import Counter
try:
	import ujson as json
except ImportError:
	import json

ENGLISH=None

ADDR_SEP='|'

class Text(object):
	def __init__(self,idx,corpus,path_to_xml=None,path_to_txt=None):
		self._id=idx
		self.corpus=corpus
		self._sections=[]
		self._fnfn_xml=path_to_xml
		self._fnfn_txt=path_to_txt

	@property
	def addr(self):
		if not hasattr(self,'_addr'):
			if self.corpus.id and self.corpus.id!='corpus':
				self._addr=str(self.corpus.id)+ADDR_SEP+str(self.id)
			else:
				self._addr=self.id
		return self._addr

	@property
	def col_id(self): return self.corpus.col_id

	@property
	def col_fn(self): return self.corpus.col_fn

	@property
	def fn(self): return self.meta.get(self.col_fn,'')

	@property
	def i(self):
		if hasattr(self,'_i'): return self._i
		return None

	@property
	def id(self):
		if not hasattr(self,'_id'):
			if self.fn:
				self._id=os.path.splitext(self.fn)[0]
			elif self.i is not None:
				self._id=str(self.i)

		return str(self._id)

	@property
	def nation(self):
		return self.meta.get('nation','')

	def sections_xml(self,divider_tag,text_section_class=None):
		xml=self.text_xml
		o=[]
		for part in xml.split('<'+divider_tag)[1:]:
			part_type=part.split(' TYPE="')[1].split('"')[0]
			part='<'+divider_tag+part
			o+=[(divider_tag,text_section_class,part_type,part)]
		return o

	def get_section(self,idx):
		for s in self.sections:
			if s.id==idx:
				return s

	def load_sections(self):
		o=[]
		for i,(divider_tag,text_section_class,section_type,section_xml) in enumerate(self.sections_xml()):
			if not text_section_class: text_section_class=TextSection
			o+=[text_section_class(text=self,section_num=i+1,section_type=section_type,section_xml=section_xml,section_txt=None,divider_tag=divider_tag)]
		self._sections=o

	def unload_sections(self):
		self._sections=[]

	@property
	def sections(self):
		if not hasattr(self,'_sections') or not self._sections:
			self.load_sections()
		return self._sections

	@property
	def is_metatext(self):
		return False

	@property
	def path_xml(self): return self.fnfn_xml

	@property
	def path_nlp(self): return os.path.splitext(self.path_txt.replace('/txt/','/corenlp/'))[0]+'.json'

	@property
	def path(self):
		#if os.path.exists(self.path_txt): return self.path_txt
		#if os.path.exists(self.path_xml): return self.path_xml
		#return ''
		return self.path_txt

	@property
	def ext_xml(self): return self.corpus.EXT_XML

	@property
	def ext_txt(self): return self.corpus.EXT_TXT

	@property
	def fnfn(self):
		return self.fnfn_xml if self.fnfn_xml and os.path.exists(self.fnfn_xml) else self.fnfn_txt

	@property
	def fnfn_txt(self):
		if hasattr(self,'_fnfn_txt') and self._fnfn_txt: return self._fnfn_txt
		#print(type(self.corpus.path_txt), type(self.id), type(self.ext_txt))
		try:
			return os.path.join(self.corpus.path_txt, self.id + self.ext_txt)
		except TypeError:
			if self.fn.endswith(self.ext_txt):
				return os.path.join(self.corpus.path_txt, self.fn)
		return ''

	@property
	def path_txt(self): return self.fnfn_txt

	@property
	def fnfn_xml(self):
		if hasattr(self,'_fnfn_xml') and self._fnfn_xml: return self._fnfn_xml
		try:
			return os.path.join(self.corpus.path_xml, self.id + self.ext_xml)
		except TypeError:
			if self.fn.endswith(self.ext_txt):
				return os.path.join(self.corpus.path_txt, self.fn)
		return ''

	@property
	def fnfn_spacy(self):
		return os.path.join(self.corpus.path_spacy, self.id + '.bin')

	@property
	def fnfn_parsed_spacy(self):
		path=self.corpus.path_parsed_spacy if hasattr(self.corpus,'path_parsed_spacy') else None
		if path: return os.path.join(path, self.id + '.txt')
		return ''

	@property
	def is_parsed_spacy(self):
		return (self.fnfn_parsed_spacy and os.path.exists(self.fnfn_parsed_spacy))

	@property
	def parsed_spacy(self,sep='\t'):
		#for dx in tools.readgen(self.fnfn_parsed_spacy):
		#	yield dx
		with codecs.open(self.fnfn_parsed_spacy,encoding='utf-8') as f:
			num_cols=None
			header=None
			num_cols=None
			for i,ln in enumerate(f):
				if header is None:
					header=ln.strip().split(sep)
					num_cols=len(header)
					continue
				dat=ln.strip().split(sep)
				if len(dat)<2:
					yield {'word':'\n'}
				elif len(dat)<num_cols:
					continue

				yield dict(list(zip(header,dat)))

	def all_tokens(self,modernize_spelling=False):
		"""words=[]
		for ln in self.txt.split('\n'):
			for w in ln.split(' '):
				a,b,c=tools.gleanPunc2(w)
				if a: words+=[a]
				words+=[b]
				if c: words+=[c]
				words+=[' ']
			words+=['\n']
		return words"""
		#return list(self.fast_tokens())
		import re
		txt=self.txt.replace('\n','\\')
		txt=txt.replace('&hyphen;','-')
		words=re.findall(r"[\w]+|[^\s\w]", txt)
		words=[w if w!='\\' else '\n' for w in words]

		try:
			if modernize_spelling:
				words=[self.corpus.modernize_spelling(w) for w in words]
		except AttributeError:
			pass

		return words


	def fast_counts_from_fn(self):
		from future_builtins import map
		from itertools import chain
		with codecs.open(path,encoding='utf-8',errors='ignore') as f:
			return Counter(chain.from_iterable(list(map(tokenize_fast, self.fnfn_txt))))

	def fast_tokens(self):
		for ln in self.lines_txt():
			for w in tokenize_fast(ln):
				yield w

	def fast_counts(self):
		return Counter(self.fast_tokens())




	# Corenlp ####
	def parse_corenlp(self,**opt):
		from lltk.model import corenlp
		corenlp.annotate_path(self.path_txt)

	


	#####


























	def html(self,word2fields={},bad_words={'[','Footnote'},bad_strs={'Kb'},fn_model_result=None,fn_model_coeffs=None,data={},only_words=set(),lim_words=None,modernize_spelling=False,stopwords=set()):

		if False: #self.is_parsed_spacy:
			html=self.html_spacy(word2fields=word2fields,bad_words=bad_words,bad_strs=bad_strs,fn_model_result=fn_model_result,fn_model_coeffs=fn_model_coeffs)

			return html,data

		from collections import Counter
		field_counts = {} #Counter()

		tags=[]
		#words=self.fast_tokens()
		#fields=' '.join(['field_'+field.replace('.','_') for field in word2fields.get(w,[])])
		for word in self.all_tokens(modernize_spelling=modernize_spelling):
			if lim_words and len(tags)>=lim_words: break
			word=word.replace('\n','<br/>')
			w=word.lower()
			if not w: continue

			if w[0].isalpha():
				# field_counts.update(word2fields.get(w,[]))
				wfields=word2fields.get(w,[])
				fieldstr=' '.join(['field_'+field.replace('.','_') for field in wfields])
				if (only_words and w not in only_words) or (stopwords and w in stopwords):
					tag=word
				elif fieldstr:
					# if w in word2fields: field_counts['has_field']+=1
					for fld in wfields:
						if not fld in field_counts: field_counts[fld]=Counter()
						field_counts[fld][w]+=1
					#field_counts.update(word2fields.get(w,[]))
					tag=f'<span class="{fieldstr} word">{word}</span>' #.format(fields=fields, word=word)
				else:
					tag=word
				tags+=[tag]
			elif tags:
				tags[-1]+=word
			else:
				tags+=[word]

		htmlx='\n'.join(tags)
		#htmlx=htmlx.replace('<br/><br/>','<br/>')
		return htmlx,field_counts


	def html_spacy(self,word2fields={},bad_words={'[','Footnote'},bad_strs={'Kb'},fn_model_result=None,fn_model_coeffs=None):
		import pandas as pd
		tags=['<p>']
		para=None
		dfr=None if not fn_model_result else pd.read_csv(fn_model_result,sep='\t',encoding='utf-8').set_index('_i')
		dfc=None if not fn_model_coeffs else pd.read_csv(fn_model_coeffs,sep='\t',encoding='utf-8').set_index('feat')

		#print dfr.index

		for dx in self.parsed_spacy:
			try:
				#print [dx['word']]
				word=dx['word'].replace('\n','<br/>')
			except KeyError:
				continue
			"""if word=='Footnote':
				# crap!
				tags.pop()
				continue
			if 'Kb' in word: # hack!!!
				tags.pop()
				word=word.split(']')[-1]
				if 'Kb' in word:
					continue
			"""

			_i=int(dx.get('_i',-1))
			w=word.lower()

			# punct
			if not w: continue
			if not w[0].isalnum():
				tags[-1]+=w
				continue


			classes=[]
			classes+=['field_'+field.replace('.','_') for field in word2fields.get(w,[])]
			if para!=dx.get('para',para):
				para=dx.get('para',para)
				tags+=['</p><p>']

			if dx.get('pos'): classes+=['pos_'+dx.get('pos')]
			if dx.get('dep'): classes+=['dep_'+dx.get('dep')]

			#print _i,w,_i in dfr.index,w in dfc.index,dx
			if dfr is not None and _i and _i in dfr.index:
				dfrx=dfr.loc[_i].to_dict()
				for kk,vv in list(dfrx.items()):
					if not type(vv) in {str,six.text_type}:
						vv=str(round(vv,1))
					classes+=[kk+'_'+vv]

				if dfrx['pred'] and dfrx['true']:
					classes+=['correct_'+str(dfrx['pred']==dfrx['true'])]

			feat='w_'+w

			if w and dfc is not None and feat in dfc.index:
				dfcx=dfc.loc[feat].to_dict()
				for kk,vv in list(dfcx.items()):
					if 'Unnamed' in kk: continue
					classes+=[kk+'_'+str(round(vv,1))]

			if classes:
				classes = [c.replace('.','_') for c in classes]
				tag=u'<span id="i_{i}" class="span_word {classes}" title="{classes}">{word}</span>'.format(i=_i, classes=' '.join(classes), word=word)
			else:
				tag=word
			tags+=[tag]
		tags+=['</p>']
		return u'\n'.join(tags)


	@property
	def genre(self): return self.meta.get('genre','')

	@property
	def medium(self):
		medium=self.meta.get('medium','')
		#if not medium: medium=self.meta.get('genre','')
		#if medium=='Verse': medium='Poetry'
		return medium

	@property
	def author(self): return self.meta.get('author','')

	def get_author_gender(self,name):
		# Guess
		import re
		import gender_guesser.detector as gender
		isnt_this_problematic_just_to=gender.Detector()
		genders = [isnt_this_problematic_just_to.get_gender(x) for x in re.split('\W',self.author)]
		for x in genders:
			if x!='unknown':
				return x
		return 'unknown'

	@property
	def author_gender(self):
		if 'gender' in self.meta and self.meta['gender']: return self.meta['gender']
		if 'author_gender' in self.meta and self.meta['author_gender']: return self.meta['author_gender']
		return self.get_author_gender(self.author)

	def get_is_posthumous(self,author,year):
		author_dates=self.get_author_dates(author)
		if year and author_dates[0] and year>author_dates[0]:
			if not author_dates[1]:
				if author_dates[0] > 1900: # so it's plausible they're still alive
					return False
			else: # has death date
				if year>author_dates[1]:
					return True
				else:
					return False
		return None


	@property
	def is_posthumous(self):
		return self.get_is_posthumous(self.author,self.year)



	@property
	def year(self):
		if type(self.meta['year']) in {int,float}:
			return int(self.meta['year'])

		try:
			yearstr=''.join([x for x in str(self.meta['year']) if x.isdigit()])[:4]
			while len(yearstr)<4:
				yearstr+='0'
			return int(float(yearstr))
		except (ValueError,TypeError,KeyError) as e:
			#except AttributeError:
			return 0

	@property
	def year_author_is_30(self):
		try:
			dob=int(self.author_dates[0])
			return dob+30
		except (ValueError,TypeError,KeyError) as e:
			return 0

	@property
	def title(self): return six.text_type(self.meta['title'])

	@property
	def decade(self): return int(self.year)/10*10

	@property
	def twentyyears(self): return int(self.year)/20*20

	@property
	def quartercentury(self): return int(self.year)/25*25

	@property
	def halfcentury(self): return int(self.year)/50*50

	@property
	def century(self): return int(self.year)/100*100

	def get_author_dates(self,author=None):
		if not author: author=self.author
		return get_author_dates(author)

	@property
	def author_dates(self,keys=['author','author_dob','a1']):
		if hasattr(self,'_author_dates'): return self._author_dates
		dob=self.meta.get('author_dob')
		dod=self.meta.get('author_dod')

		if dob and dod: self._author_dates=(dob,dod)
		elif dob: self._author_dates=(dob,0)
		elif dod: self._author_dates=(0,dod)
		else: self._author_dates=get_author_dates(self.author)

		return self._author_dates

	def get_meta_from_corpus_metadata(self):
		try:
			return self.corpus._metad.get(self.id)
		except AttributeError:
			return {}

	def get_metadata(self,from_metadata=True,from_files=True):
		"""
		This function is called by the corpus class to construct...
			corpus.meta:          a list of dictionaries
			corpus.metad:         a dictionary, keyed {text_id:text_metadata dictionary}
			corpus.metadata:      a pandas dataframe
			corpus.get_text_ids   (only if path_metadata exists)

		This function calls in order:
			self.get_meta_from_corpus_metadata()
			self.get_meta_from_file()
			self.meta_by_file
		"""

		def do_return(meta):
			if not 'corpus' in meta: meta['corpus']=self.corpus.name
			if not 'id' in meta: meta['id']=self.id
			if not '_lltk_' in meta: meta['_lltk_']=self.addr
			return meta

		if hasattr(self,'_meta'): return do_return(self._meta)

		if from_metadata:
			meta=self.get_meta_from_corpus_metadata()
			if meta: return do_return(meta)

		if from_files:
			meta=self.get_meta_from_file()
			if meta: return do_return(meta)

			meta=self.meta_by_file
			if meta: return do_return(meta)
		return do_return({})


	@property
	def meta(self):
		return self.get_metadata()

	@property
	def spacy(self):
		return
		return self.get_spacy()

	def get_spacy(self,load_from_file=False,model_name='en_core_web_sm'):
		import spacy
		global nlp
		if not nlp:
			#print('>> loading spacy...')
			nlp = spacy.load(model_name)

		doc=None
		if self.parsed and load_from_file:
			#print self.fnfn_spacy
			from spacy.tokens.doc import Doc

			try:
				for byte_string in Doc.read_bytes(open(self.fnfn_spacy, 'rb')):
					doc = Doc(nlp.vocab)
					doc.from_bytes(byte_string)
			except UnicodeDecodeError:
				print("!! UNICODE ERROR:",self.fnfn_spacy)
		#else:

		if not doc:
			#print '>> making spacy document for text',self.id
			txt=self.text
			txt=clean_text(txt)
			doc=nlp(txt)

		return doc

	@property
	def parsed(self):
		return os.path.exists(self.fnfn_spacy)

	@property
	def txt(self):
		return self.text

	@property
	def text(self):
		return self.text_plain()
		#if not hasattr(self,'_text'):
		#	self._text=codecs.open(self.fnfn,encoding='utf-8').read()
		#return self._text
		#print self.path_xml, self.fnfn
		"""
		if not self.fnfn or not os.path.exists(self.fnfn): return ''
		of=codecs.open(self.fnfn,encoding='utf-8',errors='ignore')
		txt=of.read()
		of.close()
		return txt
		"""

	@property
	def xml(self):
		return self.text_xml


	@property
	def text_xml(self):
		return '\n'.join([line for line in self.lines_xml()])

	@property
	def exists(self):
		return os.path.exists(self.fnfn_xml) or os.path.exists(self.fnfn_txt)

	@property
	def exists_txt(self):
		return os.path.exists(self.fnfn_txt)

	@property
	def exists_xml(self):
		return os.path.exists(self.fnfn_xml)

	def xml_soup(self):
		import bs4
		return bs4.BeautifulSoup(self.text_xml,'lxml')

	@property
	def dom(self):
		import bs4
		return bs4.BeautifulSoup(self.text_xml,'lxml')


	def split(self,doc_tag='doc'):
		md={}
		docnum=0
		for line in self.lines_xml():
			if '<'+doc_tag+'>' in line:
				docnum+=1
				print('>>',self.id,docnum,'...')
				newfn=self.fnfn_xml.replace('_xml_','_xml_split_').replace(self.ext_xml,'.'+str(docnum).zfill(2)+self.ext_xml)
				if not os.path.exists(os.path.split(newfn)[0]): os.makedirs(os.path.split(newfn)[0])
				#if os.path.exists(newfn): continue
				if newfn.endswith('.gz'):
					f=gzip.open(newfn,'wb')
				else:
					f=codecs.open(newfn,'w','utf-8')
			if not docnum: continue


			if newfn.endswith('.gz'):
				f.write(line.encode('utf-8'))
			else:
				f.write(line)


	## GETTING TEXT

	def text_plain_from_txt(self):
		if not self.exists_txt: return ''
		txt=tools.read(self.fnfn_txt).replace('\r\n','\n').replace('\r','\n')
		return txt

	# def text_plain(self, OK=['p','l'], BAD=[], body_tag='text', force_xml=False, text_only_within_medium=True):
	# 	if not self.exists: return ''
	# 	if not force_xml and os.path.exists(self.fnfn_txt):
	# 		#print '>> text_plain from stored text file:',self.fnfn_txt
	# 		return self.text_plain_from_txt()
	#
	# 	return self.text_plain_from_xml(OK=OK,BAD=BAD,body_tag=body_tag,text_only_within_medium=text_only_within_medium)

	def text_plain(self, force_xml=None):
		"""
		This function returns the plain text file. You may want to modify this.
		"""

		# Return plain text version if it exists
		if self.exists_txt and not force_xml:
			return self.text_plain_from_txt()

		# Otherwise, load from XML?
		if self.exists_xml and hasattr(self,'text_plain_from_xml'):
			return self.text_plain_from_xml()

		return ''


	## TOKENS

	@property
	def tokens(self,pos_tag=False):
		return self.tokens_plain
		## No spacy for now
		#if not os.path.exists(self.fnfn_spacy):
		if not pos_tag:
			tuples = [(tools.noPunc(six.text_type(tok).strip().lower()),'') for tok in self.tokens_plain]
		else:
			tuples = [ (tools.noPunc(six.text_type(tok).strip().lower()), tok.tag_) for tok in self.spacy ]
			tuples = [(a,b) for a,b in tuples if a]
		tuples = [(a,b) for a,b in tuples if a]
		return tuples

	@property
	def words(self):
		#return [a for a,b in self.tokens]
		#return self.tokens_plain
		return self.blob.words

	@property
	def tokens_plain(self):
		import nltk
		try:
			txt=self.text_plain()
			return nltk.word_tokenize(txt)
		except IOError:
			return []

	@property
	def nltk(self):
		import nltk
		tokens = nltk.word_tokenize(self.text_plain())
		return nltk.Text(tokens)

	@property
	def blob(self):
		from textblob import TextBlob
		return TextBlob(self.text_plain())

	@property
	def num_words(self,keys=['num_words_adorn','num_words']):
		for k in keys:
			if k in self.meta:
				return float(self.meta[k])
		return len(self.tokens)


	@property
	def words_recognized(self):
		global ENGLISH

		if not ENGLISH:
			from lltk.tools import get_english_wordlist
			ENGLISH=get_english_wordlist()

		return [w for w in self.words if w in ENGLISH]

	@property
	def ocr_accuracy(self):
		return float(len(self.words_recognized)) / len(self.words)

	@property
	def minhash(self):
		from datasketch import MinHash
		m = MinHash()
		for word in self.tokens:
			m.update(word.encode('utf-8'))
		return m

	@property
	def length(self):
		return self.num_words

	@property
	def num_words_recognized(self):
		return len(self.tokens_recognized)
		#return int(self.num_words * self.ocr_accuracy) if self.ocr_accuracy else self.num_words

	@property
	def length_recognized(self):
		return self.num_words_recognized


	@property
	def freqs_tokens(self):
		from collections import Counter
		return Counter([x.lower() for x in self.tokens])

	@property
	def is_tokenized(self):
		ofolder=os.path.join(self.corpus.path, 'freqs', self.corpus.name)
		ofnfn=os.path.join(ofolder,self.id+'.json')
		return os.path.exists(ofnfn) and os.stat(ofnfn).st_size

	@property
	def freqs_json(self):
		import json,codecs
		fnfn=self.fnfn_freqs
		if not os.path.exists(fnfn): return {}
		with codecs.open(fnfn,encoding='utf-8') as f:
			dx={}
			try:
				for k,v in list(json.load(f).items()):
					k2=tools.noPunc(k)
					if not k2 in dx: dx[k2]=0
					dx[k2]+=v
			except ValueError:
				print("!? JSON LOADING FAILED ON TEXT:",self.id)
				return {}

			return dx

	def save_freqs(self,ofolder=None,force=False):
		return self.save_freqs_json(ofolder=ofolder,force=force)

	@property
	def meta_by_file(self):
		return self.get_meta_from_file()

	def get_meta_by_file(self,maxstrlen=1000):
		# @HACKy... Just need a functon version of property version above
		d=self.meta_by_file
		for k,v in d.items():
			d[k]=str(v).replace('\r\n',' / ').replace('\r',' / ').replace('\n',' / ').replace('\t','  ')
			d[k]=d[k][:maxstrlen]
		return d

	def get_meta_from_file(self):
		return {} #self.meta_from_file
		#return self.meta_from_file

	def save_freqs_json(self,ofolder=None,force=False):
		if not self.id: return {}
		if not ofolder: ofolder=self.corpus.path_freqs
		ofnfn=os.path.join(ofolder,self.id+'.json')
		opath = os.path.split(ofnfn)[0]
		if not os.path.exists(opath): os.makedirs(opath)
		if not force and os.path.exists(ofnfn) and os.stat(ofnfn).st_size:
			#print('>> already tokenized:',self.id)
			return self.freqs_json
		else:
			pass
			#print('>> tokenizing:',self.id,ofnfn)

		#print(self, self.id, self.path_txt, self.exists_txt, self.txt[:10])
		toks=tokenize_text(self.txt)
		#print(len(toks),ofnfn)
		tokd=dict(Counter(toks))
		with open(ofnfn,'w') as of:
			json.dump(tokd,of)
		return tokd

	@property
	def fnfn_freqs(self):
		#fnfn=os.path.join(self.corpus.path, 'freqs', self.corpus.name, self.id+'.json')
		fnfn=os.path.join(self.corpus.path_freqs, self.id+'.json')
		return fnfn

	@property
	def path_freqs(self): return self.fnfn_freqs

	@property
	def word_counts(self):
		if not hasattr(self,'_word_counts'):
			from collections import Counter
			self._word_counts=self.freqs()
		return self._word_counts

	#@property
	def freqs(self,modernize_spelling=False,modernize_spelling_before=3000,use_text_if_nec=True):
		fnfn=self.fnfn_freqs
		if not os.path.exists(fnfn):
			#print('!! no freqs file for:',self.id,'--?-->',fnfn)
			#return {}
			if use_text_if_nec:
				print('!! no freqs file for:',self.id,'--?-->',fnfn)
				dx=self.freqs_tokens
			else:
				return {}
		else:
			dx=self.freqs_json

		if modernize_spelling and self.year<modernize_spelling_before:
			dx2={}
			for k,v in list(dx.items()):
				k2=self.corpus.modernize_spelling(k).lower()
				dx2[k2]=v
			dx=dx2

		return Counter(dx)

	def freqs_ngram(self,n=2):
		return tools.toks2freq(tools.ngram([x for x in self.tokens], n))


	def get_passages(self,phrases=[],window=500):
		import time
		tic=time.clock()
		#print '>> loading passages (I/O)..'
		txt=self.text
		passages=list(tools.passages(txt,phrases=phrases,window=window))
		for dx in passages:
			dx['id']=self.id
			dx['corpus']=self.corpus.name
			for k,v in list(self.meta.items()): dx[k]=v
		toc=time.clock()
		#print '\tdone loading passages(I/O)... ('+str(toc-tic)+' seconds)'
		return passages

	def lines_xml(self):
		if os.path.exists(self.fnfn_xml):
			with open(self.fnfn_xml,errors='ignore') as f:
				for ln in f:
					yield ln

	def lines_txt(self):
		if os.path.exists(self.fnfn_txt,errors='ignore'):
			with open(self.fnfn_txt) as f:
				for ln in f:
					yield ln
		else:
			for ln in self.txt.split('\n'):
				yield ln


	def save_plain_text(self,txt=None,compress=False,force=False):
		if self.id is None: return

		fnfn_txt = os.path.join(self.corpus.path_txt,self.id+'.txt')
		if compress: fnfn_txt+='.gz'
		if not force and os.path.exists(fnfn_txt): return
		if not os.path.exists(self.fnfn_xml): return

		if txt is None: txt=self.text_plain()

		path_fnfn=os.path.dirname(fnfn_txt)
		if not os.path.exists(path_fnfn): os.makedirs(path_fnfn)

		with open(fnfn_txt,'w') as of:
			of.write(txt)
			#print('>> saved:',fnfn_txt)





	def save_vector_html(self,tsne_dd,fn=None):
		if not fn: fn=self.id+'.html'
		vals=[[],[],[]]
		word2vals={}
		for word in tsne_dd:
			if not word in word2vals: word2vals[word]=[]
			keys=sorted(tsne_dd[word].keys())
			vkeys = [k for k in keys if 'tsne_V' in k]
			for i,vk in enumerate(vkeys):
				val=float(tsne_dd[word][vk])
				vals[i]+=[val]
				word2vals[word]+=[val]

		minmaxes = [(min(vs),max(vs)) for vs in vals]
		for word in word2vals:
			for i,val in enumerate(word2vals[word]):
				OldMin,OldMax=minmaxes[i]
				NewMin,NewMax=0.0,255.0
				OldValue=val
				OldRange = (OldMax - OldMin)
				NewRange = (NewMax - NewMin)
				NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
				word2vals[word][i]=str(int(NewValue))

		html=[]
		for line in self.lines_txt():
			words=line.strip().split()
			line_html=[]
			for word in words:
				wordl=tools.noPunc(word.lower())
				if wordl in word2vals:
					span=u'<span style="color:rgb({0})">{1}</span>'.format(','.join(word2vals[wordl]), word)
				else:
					span=u'<span>'+word+'</span>'
				line_html+=[span]
			html+=[u' '.join(line_html)]
		htmlstr=u'<br/>\n'.join(html)

		tools.write2(fn,htmlstr)


	"""
	def incorporate_match(self,other):
		if not hasattr(self,'_meta'): self.meta
		if not hasattr(other,'_meta'): other.meta
		for k,v in other._meta.items():
			if k in self._meta: k+='_'+other._meta['corpus']
			self._meta[k]=v
	"""


REPLACEMENTS={
				'&hyphen;':'-',
				'&sblank;':'--',
				'&mdash;':' -- ',
				'&ndash;':' - ',
				'&longs;':'s',
				u'\u2223':'',
				u'\u2014':' -- ',
				'|':'',
				'&ldquo;':u'“',
				'&rdquo;':u'”',
				'&lsquo;':u'‘’',
				'&rsquo;':u'’',
				'&indent;':'     ',
				'&amp;':'&',


			}

import bleach
def clean_text(txt,replacements=REPLACEMENTS):
	for k,v in list(replacements.items()):
		txt=txt.replace(k,v)
	#return bleach.clean(txt,strip=True)
	return txt






class TextMeta(Text):
	def __init__(self,texts):
		self.texts=texts

	@property
	def id(self):
		return self.texts[0].id

	@property
	def corpus(self):
		return self.texts[0].corpus

	def lines_txt(self):
		return self.texts[0].lines_txt()

	@property
	def meta(self):
		if not hasattr(self,'_meta'):
			self._meta=md={'corpus':self.corpus.name}
			for t in reversed(self.texts):
				for k,v in list(t.meta.items()):
					#if k in md and md[k]:
					#	k=k+'_'+t.__class__.__name__.replace('Text','').lower()
					md[k]=v
		return self._meta


	@property
	def is_metatext(self):
		return True




class TextSection(Text):
	def __init__(self,text,section_num,section_type,section_xml=None,section_txt=None,divider_tag=None):
		self.parent=text
		self.num=section_num
		self.type=section_type
		self.is_text_section = True
		self._xml=section_xml
		self._txt=section_txt
		if divider_tag:
			self.divider_tag=divider_tag.lower()
		elif hasattr(self,'DIVIDER_TAG') and self.DIVIDER_TAG:
			self.divider_tag=self.DIVIDER_TAG
		else:
			self.divider_tag=None

	@property
	def fnfn_freqs(self):
		fnfn=os.path.join(self.corpus.path, 'freqs', self.corpus.parent.name, self.id+'.json')
		return fnfn

	@property
	def text_xml(self):
		if not self._xml:
			self.parent.load_sections()
		return self._xml

	def unload(self):
		self._xml=''
		self._txt=''



	@property
	def id(self):
		return self.parent.id+'/section'+str(self.num).zfill(3)

	@property
	def corpus(self):
		return self.parent.corpus.sections

	def text_plain(self):
		return self.parent.text_plain_from_xml(self.text_xml,text_only_within_medium=True,body_tag=self.divider_tag)

	def lines_txt(self):
		for ln in self.text_plain().split('\n'):
			yield ln

	def load_metadata(self):
		# #md=self._meta=dict(self.parent.meta.items())
		# if not hasattr(self,'_meta'):
		# 	md=self._meta={}
		# else:
		# 	md=self._meta
		#
		md={}
		md['id']=self.id
		md['text_id']=self.parent.id
		md['section_type']=self.type
		return md


	@property
	def meta(self):
		if not hasattr(self,'_meta'):
			md=self._meta={}
		else:
			md=self._meta
		for k,v in list(self.parent.meta.items()):
			if not k in md or not md[k]:
				md[k]=v
		#for k,v in self.load_metadata().items(): md[k]=v
		return self._meta


	@property
	def meta_by_file(self):
		self._meta=self.load_metadata()
		return self._meta


def text_plain_from_xml(xml, OK={'p','l'}, BAD=[], body_tag='text'):
	#print '>> text_plain from stored XML file...'
	import bs4

	## get dom
	dom = bs4.BeautifulSoup(xml,'lxml') if type(xml) in [str,six.text_type] else xml
	txt=[]
	## remove bad tags
	for tag in BAD:
		[x.extract() for x in dom.findAll(tag)]
	## get text
	for doc in dom.find_all(body_tag):
		for tag in doc.find_all():
			if tag.name in OK:
				txt+=[clean_text(tag.text)]
	TXT='\n\n'.join(txt).replace(u'∣','')
	return TXT


def get_author_dates(author):
	import re
	dates = [x for x in re.split('\W',author) if x.isdigit() and len(x)==4]
	if not dates:
		return (None,None)
	elif len(dates)==1:
		return (int(dates[0]),None)
	else:
		return tuple([int(x) for x in dates[:2]])
	return (None,None)

def tokenize_fast(line):
	return re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line.lower())

def tokenize_text(txt):
	from nltk import word_tokenize
	txt=txt.lower().strip()
	toks=word_tokenize(txt)
	toks=[tok for tok in toks if tok and tok[0].isalpha()]
	return toks



########################################################
class PlainText(Text):
	def __init__(self,txt=None,path_to_txt=None):
		self._id=None
		self.corpus=None
		self._sections=[]
		self._fnfn_xml=None
		self._fnfn_txt=path_to_txt
		self._txt=txt

	@property
	def txt(self):
		if self._txt: return self._txt
		return self.text









def save_freqs_json(obj):
	ifnfn,ofnfn=obj
	if not os.path.exists(ifnfn): return
	if os.path.exists(ofnfn): return
	
	opath = os.path.dirname(ofnfn)
	if not os.path.exists(opath): os.makedirs(opath)
	
	# read txt
	with open(ifnfn) as f: txt=f.read()
	
	# tokenize
	toks=tokenize_text(txt)
	#print(len(toks),ofnfn)
	
	# count
	tokd=dict(Counter(toks))
	
	# save
	with open(ofnfn,'w') as of: json.dump(tokd,of)
	
	# return?
	# return tokd