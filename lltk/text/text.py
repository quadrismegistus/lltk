from lltk.imports import *

## utility funcs

class AuthorBunch(Bunch):
	def __iter__(self):
		for v in self.__dict__.values():
			if issubclass(v.__class__, BaseText):
				yield v
	@property
	def ti(self): return list(self)
	@property
	def tl(self): return list(self)
	@property
	def ids(self): return [t.id for t in self]
	@property
	def id(self): return self.ids
	
	@property
	def meta(self): return self.corpus.meta[self.corpus.meta.id.isin(set(self.ids))]
	
def get_idx(
		id='',
		i=None,
		allow='_/.-',
		prefstr='X',
		numposs=1000000,
		numzero=None,
		**kwargs):
	if issubclass(id.__class__, BaseText): return id.id
	if id: return ensure_snake(id,allow=allow,lower=False)
	if not numzero: numzero=len(str(numposs))-1
	if not i: i=random.randint(1,numposs-1)
	return f'{prefstr}{i:0{numzero}}'



class BaseText(object):
	BAD_TAGS={'note','footnote','greek','latin'}
	# BODY_TAG=None
	XML2TXT=default_xml2txt
	TOKENIZER=tokenize
	SECTION_CLASS=None
	SECTION_CORPUS_CLASS=None
	SECTION_DIR_NAME=DIR_SECTION_NAME

	def __eq__(self,other): return self.addr == other.addr


	def __init__(self,
			id=None,
			_corpus=None,
			_section_corpus=None,
			_source=None,
			_txt=None,
			_xml=None,
			# lang=None,
			# tokenizer=None,
			**meta):
		self.id=get_idx(id)
		self._source=_source
		self._corpus=_corpus
		self._section_corpus=_section_corpus
		# self._sections=[]
		self._sections={}

		if _txt: self._txt=_txt
		if _xml: self._xml=_xml
		
		for k,v in meta.items():
			try:
				v=int(float(v))
				meta[k]=v
			except (ValueError,TypeError) as e:
				if type(v)==str:
					meta[k]=clean_text(v)
		meta[COL_ID]=self.id
		meta[COL_ADDR]=self.addr
		if self._source is not None:
			meta['_addr_source']=self._source.addr
		
		# meta={**self.init_meta_json(), **meta}
		
		self._meta=meta
		self._orig_cols=set(meta.keys())


		if is_corpus_obj(_corpus):
			self.XML2TXT=_corpus.XML2TXT
			self.TOKENIZER=_corpus.TOKENIZER
		
	@property
	def corpus(self): return self._corpus

	def __repr__(self):
		o=f'[{self.__class__.__name__}]({self.addr})'
		return o

	def init_meta_json(self):
		if self._meta_json is None and type(self.path_meta_json)==str:
			if os.path.exists(self.path_meta_json):
				try:
					with open(self.path_meta_json) as f:
						self._meta_json=json.load(f)
				except Exception as e:
					log.error(e)
					self._meta_json={}
		return self._meta_json if self._meta_json else {}

	def cache_meta_json(self):
		ensure_dir_exists(self.path_meta_json)
		with open(self.path_meta_json,'w') as of:
			json.dump(self._meta,of,indent=4,sort_keys=False)
			# log.debug('Cached: '+self.path_meta_json)

	
	# convenience
	def __getattr__(self, name):
		if name.startswith('path_') and hasattr(self.corpus,name):
			ptype=name[len('path_'):]
			exttype=f'ext_{ptype}'.upper()
			if not hasattr(self.corpus, exttype): raise Exception(f'Corpus has no {exttype}') 
			ext=getattr(self.corpus,exttype)
			cpath=getattr(self.corpus,name)
			try:
				opath=os.path.abspath(os.path.join(cpath, self.id + ext))
			except TypeError as e:
				log.debug(f'Error getting attribute "{name}": {e}')
				return

			# on source?
			if not os.path.exists(opath) and self.source:
				opath2=getattr(self.source,name)
				if os.path.exists(opath2): return opath2
			return opath
		
		if name in self._meta:
			return self._meta[name]
		
		
		res = get_from_attrs_if_not_none(self, name)
		if res is not None: return res
		res = get_from_attrs_if_not_none(self._source, name)
		if res is not None: return res
		return None


	def get_path(t,part='texts'):
		if not t.corpus: return ''
		partattr='path_'+part
		if hasattr(t.corpus, partattr):
			croot=getattr(t.corpus,partattr)
			return os.path.join(croot,t.id)
	
	@property
	def path(self): return self.get_path()
	@property
	def path_meta_json(self): return os.path.join(self.path,'meta.json')
	

	# load text?
	@property
	def addr(self):
		return f'{IDSEP_START}{self.corpus.id}{IDSEP}{self.id}'
	@property
	def txt(self): return self.get_txt()

	@property
	def xml(self):
		if self._xml: return self._xml
		path_xml = self.get_path_xml()
		if not os.path.exists(path_xml): return ''
		with open(path_xml) as f: return clean_text(f.read())
	
	
	def get_path_xml(self):
		if not os.path.exists(self.path_xml):
			tsrc = self.source
			if tsrc is not None and os.path.exists(tsrc.path_xml):
				return tsrc.path_xml
		return self.path_xml

	def get_path_text(self,part='txt'):
		if part=='txt':
			return os.path.join(self.path, 'text.txt')
		elif part=='xml':
			return os.path.join(self.path, 'text.xml')
		return ''

	# xml
	@property
	def dom(self):
		if self._dom is not None: return self._dom
		import bs4
		xml=self.xml
		if xml:
			dom=bs4.BeautifulSoup(xml,'lxml')
			for tag in self.BAD_TAGS:
				for x in dom(tag):
					x.extract()
		else:
			dom=bs4.BeautifulSoup()

		if self.BODY_TAG is not None:
			dom = dom.find(self.BODY_TAG)
		
		return dom

	@property
	def source(self):
		if self._source is not None: return self._source
		return self.source_text()

	def source_text(self):
		if self.id and self.id.startswith(IDSEP_START) and IDSEP in self.id:
			id_corpus,id_text = self.id[1:].split(IDSEP,1)
			if id_corpus and id_text:
				try:
					C = load(id_corpus,install_if_nec=True)
					t = C.textd.get(id_text)
					return t
				except KeyError:
					pass


	@property
	def meta(self): return self.metadata()

	def is_valid(self): return True
	

	
	def metadata(self,force=False,from_source=True,from_wikidata=False,from_json=True,allow_http=True,**kwargs):
		meta={COL_ADDR:self.addr}
		
		if from_source and self.source is not None:
			meta[f'{COL_ADDR}_source']=self.source.addr
			for k,v in self.source.metadata(**kwargs).items(): meta[k]=v
		
		if from_wikidata:
			for k,v in self.wikidata.metadata(allow_http=allow_http,**kwargs).items(): meta[k]=v
			meta[f'{COL_ADDR}_wikidata']=self.source.addr
		
		if from_json:
			for k,v in self.init_meta_json().items(): meta[k]=v

		# from now
		for k,v in self._meta.items(): meta[k]=v

		return meta
	
	def init(self,force=False,**kwargs):
		if force or not self._init:
			self.metadata()
			self._init=True


	def text_plain(self, force_xml=None):
		"""
		This function returns the plain text file. You may want to modify this.
		"""
		# Return plain text version if it exists
		if self.path_txt and os.path.exists(self.path_txt) and not force_xml:
			with open(self.path_txt,encoding='utf-8',errors='ignore') as f:
				return f.read()
		# Otherwise, load from XML?
		if os.path.exists(self.path_xml): return self.xml2txt(self.path_xml)
		return ''

	def get_txt(self,force=False,prefer_sections=True,section_type=None,force_xml=False):
		if force or not self._txt:
			if not prefer_sections:
				self._txt=self.text_plain(force_xml=force_xml)
				self._txt_offsets={}
			else:
				secs=self.sections(section_type)
				if secs is not None and secs.txt:
					self._txt=secs.txt
					self._txt_offsets=secs._txt_offsets
				else:
					self._txt=self.text_plain(force_xml=force_xml)
					self._txt_offsets={}
		return clean_text(self._txt) if self._txt else ''

	
	# freqs
	def save_freqs_json(self,ofolder=None,force=False):
		if not self.id: return {}
		if not os.path.exists(self.path_txt): return {}
		if not force and os.path.exists(self.path_freqs): return
		return save_freqs_json((self.path_txt,self.path_freqs,self.corpus.TOKENIZER.__func__))

	def freqs(self,lower=True,modernize_spelling=None):
		if not hasattr(self,'_freqs') or not self._freqs:
			# print('loading from file')
			if not os.path.exists(self.path_freqs): self.save_freqs_json()
			if not os.path.exists(self.path_freqs): return {}
			with open(self.path_freqs) as f: freqs=Counter(json.load(f))
			self._freqs=freqs
		return filter_freqs(self._freqs,modernize=modernize_spelling,lower=lower)

	@property
	def length(self): return sum(self.freqs().values())

	def tokens(self,lower=True):
		return self.TOKENIZER.__func__(self.txt.lower() if lower else self.txt)
	@property
	def words(self,lower=False):
		tokens=[noPunc(w) for w in self.tokens(lower=lower)]
		return [w for w in tokens if w]
	def sents(self):
		import nltk
		return nltk.sent_tokenize(self.txt)
	@property
	def counts(self,*x,**y): return self.freqs(*x,**y)
	def len():
		return self.num_words()
	@property
	def tfs(self,*x,**y): 
		counts=self.freqs(*x,**y)
		total=self.length
		return dict((w,v/total) for w,v in counts.items())
	@property
	def fpm(self,*x,**y):
		return dict((w,v*1000000) for w,v in self.tfs(*x,**y).items())
	def num_words(self,keys=['num_words','length']):
		for k in keys:
			if k in self.meta:
				return int(self.meta[k])
		return sum(self.counts().values())
	@property
	def words_recognized(self):
		wordlist=get_wordlist(lang=self.lang)
		return [w for w in self.words if w in wordlist or w.lower() in wordlist]
	@property
	def ocr_accuracy(self):
		return float(len(self.words_recognized)) / len(self.words) if len(self.words) else np.nan
	
	def yearbin(self,ybin,as_str=False,zfill=4):
		try:
			binval=self.year//ybin*ybin
			return binval if not as_str else f'{str(binval).zfill(zfill)}-{str(binval+ybin).zfill(zfill)}'
		except Exception:
			return np.nan
	@property
	def halfdecade(self): return self.yearbin(5)
	@property
	def decade(self): return self.yearbin(10)
	@property
	def quartercentury(self): return self.yearbin(25)
	@property
	def halfcentury(self): return self.yearbin(50)
	@property
	def century(self): return self.yearbin(100)
	@property
	def halfdecade_str(self): return self.yearbin(5,as_str=True)
	@property
	def decade_str(self): return self.yearbin(10,as_str=True)
	@property
	def quartercentury_str(self): return self.yearbin(25,as_str=True)
	@property
	def halfcentury_str(self): return self.yearbin(50,as_str=True)
	@property
	def century_str(self): return self.yearbin(100,as_str=True)
	@property
	def au(self): return to_authorkey(self.author)
	@property
	def ti(self): return to_titlekey(self.title)
	@property
	def shorttitle(self,puncs=':;.([,!?',ok={'Mrs','Mr','Dr'}):
		o=self.title.strip().replace('—','--').replace('–','-')
		for x in puncs:
			o2=o.split(x)[0].strip()
			if o2 in ok: continue
			o=o2
		return o
	
	@property
	def shortauthor(self):
		au=clean_text(self.author)
		if not au: return ''
		if not ',' in au: return au
		
		parts=[x.strip() for x in au.split(',') if x.strip() and x.strip()[0].isalpha()]
		if len(parts)==0: return au
		if len(parts)==1: return parts[0]
		oparts=[parts[1]] + [parts[0]]

		# parentheses
		def grabparen(x):
			if '(' in x and ')' in x: return x.split('(',1)[-1].split(')',1)[0].strip()
			return x
		oparts=[grabparen(x) for x in oparts]
		ostr=' '.join(oparts)
		return ostr



	@property
	def prose_or_verse(t):
		for g in [t.medium, t.genre, t.major_genre, t.canon_genre]:	
			if g in {'Prose','Non-Fiction','Fiction','Biography','Oratory'}:
				return 'Prose'
			elif g in {'Verse','Poetry'}:
				return 'Verse'
		if t.corpus.name in {'Chadwyck'}:
			return 'Prose'
		elif t.corpus.name in {'ChadwyckPoetry'}:
			return 'Verse'
		# else:
		# 	txt_verse, txt_prose = t.txt_verse, t.txt_prose
		# 	if txt_verse or txt_prose:
		# 		return 'Verse' if len(txt_verse)>len(txt_prose) else 'Prose'
		return ''
	@property
	def is_prose(self): return self.prose_or_verse=='Prose'
	@property
	def is_verse(self): return self.prose_or_verse=='Verse'

	@property
	def txt_prose(self):
		paras=self.paras_xml
		if not paras and self.is_prose: paras=self.paras
		return '\n\n'.join(paras if paras else [])
	@property
	def txt_verse(self):
		lines=self.lines_xml
		if not lines and self.is_verse: return self.txt
		return '\n'.join(lines if lines else [])

	@property
	def lines_xml(self):
		dom=self.dom
		for x in dom('p'): x.extract()
		return [clean_text(e.text).strip() for e in dom('l')]
	
	@property
	def paras_xml(self):
		dom=self.dom
		for x in dom('l'): x.extract()
		paras=[e.text.strip() for e in dom('p')]
		return [para for para in paras if para]
	@property
	def paras_txt(self):
		paras=[para.strip() for para in self.txt.split('\n\n')]
		return [para for para in paras if para]
	@property
	def paras(self):
		return self.paras_txt
		# paras = self.paras_xml
		# if not paras: paras = self.paras_txt
		# return paras

	@property
	def nltk(self):
		import nltk
		return nltk.Text(self.tokens())
	@property
	def blob(self):
		from textblob import TextBlob
		return TextBlob(self.txt)
	def stanza_paras(self,lang=None,num_proc=1):
		if lang is None: lang=self.lang
		txt=self.txt
		if not txt: return
		yield from pmap_iter(
				do_parse_stanza,
				# self.paras,
				[(para,lang) for para in self.paras],
				desc='Parsing paragraphs with Stanza',
				num_proc=num_proc)
	@property
	def stanza(self,lang=None):
		if lang is None: lang=self.lang
		#return do_parse_stanza(self.txt)
		return list(self.stanza_paras(lang=lang))
	@property
	def spacy(self,lang=None,num_proc=1):
		if lang is None: lang=self.lang
		objs=[(para,lang) for para in self.paras]
		if not objs: return []
		return pmap(
			do_parse_spacy,
			objs,
			desc='Parsing paragraphs with spaCy',
			num_proc=num_proc
		)
	@property
	def minhash(self):
		from datasketch import MinHash
		m = MinHash()
		for word in self.tokens:
			m.update(word.encode('utf-8'))
		return m



	def get_section_class(self,section_class=None):
		if section_class is not None: return section_class
		if self.SECTION_CLASS is not None: return self.SECTION_CLASS
		return TextSection

	def get_section_corpus_class(self,section_corpus_class=None):
		if section_corpus_class is not None: return section_corpus_class
		if self.SECTION_CORPUS_CLASS is not None: return self.SECTION_CORPUS_CLASS
		from lltk.corpus.corpus import SectionCorpus
		return SectionCorpus


	


	# @property
	@property
	def letters(self): return self.sections(_id='letters')
	@property
	def chapters(self): return self.sections(_id='chapters')

	def sections(self,_id=None,section_class=None,section_corpus_class=None,force=False):
		if _id is None: _id=self.SECTION_DIR_NAME
		if force or _id not in self._sections:
			SectionCorpusClass = self.get_section_corpus_class(section_corpus_class)
			self._sections[_id]=SectionCorpusClass(
				# id=os.path.join(self.id, _id),
				id=_id,
				_source=self,
				_id_allows='_/',
				_id=_id
			)
		return self._sections.get(_id)

	@property
	def text_root(self):
		if not issubclass(self.__class__,BaseText): return
		if issubclass(self.__class__,TextSection): return self.source
		return self

	def characters(self,id='default',systems={'booknlp'},**kwargs):
		if type(self._characters)!=dict: self._characters={}
		if not id in self._characters:
			from lltk.model.characters import CharacterSystem
			CS=self._characters[id]=CharacterSystem(self.text_root)
			for sysname in systems:
				system=getattr(self,sysname)
				CS.add_system(system)
		return self._characters[id]

	def get_character_id(self,char_tok_or_id,**kwargs):
		return self.characters().get_character_id(char_tok_or_id,**kwargs)

	@property
	def charsys(self): return self.characters()
	def interactions(self,**kwargs): return self.charsys.interactions(**kwargs)

	@property
	def booknlp(self):
		if self._booknlp is None: self._booknlp={}
		if not self.addr in self._booknlp:
			from lltk.model.booknlp import ModelBookNLP
			self._booknlp[self.addr]=ModelBookNLP(self)
		return self._booknlp[self.addr]
	
	@property
	def wikidata(self):
		t=Text(self.addr, 'wikidata')
		t.metadata()
		return t
	



class TextSection(BaseText):
	_type='sections'

	@property
	def corpus(self): return self._section_corpus

	# @property
	# def corpus(self): return self._section_corpus
	
	# @property
	# def path(self): return os.path.join(self.source.path,self.id)
	# @property
	# def addr(self): return os.path.join(self.source.addr,self.id)
	# @property
	# def txt(self): return self._txt if self._txt else ''
	# @property
	# def xml(self): return self._xml if self._xml else ''
	@property
	def path_txt(self): return self.get_path_text('txt')
	@property
	def path_xml(self): return self.get_path_text('xml')
	
	# def __repr__(self):
	# 	o=f'[{self.__class__.__name__}]({self.corpus.id}|{self.id})'
	# 	return o

TEXT_CACHE={}

def is_text_obj(obj):
	return issubclass(type(obj), BaseText)

def is_corpus_obj(obj): 
	from lltk.corpus.corpus import BaseCorpus
	return issubclass(type(obj), BaseCorpus)


def get_addr(text_ref,corpus_ref=None,**kwargs):
	if is_text_obj(text_ref): text_ref = text_ref.addr
	if is_corpus_obj(corpus_ref): corpus_ref = corpus_ref.id    
	addr = text_ref
	if corpus_ref: 
		corpus_ref_addr = f'_{corpus_ref}/'
		if not addr.startswith(corpus_ref_addr):
			addr = corpus_ref_addr + addr
	return addr


def Text(text_ref=None,corpus_ref=None,force=False,**kwargs):
	global TEXT_CACHE
	
	# log.debug(f'Generating text with id="{text_ref}" and corpus="{corpus_ref}"')
	# have text?
	addr = get_addr(text_ref,corpus_ref)
	if force or addr not in TEXT_CACHE:
		log.debug(f'Getting text with address {addr}')
		# log.debug('Genereating text')
		id_corp,id_text = to_corpus_and_id(addr)
		# any left?
		id_text_corp, id_text_text = to_corpus_and_id(id_text)
		if id_text_corp:
			sub_text = Text(id_text)
		else:
			sub_text = id_text

		from lltk.corpus.corpus import Corpus
		TEXT_CACHE[addr] = Corpus(id_corp,force=force).text(sub_text)
	return TEXT_CACHE.get(addr)