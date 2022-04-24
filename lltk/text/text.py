from heapq import merge
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
	


class BaseText(BaseObject):
	BAD_TAGS={'note','footnote','greek','latin'}
	# BODY_TAG=None
	XML2TXT=default_xml2txt
	TOKENIZER=tokenize
	SECTION_CLASS=None
	SECTION_CORPUS_CLASS=None
	SECTION_DIR_NAME=DIR_SECTION_NAME
	META={'genre':'', 'medium':''}

	def __eq__(self,other):
		addr1 = self.addr
		addr2 = other.addr if is_text_obj(other) else other
		return addr1==addr2
	
	def __hash__(self): return hash(self.addr)

	def __init__(self,
			id=None,
			_corpus=None,
			_section_corpus=None,
			_source=None,
			_sources=set(),
			_txt=None,
			_xml=None,
			# lang=None,
			# tokenizer=None,
			**meta):
		
		if id is None and _source is not None: id=_source.addr
		if id is None and len(_sources): id=list(_sources)[0].addr
		self.id=get_idx(id)
		
		self._sources = {src for src in list(_sources) + [_source] if src is not None}
		
		# self._source=_source
		from lltk.corpus.corpus import Corpus
		corpus=Corpus(_corpus)
		self._corpus=corpus
		self._section_corpus=_section_corpus
		self._sections={}
		self._is=set()
		self._meta={}

		if _txt: self._txt=_txt
		if _xml: self._xml=_xml

		self.set_meta(**meta)		
		self._orig_cols=set(self._meta.keys())
	
	def set_meta(self,ensure_id=True,**meta):
		# self.log('_meta',self._meta)
		# self.log('meta',meta)
		ometa = merge_dict(self._meta, meta)
		# self.log('ometa',ometa)
		if ensure_id: ometa=self.ensure_id_addr(ometa)
		# self.log('ometa 2',ometa)
		self._meta = ometa

	# @property
	# def XML2T
	# 	self.XML2TXT=corpus.XML2TXT
	# 	self.TOKENIZER=corpus.TOKENIZER.__func__
		
	@property
	def corpus(self): return self._corpus

	def __repr__(self):
		# o=f'[{self.__class__.__name__}]({self.addr.split("/")[-1]})'
		o=f'(({self.addr}))'
		# o=f'[{self.corpus.name}]({self.idx})'
		return o
	
	def init_cache(self,*x,**y):
		newmeta1={}#self.init_cache_json(*x,**y)
		newmeta2=self.init_cache_db(*x,**y)
		self._meta=merge_dict(newmeta1, newmeta2, self._meta)
		return self._meta

	def init_cache_db(self,*x,**y):
		with self.corpus.get_cachedb(mode="r") as db: return db.get(self.id,{})

	def init_cache_json(self,*x,**y):
		return read_json(self.path_meta_json)
	
	def cache_meta(self,cache=True,force=False,from_sources=False,from_query=False,**kwargs):
		return self.metadata(cache=cache,force=force,from_sources=from_sources,from_query=from_query,**kwargs)
		
	def cache(self,*x,**y):
		self.zsave()

		# ometa=merge_dict(self.init_cache(), self._meta, meta)
		# self.cache_db(ometa)
		# self.cache_json(ometa)
	
	def cache_json(self,ometa):
		write_json(ometa,self.path_meta_json)
		log.debug(f'Cached json: {self.path_meta_json}')
	
	def cache_db(self,ometa,verbose=False):
		if verbose: self.log(f'[{self.addr}] Caching',ometa)
		with self.corpus.get_cachedb() as db: db[self.id]=ometa

	
		# convenience
	def __getattr__(self, name, allow_sources=True, **kwargs):
		if name.startswith('path_') and hasattr(self.corpus,name):
			ptype=name[len('path_'):]
			exttype=f'ext_{ptype}'.upper()
			if not hasattr(self.corpus, exttype): raise Exception(f'Corpus has no {exttype}') 
			ext=getattr(self.corpus,exttype)
			cpath=getattr(self.corpus,name)
			try:
				opath=os.path.abspath(os.path.join(cpath, self.id + ext))
			except TypeError as e:
				log.error(f'Error getting attribute "{name}": {e}')
				return

			# on source?
			if not os.path.exists(opath) and self.sources:
				for source in self.sources:
					opath2=getattr(source,name)
					if os.path.exists(opath2):
						return opath2
			return opath
		
		res = self._meta.get(name)
		if res is not None: return res
		
		res = get_from_attrs_if_not_none(self, name)
		if res is not None: return res

		# res = get_prop_ish(self.meta, name)
		# if res is not None: return res
		# log.debug('??')
		
		# if allow_sources:
		# 	for source in self.get_sources(wikidata=False):
		# 		# print(f'Consulting source: {source}')
		# 		res = source.__getattr__(name,allow_sources=False,**kwargs)
		# 		if res is not None:  return res
		
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
	def meta(self): return self.metadata()


	def id_is_valid(self,*x,**y): return True
	def meta_is_valid(self,*x,**y): return True
	def is_valid(self,meta=None,**kwargs):
		"""
		@TODO: Subclasses need to implement this
		"""
		return True

	@property
	def matcher(self): return self.corpus.matcher
	@property
	def matches(self): return self.get_matches()
	def get_matches(self,**kwargs): 
		return set(self.matcher[self.addr]) - set([self])
	
	
	# Text
	def add_source(self,source,viceversa=False,yn='',**kwargs):
		source=Text(source)
		self._sources|={source}
		self.matcher.match(self,source,yn=yn,**kwargs)
		if viceversa: source.add_source(self,yn=yn,viceversa=False,**kwargs)
		# self.zsave()


	@property
	def idx(self): return self.id.split('/')[-1]
	
		

	def get_sources(self,remote=True,**kwargs):
		sources=self.get_local_sources(**kwargs)
		if remote:
			self.get_remote_sources(sources,**kwargs)
			sources=self.get_local_sources(**kwargs)
		
		return sorted(list(sources),key=lambda t: t.addr)

	def get_remote_sources(self,sources=None,wikidata=True,**kwargs):
		self.log('Sources?',sources)
		sofar=set()
		if wikidata:
			wiki=self.wikidata(sources,**kwargs)
			if wiki is not None:
				sofar|={wiki}
		return sofar - {self}


	def get_local_sources(self,sofar=None,recursive=True,verbose=False,**kwargs):
		if verbose: self.log(f'get_sources(sofar = {sofar}, recursive = {recursive}, **kwargs')
		sofar = set() if not sofar else set([x for x in sofar])

		matches = set(self.get_matches(**kwargs)) - {self}
		if matches and verbose: self.log('Matches:',matches)

		_sources = set(self._sources) - {self}
		if _sources and verbose: self.log('My _sources:',_sources)
		

		srcs_recursive = set()
		newsrcs = (_sources | matches) - set(sofar) - {self}
		if newsrcs and verbose: self.log('New sources:',newsrcs)

		for src in newsrcs:
			if src not in sofar:
				src = Text(src)
				if src not in sofar:
					if verbose: self.log('Found source:',src)
					sofar|={src}
					srcs_recursive|={src}
		
		if recursive:
			if srcs_recursive and verbose: self.log('_recursive: ',srcs_recursive)
			for src in (srcs_recursive - {self}):
				if src.id_is_valid():
					if verbose: self.log(f'Src: {src}')
					try:
						sofar|=set(src.get_local_sources(
							sofar=sofar, 
							recursive=recursive,
							**kwargs
						))
					except AttributeError as e:
						log.error(e)
						pass
				
				sofar|={src}
		
		return sofar - {self}

	
	@property
	def sources(self): return self.get_sources()

	@property
	def source(self):
		srcs=self.sources
		if srcs: return list(srcs)[0]

	def query(self,*x,**y): return {}

	def metadata(
			self,
			meta={},
			from_cache=True,
			from_query=True,
			from_sources=True,
			cache=True,
			stamped=False,
			unstamped=False,
			**kwargs):
		# get meta
		# return self._meta
		self.corpus.init()					
		# query?
		ometa={} if not from_cache else {**self.init_cache()}
		ometa=merge_dict(TEXT_META_DEFAULT, self.META, ometa, self._meta, meta)
		ometa={k:v for k,v in ometa.items() if k not in {'_source'}}

		if stamped:
			ometa_unstamped = unstamp_d(self, ometa)
			ometa_stamped = stamp_d(self, ometa_unstamped)
			if unstamped:
				ometa = merge_dict(ometa_stamped, ometa_unstamped)
			else:
				ometa = ometa_stamped
		

		if from_query:
			if self.id_is_valid() and not self.meta_is_valid(ometa):
				query_meta = self.query(**kwargs)
				if query_meta:
					ometa=merge_dict(ometa, query_meta)
		
		# sources?
		if from_sources:
			for src in self.get_sources(ometa):
				# if not wikidata and src.corpus.id=='wikidata': continue
				sd = src.metadata(from_sources=False,stamped=False,**kwargs)
				if stamped:
					sd1 = unstamp_d(src,sd)
					sd2 = stamp_d(src,sd1)
					sd = merge_dict(sd1,sd2) if unstamped else sd2
				
				sd2={k:v for k,v in sd.items() if k!=self.corpus.col_id}
				ometa = merge_dict(ometa, sd2)

		ometa = to_numeric_dict(ometa)
		ometa = self.ensure_id_addr(ometa)
		self._meta = ometa
		if cache: self.cache()
		return self._meta
	
	@property
	def col_addr(self): return self.corpus.col_addr
	@property
	def col_id(self): return self.corpus.col_id

	def ensure_id_addr(self,meta=None,col_id=COL_ID,col_corpus='_corpus'):
		if meta is None: meta=self._meta
		
		ometa1={col_id:self.id}
		# ometa1[f'{col_id}__{self.corpus.id}']=self.id
		# for src in self.get_sources(meta):
		# 	ometa1[f'{col_id}__{src.corpus.id}']=src.id
		
		ometa={
			**ometa1,
			**dict(sorted({k:v for k,v in meta.items() if k!=col_id}.items()))
		}

		return ometa
		
		# meta[f'{_id_}{self.corpus.id}']=self.id
		#meta['_'+col_id]=self.id
		#meta0 = {k:v for k,v in meta.items() if k==col_id}
		#meta1 = {k:v for k,v in meta.items() if k!=col_id and k.startswith(_id_)}
		#meta2 = {k:v for k,v in meta.items() if k!=col_id and not k.startswith(_id_)}
		# return merge_dict(meta0,meta1,meta2)
	
	def init(self,force=False,**kwargs):
		if force or not self._init:
			self.metadata(**kwargs)
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
	def title(self): return get_prop_ish(self._meta, 'title')
	@property
	def author(self): return get_prop_ish(self._meta, 'author')
	@property
	def au(self): return to_authorkey(self.author)
	@property
	def ti(self): return to_titlekey(self.title)
	
	@property
	def shorttitle(self,
			puncs=':;.([,!?',
			ok={'Mrs','Mr','Dr'},
			title_end_phrases={
				'edited by','written by',
				'a novel','a tale','a romance','a history','a story',
				'a domestic tale',
				'by the author','by a lady','being some','by Miss','by Mr',
				'an historical','the autobiography',
				'being',
				' by ',
				' or'
			},
			replacements={
				' s ':"'s ",
			},
			replacements_o={"'S ":"'s "}
			):

		ti=self.title
		ti=ti.strip().replace('—','--').replace('–','-')
		ti=ti.title()
		for x,y in replacements.items(): ti=ti.replace(x.title(),y)
		if any(x in ti for x in puncs):
			for x in puncs:
				o2=ti.split(x)[0].strip()
				if o2 in ok: continue
				ti=o2
		else:
			l=list(title_end_phrases)
			l.sort(key = lambda x: -len(x))
			for x in l:
				# log.debug(x+' ?')
				ti=ti.split(x.title())[0].strip()
		o=ti.strip()
		for x,y in replacements_o.items(): o=o.replace(x,y)
		return o
	
	@property
	def qstr(self): return clean_text(f'{self.au}, {self.shorttitle}')
	
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
	


	# @property
	@property
	def wiki(self): return self.wikidata()

	def wikidata(self,sources=None,force=False,force_inner=None,**kwargs):
		# log.debug(f'force={force}')
		from lltk.corpus.wikidata import is_wiki_text_obj
		if force or self._wikidata is None or (is_wiki_text_obj(self._wikidata) and not self._wikidata.is_valid()):
			from lltk.corpus.wikidata import TextWikidata
			self._wikidata = TextWikidata(
				self,
				_sources=sources,
				force=force_inner if force_inner is not None else force,
				**kwargs
			)			
			# log.debug(f'Adding to sources: {self._wikidata}')
			if self._wikidata is not None:
				self._sources|={self._wikidata}
		return self._wikidata
	



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
	
	
def get_addr(text_ref,corpus_ref=None,**kwargs):
	if is_text_obj(text_ref): text_ref = text_ref.addr
	if is_corpus_obj(corpus_ref): corpus_ref = corpus_ref.id    
	addr = text_ref
	if corpus_ref: 
		corpus_ref_addr = f'_{corpus_ref}/'
		if not addr.startswith(corpus_ref_addr):
			addr = corpus_ref_addr + addr
	return addr








TEXT_CACHE=defaultdict(type(None))

def Text(text=None,corpus=None,addr=None,force=False,verbose=True,use_db=USE_ZODB,col_id=COL_ID,**meta):
	global TEXT_CACHE
	if use_db:
		db=get_zodb() if USE_ZODB else defaultdict(type(None))
	
	if is_text_obj(text) and not corpus: return text
	taddr = get_addr_str(text,corpus) if not addr else addr
	if not taddr: return
	# log.debug(f'? Text({text})')

	t=None
	if not force:
		if is_text_obj(TEXT_CACHE[taddr]):
			if verbose: log.debug(f'<< Text({taddr})')
			t=TEXT_CACHE[taddr]
		elif use_db:
			tdb = db[taddr]
			if is_text_obj(tdb):
				if verbose: log.debug(f'^^ Text({taddr})')
				TEXT_CACHE[taddr] = t = tdb
	
	if not is_text_obj(t):
		if verbose: log.debug(f'++ Text({taddr})')
		tcorp,tid = to_corpus_and_id(taddr)
		from lltk.corpus.corpus import Corpus
		meta[COL_ID]=tid
		t = Corpus(tcorp).text(**meta)
		# cache
		if is_text_obj(t):
			TEXT_CACHE[taddr] = t
			db[taddr] = t
	
	return t










# def Text(text=None,corpus=None,addr=None,force=False,verbose=True,**meta):
# 	global TEXT_CACHE
# 	log.debug('?')
# 	if is_text_obj(text) and not corpus: return text
# 	taddr = get_addr_str(text,corpus) if not addr else addr
# 	if not taddr: return
# 	if force or TEXT_CACHE.get(taddr) is None:
# 		if verbose: log.debug(f'++ Text({taddr})')
# 		tcorp,tid = to_corpus_and_id(taddr)
# 		from lltk.corpus.corpus import Corpus
# 		TEXT_CACHE[taddr] = Corpus(tcorp).text(tid,**meta)
# 	else:
# 		if verbose: log.debug(f'<< Text({taddr})')
# 		t = TEXT_CACHE[taddr]
# 		if meta:
# 			t.set_meta(**meta)
# 			if USE_ZODB: t.zsave()
			
# 	return TEXT_CACHE.get(taddr)



