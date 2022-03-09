from lltk.imports import *

## utility funcs

class AuthorBunch(Bunch):
	def __iter__(self):
		for v in self.__dict__.values():
			if issubclass(v.__class__, Text):
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
        id_corpus='',
        id_text='',
        i=None,
        **kwargs):
    
    if id: return id
    if id_corpus and id_text: return f'{IDSEP_START}{id_corpus}{IDSEP}{id_text}'
    if i is None: i=random.randint(1,100000)
    return f'X{i:06}'



class BaseText(object):
	BAD_TAGS={'note','footnote','greek','latin'}
	# BODY_TAG=None
	XML2TXT=default_xml2txt
	TOKENIZER=tokenize


	def __init__(self,
			id=None,
			corpus=None,
			#meta={},

			#id_corpus='',
			#id_text='',
			#i=None,

			lang=None,
			tokenizer=None,
			**kwargs):

		self._source=None
		self.id=id
		self.corpus=corpus
		meta[COL_ID]=self.id
		self._meta=meta

		if is_corpus_obj(corpus):
			self.XML2TXT=corpus.XML2TXT
			self.TOKENIZER=corpus.TOKENIZER
			# if self.lang is None: self.lang=corpus.lang
		
		


	def __repr__(self):
		co=self.corpus.name if self.corpus is not None and hasattr(self.corpus,'name') and self.corpus.name else ''
		au,ti,yr,idx = self.author, self.title, self.year, self.id
		l = [
			co if co else 'Corpus',
			au if au else 'Author',
			ti if ti else 'Title',
			yr if yr else 'Year',
			idx if idx else 'ID',

		]

		co,au,ti,yr,idx = l
		o=f'<{self.__class__.__name__}> {au}, “{ti}” ({yr}) [{co}: {idx}]'
		#return str(self.meta)
		return o
	
	# convenience
	def __getattr__(self, name):
		if name.startswith('path_') and hasattr(self.corpus,name):
			ptype=name[len('path_'):]
			exttype=f'ext_{ptype}'.upper()
			if not hasattr(self.corpus, exttype): raise Exception(f'Corpus has no {exttype}') 
			ext=getattr(self.corpus,exttype)
			cpath=getattr(self.corpus,name)
			opath=os.path.abspath(os.path.join(cpath, self.id + ext))

			# on source?
			if not os.path.exists(opath) and self.source:
				opath2=getattr(self.source,name)
				if os.path.exists(opath2): return opath2
			return opath
		
		if name in self.meta:
			return self.meta[name]
		
		
		res = get_from_attrs_if_not_none(self, name)
		if res is not None: return res
		source = get_from_attrs_if_not_none(self, 'source')

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

	# load text?
	@property
	def addr(self):
		return f'{IDSEP_START}{self.corpus.name}{IDSEP}{self.id}'
	@property
	def txt(self): return clean_text(self.text_plain())
	@property
	def xml(self):
		path_xml = self.get_path_xml()
		if not os.path.exists(path_xml): return ''
		with open(path_xml) as f: return clean_text(f.read())
	
	
	def get_path_xml(self):
		if not os.path.exists(self.path_xml):
			tsrc = self.source
			if tsrc is not None and os.path.exists(tsrc.path_xml):
				return tsrc.path_xml
		return self.path_xml


	# xml
	@property
	def dom(self):
		#if not self._dom:
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
	def source(self): return self.source_text()

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

	# def source_text(self, col_id_corpus='id_corpus', col_id_text='id_text'):
	# 	meta = self._meta
	# 	id_corpus = meta.get(col_id_corpus)
	# 	id_text = meta.get(col_id_text)
	# 	id_c = self.corpus.id if self.corpus is not None else ''
		
	# 	if id_corpus and id_text and id_corpus!=id_c:
	# 		try:
	# 			C = load(id_corpus)
	# 			t = C.textd.get(id_text)
	# 			return t
	# 		except KeyError:
	# 			pass

	@property
	def meta(self):
		meta={
			**(self.source._meta if hasattr(self.source,'_meta') else {}),
			**self._meta,
		}
		return meta
		


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
	def shorttitle(self): return noPunc(self.title.strip().split(':')[0].split(';')[0].split('.')[0].split('(')[0].split('[')[0].strip())

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






def Text(id=None,corpus=None,**kwargs):
	text_ref,corpus_ref=id,corpus
	log.debug(f'Text(id="{text_ref}", corpus="{corpus_ref}"')

	from lltk.corpus.corpus import Corpus
	# have text?
	if issubclass(text_ref.__class__, BaseText):
		# have text
		text = text_ref
		log.debug(f'called on a text object: {text}')
		# already?
		if corpus_ref is None: 
			log.debug(f'no new corpus set, returning as-is: {text}')
			return text
	
	elif type(text_ref)==str:
		corpus_ref_id,text_ref = to_corpus_and_id(text_ref)
		if not corpus_ref: corpus_ref=corpus_ref_id
	
	
	# get corpus
	corpus = Corpus(corpus_ref,**kwargs)
	log.debug(f'got corpus: {corpus}')
	# add text
	text = corpus.init_text(text_ref,**kwargs)
	log.debug(f'returning text {text}')
	# ensure assigned
	# text.corpus = corpus
	return text