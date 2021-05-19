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
	


class Text(object):
	def __init__(self,idx,corpus,meta={},lang=None,tokenizer=None):
		self.id=idx
		self.corpus=corpus
		self.XML2TXT=self.corpus.XML2TXT
		self.TOKENIZER=self.corpus.TOKENIZER if tokenizer is None else tokenizer
		self.meta=meta
		self.lang=self.corpus.lang if lang is None else lang

	def __repr__(self):
		o=f'{self.author}, _{self.title}_ ({self.year}) [{self.corpus.name}: {self.id}]'
		return o
	# convenience
	def __getattr__(self, name):
		if name.startswith('path_') and hasattr(self.corpus,name):
			ptype=name[len('path_'):]
			exttype=f'ext_{ptype}'.upper()
			if not hasattr(self.corpus, exttype): raise Exception(f'Corpus has no {exttype}') 
			ext=getattr(self.corpus,exttype)
			cpath=getattr(self.corpus,name)
			return os.path.abspath(os.path.join(cpath, self.id + ext))
		elif name in self.meta:
			return self.meta[name]
		else:
			# Default behaviour
			return object.__getattribute__(self, name)

	# load text?
	@property
	def txt(self): return self.text_plain()
	@property
	def xml(self):
		if not os.path.exists(self.path_xml): return ''
		with open(self.path_xml) as f: return f.read()

	# xml
	@property
	def dom(self):
		import bs4
		return bs4.BeautifulSoup(self.text_xml,'lxml')


	def text_plain(self, force_xml=None):
		"""
		This function returns the plain text file. You may want to modify this.
		"""
		# Return plain text version if it exists
		if os.path.exists(self.path_txt) and not force_xml:
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

	#### Convenience
	@property
	def paras(self):
		return [para.strip() for para in self.txt.split('\n\n') if para.strip()]
	@property
	def nltk(self):
		import nltk
		return nltk.Text(self.tokens)
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
