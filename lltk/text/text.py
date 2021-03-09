from lltk.imports import *

## utility funcs

class Text(object):
	XML2TXT = xml2txt

	def __init__(self,idx,corpus,meta={},lang=None):
		self.id=idx
		self.corpus=corpus
		self.meta=meta
		self.lang=self.corpus.LANG if lang is None else lang

	# convenience
	def __getattr__(self, name):
		# is path?
		if name.startswith('path_') and hasattr(self.corpus,name):
			ptype=name[len('path_'):]
			exttype=f'ext_{ptype}'.upper()
			if not hasattr(self.corpus, exttype): raise Exception(f'Corpus has no {exttype}') 
			ext=getattr(self.corpus,exttype)
			cpath=getattr(self.corpus,name)
			return os.path.join(cpath, self.id + ext)
		# is meta?
		return self.meta.get(name)

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
		# save?
		if not os.path.exists(self.path_freqs): self.save_freqs_json()
		if not os.path.exists(self.path_freqs): return {}
		with open(self.path_freqs) as f: freqs=Counter(json.load(f))
		return filter_freqs(freqs)


	@property
	def tokens(self):
		return self.TOKENIZE.__func__(self.txt)
	@property
	def words(self):
		return [noPunc(w) for w in self.tokens if noPunc(w)]
	@property
	def sents(self):
		import nltk
		return nltk.sent_tokenize(self.txt)
	@property
	def counts(self):
		return self.freqs()
	
	@property
	def num_words(self,keys=['num_words','length']):
		for k in keys:
			if k in self.meta:
				return int(self.meta[k])
		return len(self.tokens)
	@property
	def words_recognized(self):
		global ENGLISH
		if not ENGLISH: ENGLISH=get_english_wordlist()
		return [w for w in self.words if w in ENGLISH]
	@property
	def ocr_accuracy(self):
		return float(len(self.words_recognized)) / len(self.words)




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
				self.paras,
				desc='Parsing paragraphs with Stanza',
				num_proc=num_proc)
	def stanza(self,lang=None):
		if lang is None: lang=self.lang
		return do_parse_stanza(self.txt)

	@property
	def minhash(self):
		from datasketch import MinHash
		m = MinHash()
		for word in self.tokens:
			m.update(word.encode('utf-8'))
		return m

