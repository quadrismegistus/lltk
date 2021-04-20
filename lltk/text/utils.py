from lltk.imports import *





















def do_parse_stanza(obj):
	txt,lang=obj
	nlp=get_stanza_nlp(lang=lang)
	return nlp(txt)

nlpd={}
def get_stanza_nlp(lang='en'):
	global nlpd
	if not lang in nlpd:
		import stanza
		try:
			nlpd[lang] = stanza.Pipeline(lang)
		except FileNotFoundError:
			stanza.download(lang)
			nlpd[lang] = stanza.Pipeline(lang)
	nlp=nlpd[lang]
	return nlp
def get_spacy_nlp(lang='en'):
	global nlpd
	langsp='spacy_'+lang
	model=f"{lang}_core_web_sm"
	if not langsp in nlpd: 
		import spacy
		try:
			nlp = spacy.load(model)
		except OSError:
			os.system('python -m spacy download {model}')
			try:
				nlp = spacy.load(model)
			except OSError:
				return
		nlpd[langsp]=nlp
	else:
		nlp=nlpd[langsp]
	return nlp


def do_parse_spacy(obj):
	txt,lang=obj
	nlp=get_spacy_nlp(lang=lang)
	return nlp(txt)
	


## Spelling
V2S = None
def variant2standard():
	global V2S
	if not V2S:
		V2S = dict((d['variant'],d['standard']) for d in tools.tsv2ld(SPELLING_VARIANT_PATH,header=['variant','standard','']))
	return V2S

def standard2variant():
	v2s=variant2standard()
	d={}
	for v,s in list(v2s.items()):
		if not s in d: d[s]=[]
		d[s]+=[v]
	return d



def phrase2variants(phrase):
	s2v=standard2variant()
	words = phrase.split()
	word_opts = [[s]+s2v[s] for s in words]
	word_combos = list(tools.product(*word_opts))
	phrase_combos = [' '.join(x) for x in word_combos]
	return phrase_combos
###




def load_english():
	return get_wordlist(lang='en')







### Functions to be mapped


def get_dtm_freqs(obj):
	import ujson as json
	path,words,dmeta = obj
	with open(path,encoding='utf-8',errors='ignore') as f:
		counts=json.load(f)
	total=sum(counts.values())
	dx={
		**dict((w,c) for w,c in counts.items() if w in words),
		# **{'_total':total},
		**dmeta
	}
	return dx


def do_preprocess_txt(obj):
	ifnfn, ofnfn, func = obj
	otxt = func(ifnfn)
	odir=os.path.dirname(ofnfn)
	if not os.path.exists(odir):
		try:
			os.makedirs(odir)
		except Exception:
			pass
	
	with open(ofnfn,'w',encoding='utf-8',errors='ignore') as f:
		f.write(otxt)
		# print('>> saved:',ofnfn)


def do_metadata_text(i,text,num_words=False,ocr_accuracy=False):
	global ENGLISH
	md=text.meta
	print('>> starting:',i, text.id, len(md),'...')
	if num_words or ocr_accuracy:
		print('>> getting freqs:',i,text.id,'...')
		freqs=text.freqs()
		print('>> computing values:',i,text.id,'...')
		if num_words:
			md['num_words']=sum(freqs.values())
		if ocr_accuracy:
			num_words_recognized = sum([v for k,v in list(freqs.items()) if k[0] in ENGLISH])
			print(md['num_words'], num_words_recognized)
			md['ocr_accuracy'] = num_words_recognized / float(md['num_words']) if float(md['num_words']) else 0.0
	print('>> done:',i, text.id, len(md))
	return [md]


def skipgram_do_text(text,i=0,n=10):
	from lltk import tools
	print(i, text.id, '...')
	from nltk import word_tokenize
	words=word_tokenize(text.text_plain)
	words=[w for w in words if True in [x.isalpha() for x in w]]
	word_slices = tools.slice(words,slice_length=n,runts=False)
	return word_slices

def skipgram_do_text2(text_i,n=10,lowercase=True):
	text,i=text_i
	import random
	print(i, text.id, '...')
	from lltk import tools
	words=text.text_plain.strip().split()
	words=[tools.noPunc(w.lower()) if lowercase else tools.noPunc(w) for w in words if True in [x.isalpha() for x in w]]
	#sld=[]
	for slice_i,slice in enumerate(tools.slice(words,slice_length=n,runts=False)):
		sdx={'id':text.id, 'random':random.random(), 'skipgram':slice, 'i':slice_i}
		yield sdx
		#sld+=[sdx]
	#return sld

def skipgram_save_text(text_i_mongotup,n=10,lowercase=True,batch_size=1000):
	text,i,mongotuple = text_i_mongotup
	from pymongo import MongoClient
	c=MongoClient()
	db1name,db2name=mongotuple
	db0=getattr(c,db1name)
	db=getattr(db0,db2name)

	sld=[]
	for sdx in skipgram_do_text2((text,i),n=n,lowercase=lowercase):
		sld+=[sdx]
		if len(sld)>=batch_size:
			db.insert(sld)
			sld=[]
	if len(sld): db.insert(sld)
	c.close()
	return True




def save_tokenize_text(text,ofolder=None,force=False):
	import os
	if not ofolder: ofolder=os.path.join(text.corpus.path, 'freqs', text.corpus.name)
	ofnfn=os.path.join(ofolder,text.id+'.json')
	opath = os.path.split(ofnfn)[0]
	if not os.path.exists(opath): os.makedirs(opath)
	if not force and os.path.exists(ofnfn) and os.stat(ofnfn).st_size:
		print('>> already tokenized:',text.id)
		return
	else:
		print('>> tokenizing:',text.id,ofnfn)

	from collections import Counter
	import json,codecs
	toks=tokenize_text(text)
	tokd=dict(Counter(toks))
	with codecs.open(ofnfn,'w',encoding='utf-8') as of:
		json.dump(tokd,of)
	#assert 1 == 2


def to_textids(l,col_id='id'):
	import pandas as pd
	from lltk import Text

	if issubclass(l.__class__, pd.DataFrame) and col_id in set(l.reset_index().columns):
		return list(l.reset_index()[col_id])
	
	return [
		x.id if (Text in x.__class__.mro()) else x
		for x in l
	]

def clean_text(txt):
	replacements={
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
	for k,v in list(replacements.items()):
		txt=txt.replace(k,v)
	#return bleach.clean(txt,strip=True)
	return txt



def to_lastname(name):
	name=name.strip()
	if not name: return 'Unknown'
	if ',' in name:
		name=name.split(',')[0]
	else:
		name=name.split()[-1]
	return name


def default_xml2txt(xml, *x, OK={'p','l'}, BAD=[], body_tag='text', **args):
	#print '>> text_plain from stored XML file...'
	import bs4
	if '\n' not in xml and os.path.exists(xml):
		with open(xml) as f: xml=f.read()

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

def tokenize_agnostic(txt):
    return re.findall(r"[\w']+|[.,!?; -—–\n]", txt)
    
def tokenize_fast(line,lower=False):
	line = line.lower() if lower else line
	import re
	# tokenize using reg ex (fast)
	tokens = re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line)
	# remove punctuation on either end
	from string import punctuation
	tokens = [tok.strip(punctuation) for tok in tokens]
	# make sure each thing in list isn't empty
	tokens = [tok for tok in tokens if tok]
	return tokens

# tokenize
def tokenize_nltk(txt,lower=False):
	# lowercase
	txt_l = txt.lower() if lower else txt
	# use nltk
	tokens = nltk.word_tokenize(txt_l)
	# weed out punctuation
	tokens = [
		tok
		for tok in tokens
		if tok[0].isalpha()
	]
	# return
	return tokens

def tokenize(txt,*x,**y):
	return tokenize_fast(txt,*x,**y)




def filter_freqs(freqs,modernize=False,lower=True):
	from collections import Counter
	cd=Counter()
	if modernize: mod=get_spelling_modernizer()
	for w,c in sorted(freqs.items(),key=lambda x: -x[1]):
		if lower: w=w.lower()
		if modernize: w=mod.get(w,w)
		cd[w]+=c
	return cd





def save_freqs_json(obj, lower=True):
	from collections import Counter
	import ujson as json

	ifnfn,ofnfn,tokenizer=obj
	if not os.path.exists(ifnfn): return
	# if os.path.exists(ofnfn): return
	if tokenizer is None: tokenizer=tokenize

	opath = os.path.dirname(ofnfn)
	try:
		if not os.path.exists(opath): os.makedirs(opath)
	except FileExistsError:
		pass

	# read txt
	with open(ifnfn,encoding='utf-8',errors='replace') as f: txt=f.read()
	
	# tokenize
	if lower: txt=txt.lower()
	toks=tokenizer(txt)
	#print(len(toks),ofnfn)
	
	# count
	tokd=dict(Counter(toks))
	
	# save
	with open(ofnfn,'w') as of: json.dump(tokd,of)
	
	# return?
	# return tokd

