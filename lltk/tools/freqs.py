from __future__ import absolute_import

import codecs,configparser,os,re
from collections import defaultdict

LIT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
from .tools import config

def measure_fields(word_counts,fields={},only_fields=None):
	if not fields: fields=get_fields()
	word_types=set(word_counts.keys())

	cd={}
	for fieldname,fieldwords in list(fields.items()):
		count=0
		if only_fields and not fieldname in only_fields: continue
		for matching_word in fieldwords & word_types:
			count+=word_counts[matching_word]
		cd[fieldname]=count

	return cd

def get_field2words(fnfn=None,only_fields={},only_pos={},word2fields={},sep='\t'):
	#print('1',fnfn)
	if not fnfn: fnfn=config.get('PATH_TO_FIELDS')
	#print('2',fnfn)
	if not fnfn: return {}
	if not fnfn.startswith(os.path.sep): fnfn=os.path.join(LIT_ROOT,fnfn)
	#print('3',fnfn)

	from collections import defaultdict
	fd=field2words=defaultdict(set)
	if only_pos: w2pos=get_word2pos()
	with open(fnfn) as file:
		header=None
		for i,ln in enumerate(file):
			dat=ln.strip().split(sep)
			if not header:
				header=dat
				continue
			d=dict(zip(header,dat))
			field=d.get('field','')
			if not field: continue
			if only_fields and not field in only_fields: continue
			words=set(d.get('words','').split())
			field2words[field]=words
			if only_pos:
				field2words[field]={w for w in field2words[field] if True in [w2pos.get(w,'').startswith(x) for x in only_pos]}

	return field2words



def get_fields0(fnfn=None,only_fields={},only_pos={},word2fields={}):
	if not fnfn: fnfn=config.get('PATH_TO_FIELDS')
	if not fnfn: return {}
	from collections import defaultdict
	field2words=defaultdict(set)
	if only_pos: w2pos=get_word2pos()

	word2fields = get_word2fields(only_fields=only_fields) if not word2fields else word2fields

	for word,fields in list(word2fields.items()):
		if only_pos:
			pos=w2pos.get(w,'')
			if True not in [pos.startswith(x) for x in only_pos]:
				continue

		for f in fields:
			field2words[f]|={word}

	return field2words

# def get_word2pos(worddb=None, word_col='word', pos_col='pos_byu'):
# 	from .tools import worddf
# 	df=worddf()
# 	return dict(zip(df[word_col],df[pos_col]))

def get_word2fields(fnfn=None,only_fields={},only_pos={'n','v','r','j'}):
	word2fields=defaultdict(set)
	field2words=get_field2words(only_fields=only_fields, only_pos=only_pos)
	for field,words in field2words.items():
		for word in words:
			word2fields[word]|={field}
	return word2fields



get_fields = get_field2words
