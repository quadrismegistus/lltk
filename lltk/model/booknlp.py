from __future__ import absolute_import
from __future__ import print_function
from lltk.config import settings
import os
PATH_TO_BOOKNLP_BINARY = settings['PATH_TO_BOOKNLP_BINARY']

class BookNLP(object):
	def __init__(self,text=None,path_tokens=None,path_json=None):
		self.text=text
		self._path_tokens=path_tokens
		self._path_json=path_json

	@property
	def path_txt(self):
		return self.text.path_txt

	@property
	def path_default(self):
		return os.path.join(self.text.corpus.path,'booknlp',self.text.id)

	@property
	def path_output(self):
		return os.path.split(self.path_tokens)[0]

	@property
	def path_tokens(self):
		return self._path_tokens if self._path_tokens else os.path.join(self.path_default,'tokens.txt')

	@property
	def path_json(self):
		return self._path_tokens if self._path_tokens else os.path.join(self.path_default,'book.json')

	def gen(self):
		if not os.path.exists(self.path_output): os.makedirs(self.path_output)
		return parse_text(self.path_txt, self.path_output, self.path_tokens)

	def charfreqs(self):
		import json,codecs
		from nltk import word_tokenize
		from collections import Counter,defaultdict
		dat=json.load(codecs.open(self.path_json,encoding='utf-8'))
		keys=[]
		nullchar=defaultdict(Counter)
		for char in dat['characters']:
			if not char['names']: continue
			names=[x['n'] for x in char['names']]

			for datatype,data in list(char.items()):
				if type(data)!=list: continue
				words=[]
				for event in data:
					if 'w' in event:
						wtxt=event['w']
						wlist=word_tokenize(wtxt) if ' ' in wtxt else [wtxt]
						wlist=[w.lower() for w in wlist if w and w[0].isalpha()]
						words+=wlist

				freqs=Counter(words)
				nullchar[datatype].update(freqs)
				sumfreq=float(sum(freqs.values()))
				for k,v in list(freqs.items()):
					odx={
						'text_id':self.text.id if self.text else '',
						'char_id':char['id'],
						'names':names, 'name':names[0],
						'context':datatype,
						'context_sum':sumfreq,
						'word':k,
						'count':v,
						'tf':v/sumfreq
					}
					yield odx


		for datatype,freqs in list(nullchar.items()):
			sumfreq=float(sum(freqs.values()))
			for k,v in list(freqs.items()):
				odx={
					'text_id':self.text.id if self.text else '',
					'char_id':None,
					'names':['All Characters'], 'name':'All Characters',
					'context':datatype,
					'context_sum':sumfreq,
					'word':k,
					'count':v,
					'tf':v/sumfreq
				}
				yield odx



def parse_text(path_txt,path_output,path_tokens):
	print(PATH_TO_BOOKNLP_BINARY+' novels/BookNLP -doc {0} -printHTML -p {1} -tok {2} -f'.format(
			path_txt,
			path_output,
			path_tokens
	))
