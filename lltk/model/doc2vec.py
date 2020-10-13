from __future__ import absolute_import
from __future__ import print_function
from lltk.model import Model
from lltk import tools
from gensim.models.doc2vec import LabeledSentence
import six
from six.moves import range
class SentenceSampler(Model):
	def __init__(self, fn, num_skips_wanted=True,min_words=50,min_words_in_sent=6,save_key=False, key_fn=None,save_word2vec=False):
		self.fn=fn
		self.num_skips_wanted=num_skips_wanted
		self.line_nums_wanted=None
		self.min_words=min_words
		self.min_words_in_sent=min_words_in_sent
		self.save_key=save_key
		self.key_fn=self.fn.replace('.txt','.key.txt')
		self.save_word2vec=save_word2vec

	def __iter__(self):
		i=0
		import codecs
		fkey=codecs.open(self.key_fn,'w',encoding='utf-8') if self.save_key else None
		fw2v=codecs.open(self.fn.replace('.txt','.word2vec-input.txt'),'w',encoding='utf-8') if self.save_word2vec else None
		f=codecs.open(self.fn,encoding='utf-8')


		words=[]
		line_nums=[]
		tagnum=0
		for i,line in enumerate(f):
			if self.save_key:
				if line.startswith('#'): continue
				linelist=eval(line)
				if len(linelist)<self.min_words_in_sent: continue #print linelist
				linelist2 = []
				for x in linelist:
					x=x.strip().lower()
					if not x: continue
					if not x.isalpha(): continue
					if u'\u2014' in x:
						linelist2+=x.replace(u'\u2014', ' -- ').split()
					else:
						linelist2+=[x]
				words+=linelist2
				line_nums+=[i+1]
			else:
				linelist2 = line.strip().split('\t')[-1].split()
				words+=linelist2
				line_nums+=line.strip().split('\t')[1].split()


			if len(words)>self.min_words:
				tagnum+=1
				yield LabeledSentence(words=words, tags=[tagnum])

				if self.save_word2vec:
					fw2v.write('PARA_'+str(tagnum)+' '+(' '.join(words).replace('\n',' ').replace('\r',' ').replace('\t',' '))+'\n')
				if self.save_key:
					fkey.write('PARA_'+str(tagnum)+'\t'+' '.join(str(x) for x in line_nums)+'\t'+(' '.join(words).replace('\n',' ').replace('\r',' ').replace('\t',' '))+'\n')

				words=[]
				line_nums=[]

				#if i>1000: break

class Doc2Vec(Word2Vec):
	def __init__(self, corpus, skipgram_n=10, name=None, fn=None, skipgram_fn=None, vocab_size=20000, num_skips_wanted=None):
		"""Initialize a Word2Vec object.

		Each W2V object...
		- must be immediately associated with a Corpus object (corpus).
		- has a set skipgram size (skipgram_n)
		- has a name (if not set, will default to its corpus' name, unless a filename [fn] is supplied)

		Some W2V objects...
		- can have a fn: initialized straight from file (already made, just loading).
		- can have a mongo_q: a custom mongo query to the skipgram mongo database.
		- can have a skipgram_fn: not made yet, but when done so, will use a .txt.gz file instead of the database (useful for very large corpora).
		"""

		self.corpus=corpus
		if name:
			self.name=name
		elif fn:
			self.name=fn.split('word2vec.')[1].split('.skipgram_n=')[0]
		else:
			self.name=self.corpus.name
		self.skipgram_n=skipgram_n
		self._gensim=None
		self.mfw_d=DEFAULT_MFWD
		self.path_model = self.corpus.path_model
		self._fn=fn
		self.skipgram_fn=skipgram_fn.replace('.gz','')
		self.vocab_size=vocab_size
		self.num_skips_wanted=num_skips_wanted


	def save_keys(self):
		skips = SentenceSampler(self.skipgram_fn,save_key=True,save_word2vec=True)
		for skip in skips:
			pass


	def model(self,num_workers=8,num_dimensions=300,num_iter=100):
		fn_w2v_i=self.skipgram_fn.replace('.txt','.word2vec-input.txt')
		path_w2v_i,fname_w2v_i = os.path.split(fn_w2v_i)

		if not os.path.exists(fn_w2v_i):
			self.save_keys()

		fn_w2v_o=self.fnfn
		cmd=['cd',path_w2v_i,'&&',PATH_TO_WORD2VEC_BINARY, '-train', fname_w2v_i, '-output', fn_w2v_o, '-cbow', '1', '-size', str(num_dimensions), '-window', str(self.skipgram_n), '-negative', '5', '-hs', '1', '-sample', '1e-4', '-threads', '8', '-binary', '1', '-iter', str(num_iter), '-min-count', '1', '-sentence-vectors', '1']

		cmdstr=' '.join(cmd)
		print(cmdstr)
		os.system(cmdstr)
		print()
		print('>> saved:',fn_w2v_o)



	"""def model_gensim(self, num_workers=8, min_count=10, num_dimensions=300, num_iter=10):
		#
		#Train the model!
		#
		logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

		skips1 = SentenceSampler(self.skipgram_fn,save_key=True)
		skips2 = SentenceSampler(self.skipgram_fn,save_key=False)

		m = self._gensim = gensim.models.Doc2Vec(workers=num_workers, min_count=min_count, size=num_dimensions, window=self.skipgram_n, dm=0, alpha=0.025, min_alpha=0.025,dbow_words=1)

		m.build_vocab(skips1)
		assert 1 == 2

		for epoch in range(num_iter):
			print '>>>> EPOCH #',epoch
			m.train(skips2)
			m.alpha -= 0.002  # decrease the learning rate
			m.min_alpha = m.alpha  # fix the learning rate, no decay

		return self._gensim
	"""


	@property
	def fn(self):
		if not self._fn:
			fn_txt='doc2vec.{0}.skipgram_n={1}.model.txt'.format(self.name, self.skipgram_n)
			fn_gz=fn_txt+'.gz'
			self._fn=fn_gz.replace('.txt.gz','.bin')
		return self._fn

	@property
	def fn_vocab(self):
		if not hasattr(self,'_fn_vocab'):
			fn_txt='doc2vec.{0}.skipgram_n={1}.model.vocab.txt'.format(self.name, self.skipgram_n)
			self._fn_vocab=fn_txt
		return self._fn_vocab

	def save(self, gensim_format=True, word2vec_format=True):
		#self.gensim.save(os.path.join(odir,self.fn))
		#self.gensim.init_sims(replace=True)
		#self.gensim.save(self.fnfn.replace('.txt.gz','3'))
		return

	"""def load_gensim(self):
		if not self._gensim is None: return True
		then=time.time()
		self._gensim = gensim.models.Doc2Vec.load(self.fnfn.replace('.txt.gz','3'))
		now=time.time()
		print '>> done loading doc2vec model:',self.fn,'['+str(round(now-then,1))+' seconds]'"""

	def load(self):
		if not self._gensim is None: return True
		then=time.time()
		self._gensim = gensim.models.Word2Vec.load_word2vec_format(self.fnfn,binary=True)
		now=time.time()
		#print '>> done loading doc2vec model:',self.fn,'['+str(round(now-then,1))+' seconds]'


	def similar_sents(self,words,topn=10,infer_vector=False,in_top_mfw=50000,word_csim_min=0.2,docs_sofar=set(),allow_words=False):
		import linecache,numpy as np
		fn=self.skipgram_fn


		mfw=set(self.mfw(n=in_top_mfw,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)) if in_top_mfw else set()
		#print len(mfw),type(mfw)

		words_included = []
		if type(words) in [np.ndarray]:
			wordvecs=[words]
		elif type(words) in [int]:
			wordvecs=['PARA_'+str(words)]
		elif type(words) in [list,tuple]:
			if infer_vector:
				wordvecs=[self.gensim.infer_vector(words)]
			else:
				wordvecs=[self.gensim[w.lower()] for w in words if w in self.gensim]
				words_included+=[w.lower() for w in words]
				#wordvecs=words
		elif type(words) in [str,six.text_type]:
			if infer_vector:
				wordvecs=[self.gensim.infer_vector(words)]
			else:

				wordl=words.split()
				if set(wordl) & {'+','-','/','*'}:
					variables={}
					for w in wordl:
						if w[0].isalpha() and w[-1].isalpha() and w in self.gensim:
							variables[w]=self.gensim[w]
							words_included+=[w]
					wordvecs = [parse_math_str(words, variables=variables)]
				else:
					#wordvecs=[w.lower() for w in wordl if w.lower() in self.gensim]
					wordvecs=[self.gensim[w.lower()] for w in words.split() if w.lower() in self.gensim]
					words_included+=[w.lower() for w in words.split()]
		else:
			return []

		if not wordvecs: return []
		wordvec=np.mean(wordvecs,0)

		tuples = self.gensim.most_similar(wordvecs,topn=topn*100)
		if type(wordvecs) in [int]:
			tuples.insert(0,(wordvecs,1.0))

		numfound=0
		o=[]
		for i,(docid,csim) in enumerate(tuples):
			if not docid.startswith('PARA_'): continue
			if docid in docs_sofar: continue
			if numfound>topn: break
			#print '>>>>',i,docid,csim
			doci=int(docid.split('_')[1])
			line=linecache.getline(fn.replace('.txt','.key.txt'),doci)
			sentids = [int(x) for x in line.split('\t')[1].split()]
			#words = line.split('\t')[2].strip().split()

			ol=[]
			textid=None
			for x in range(sentids[0], sentids[-1]+1):
				minusx=0
				while not textid:
					minusx+=1
					## go up to find it
					line=linecache.getline(fn,x-minusx)
					if line.startswith('# '):
						textid=line[2:].strip()

				line=linecache.getline(fn,x)
				if line.startswith('#'): break
				line=eval(line)

				for w in line:
					_csim=1-cosine(wordvec,self.gensim[w]) if w in self.gensim else 0
					wdx={'word':w, 'csim':_csim, 'doc':docid, 'doc_csim':csim}
					ol+=[wdx]

			## Check MFW
			if mfw and False in [(not wd['word'].isalpha() or wd['word'].lower() in mfw) for wd in ol]:
				#print "doesn't work:"
				#print [wd['word'] for wd in ol]
				#for wd in ol:
				#	print wd['word'],wd['word'].isalpha(), wd['word'].lower() in mfw
				#print
				continue

			if word_csim_min and not True in [wd['csim']>=word_csim_min for wd in ol]: continue

			if not allow_words:
				passage_includes_word=False
				for w in words_included:
					if True in [wd['word'].startswith(w) or w.startswith(wd['word']) for wd in ol]:
						passage_includes_word=True
				if passage_includes_word: continue

			#footer="\n-- "+
			#sent.insert(0,{'word':sent[0]['doc']+' '+str(round(sent[0]['doc_csim'],2)), 'csim':0})
			text=self.corpus.text(textid)
			#footer='-- '+text.title+' ('+text.author+')'+' ['+str(text.year)+'] ['+docid+' ('+str(round(csim,2))+')]'
			#footer=' -- '+text.author+'\n'+('. ' if not text.author.endswith('.') else ' ')+text.title+' ('+str(text.year)+')' #\n ['+docid+' ('+str(round(csim,2))+')]'
			footer=['\n\n',' -- ', text.author, '\n'] + text.title.split() + [' ('+str(text.year)+')'] #

			#for w in ['\n\n']+footer.split():
			for w in ['\n\n']+footer:
				ol+=[{'word':w,'csim':0}]


			numfound+=1
			o+=[ol]

			#print
			#print
		return o



def getdoc(docid):
	line=linecache.getline('sentences.ECCO-TCP.key3.txt',docid)
	sentids = [int(x) for x in line.split('\t')[1].split()]
	words = line.split('\t')[2].strip().split()
	print(sentids)

	Line=[]
	for x in sentids:
		line=eval(linecache.getline('sentences.ECCO-TCP.txt',x))
		Line+=line

	print(tools.toks2str(Line))


def word2doc(m,word,nummax=10):
	num=0
	for x,y in m.most_similar(word,topn=10000):
		if x.startswith('SENT_'):
			num+=1
			if num>nummax: break

			idx=int(x.split('_')[1])
			print(x,y)
			getdoc(idx)
			print()



def parse_math_str(input_string,variables={}):

	# Uncomment the line below for readline support on interactive terminal
	# import readline
	import re
	from pyparsing import Word, alphas, ParseException, Literal, CaselessLiteral, Combine, Optional, nums, Or, Forward, ZeroOrMore, StringEnd, alphanums
	import math

	# Debugging flag can be set to either "debug_flag=True" or "debug_flag=False"
	debug_flag=False

	exprStack = []
	varStack  = []

	def pushFirst( str, loc, toks ):
		exprStack.append( toks[0] )

	def assignVar( str, loc, toks ):
		varStack.append( toks[0] )

	# define grammar
	point = Literal('.')
	e = CaselessLiteral('E')
	plusorminus = Literal('+') | Literal('-')
	number = Word(nums)
	integer = Combine( Optional(plusorminus) + number )
	floatnumber = Combine( integer + Optional( point + Optional(number) ) + Optional( e + integer ) )

	ident = Word(alphas,alphanums + '_')

	plus  = Literal( "+" )
	minus = Literal( "-" )
	mult  = Literal( "*" )
	div   = Literal( "/" )
	lpar  = Literal( "(" ).suppress()
	rpar  = Literal( ")" ).suppress()
	addop  = plus | minus
	multop = mult | div
	expop = Literal( "^" )
	assign = Literal( "=" )

	expr = Forward()
	atom = ( ( e | floatnumber | integer | ident ).setParseAction(pushFirst) |
					 ( lpar + expr.suppress() + rpar )
				 )

	factor = Forward()
	factor << atom + ZeroOrMore( ( expop + factor ).setParseAction( pushFirst ) )

	term = factor + ZeroOrMore( ( multop + factor ).setParseAction( pushFirst ) )
	expr << term + ZeroOrMore( ( addop + term ).setParseAction( pushFirst ) )
	bnf = Optional((ident + assign).setParseAction(assignVar)) + expr

	pattern =  bnf + StringEnd()

	# map operator symbols to corresponding arithmetic operations
	opn = { "+" : ( lambda a,b: a + b ),
					"-" : ( lambda a,b: a - b ),
					"*" : ( lambda a,b: a * b ),
					"/" : ( lambda a,b: a / b ),
					"^" : ( lambda a,b: a ** b ) }

	# Recursive function that evaluates the stack
	def evaluateStack( s ):
		op = s.pop()
		if op in "+-*/^":
			op2 = evaluateStack( s )
			op1 = evaluateStack( s )
			return opn[op]( op1, op2 )
		elif op == "PI":
			return math.pi
		elif op == "E":
			return math.e
		elif re.search('^[a-zA-Z][a-zA-Z0-9_]*$',op):
			if op in variables:
				return variables[op]
			else:
				return 0
		elif re.search('^[-+]?[0-9]+$',op):
			return int( op )
		else:
			return float( op )

	# Start with a blank exprStack and a blank varStack
	exprStack = []
	varStack  = []

	if input_string != '':
		# try parsing the input string
		try:
			L=pattern.parseString( input_string )
		except ParseException as err:
			L=['Parse Failure',input_string]

		# show result of parsing the input string
		if debug_flag: print(input_string, "->", L)
		if len(L)==0 or L[0] != 'Parse Failure':
			if debug_flag: print("exprStack=", exprStack)

			# calculate result , store a copy in ans , display the result to user
			result=evaluateStack(exprStack)
			variables['ans']=result
			#print result
			return result

			# Assign result to a variable if required
			if debug_flag: print("var=",varStack)
			if len(varStack)==1:
				variables[varStack.pop()]=result
			if debug_flag: print("variables=",variables)
		else:
			print('Parse Failure')
			print(err.line)
			print(" "*(err.column-1) + "^")
			print(err)
