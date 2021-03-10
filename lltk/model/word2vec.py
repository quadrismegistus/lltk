from __future__ import absolute_import
from __future__ import print_function
import six
from six.moves import range
from six.moves import zip
# -*- coding: utf-8 -*-

PATH_TO_WORD2VEC_BINARY = '/Users/ryan/DH/github/word2vec/bin/word2vec'

TOP2000=dict(n=2000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP5000=dict(n=5000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP10000=dict(n=10000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP2000ABSNOUN=dict(n=2000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=True)
TOP1000ABSNOUN=dict(n=1000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=True)
TOP50000ALLWORDS = dict(n=50000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP2000SINGNOUN=dict(n=2000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=None)


TOPALL=dict(n=None,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)

DEFAULT_MFWD=TOPALL

import os,gensim,logging,time,numpy as np,random
from lltk.model import Model
from scipy.spatial.distance import cosine,pdist,squareform,cdist
from scipy.stats import pearsonr,spearmanr
import multiprocessing as mp,gzip,random,time
from lltk import tools

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)


#KEYWORDS = {'art','class','culture','democracy','common','economy','economic','genius','humanity','industry','labour','liberty','literature','opinion','public','private','virtue','taste'}
#KEYWORDS_OTHER = {'passion','abstract','abstraction','literature','writing','poetry','novel','romance','adventure','history','literary','poetic','beautiful','sublime','historical','fiction','fictional','novelistic','invisible','virtual','invisibility','agent','agency','london','suspence','suspense','suspenseful','suspenceful'}

# KEYWORDS = {'art_n','class_n','culture_n','democracy_n','common_j','economy_n','economic_j','genius_n','humanity_n','industry_n','labour_n','liberty_n','literature_n','opinion_n','public_j','public_n','private_j','virtue_n','taste_n'}
# KEYWORDS_OTHER = {'passion_n','abstract_j','abstraction_n','literature_n','writing_n','poetry_n','novel_n','romance_n','adventure_n','history_n','literary_j','poetic_j','beautiful_j','sublime_j','historical_j','fiction_n','fictional_j','novelistic_j','invisible_j','virtual_j','invisibility_n','agent_n','agency_n','london_n','suspence_n','suspense_n','suspenseful_j','suspenceful_j'}
# KEYWORDS_BECOMING_ABSTRACT = {'attachment_n', 'plans_n', 'plan_n', 'talents_n', 'plunder_n', 'alarm_n', 'situations_n', 'countrymen_n', 'attack_n', 'situation_n', 'individual_n', 'agriculture_n', 'taste_n', 'attacks_n', 'chance_n', 'fate_n', 'basis_n', 'spirit_n', 'career_n', 'sacrifices_n', 'habit_n', 'rank_n', 'worth_n', 'scenes_n', 'fund_n', 'sex_n', 'woes_n', 'charms_n', 'horrors_n', 'tribunal_n', 'wars_n', 'source_n', 'infancy_n', 'disease_n', 'income_n', 'sources_n', 'tyrants_n', 'annals_n', 'manufactures_n', 'marriage_n', 'sacrifice_n', 'delivery_n', 'revenues_n', 'almighty_n', 'term_n', 'tribute_n', 'murder_n', 'proprietors_n', 'enemies_n', 'complexion_n', 'classes_n', 'debt_n', 'bliss_n', 'woe_n', 'humour_n', 'art_n', 'libel_n', 'thirst_n', 'profits_n', 'pressure_n', 'uses_n', 'experiment_n', 'organs_n', 'passages_n', 'sovereign_n', 'moments_n', 'accident_n', 'testament_n', 'forfeiture_n', 'france_n', 'statute_n', 'secret_n', 'sickness_n', 'right_n', 'sums_n'}

KEYWORDS = {'value','interest','commerce','art','class','culture','democracy','common','economy','economic','genius','humanity','industry','labour','liberty','literature','opinion','public','public','private','virtue','taste'}
KEYWORDS_OTHER = {'passion','abstract','abstraction','literature','writing','poetry','novel','romance','adventure','history','literary','poetic','beautiful','sublime','historical','fiction','fictional','novelistic','invisible','virtual','invisibility','agent','agency','london','suspence','suspense','suspenseful','suspenceful'}
KEYWORDS_BECOMING_ABSTRACT = {'attachment', 'plans', 'plan', 'talents', 'plunder', 'alarm', 'situations', 'countrymen', 'attack', 'situation', 'individual', 'agriculture', 'taste', 'attacks', 'chance', 'fate', 'basis', 'spirit', 'career', 'sacrifices', 'habit', 'rank', 'worth', 'scenes', 'fund', 'sex', 'woes', 'charms', 'horrors', 'tribunal', 'wars', 'source', 'infancy', 'disease', 'income', 'sources', 'tyrants', 'annals', 'manufactures', 'marriage', 'sacrifice', 'delivery', 'revenues', 'almighty', 'term', 'tribute', 'murder', 'proprietors', 'enemies', 'complexion', 'classes', 'debt', 'bliss', 'woe', 'humour', 'art', 'libel', 'thirst', 'profits', 'pressure', 'uses', 'experiment', 'organs', 'passages', 'sovereign', 'moments', 'accident', 'testament', 'forfeiture', 'france', 'statute', 'secret', 'sickness', 'right', 'sums'}

KEYWORDS|=KEYWORDS_OTHER
KEYWORDS|=KEYWORDS_BECOMING_ABSTRACT

class Word2Vec(Model):
	def __init__(self, corpus=None, skipgram_n=10, name=None, mongo_q={}, fn=None, skipgram_fn=None, vocab_size=20000, num_skips_wanted=None,period=None):
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
			try:
				self.name=fn.split('word2vec.')[1].split('.skipgram_n=')[0]
			except IndexError:
				self.name=fn
		else:
			self.name=self.corpus.name
		self.period = period
		self.skipgram_n=skipgram_n
		self._gensim=None
		self.mfw_d=DEFAULT_MFWD
		self.path_model = self.corpus.path_model if self.corpus else ''
		self.mongo_q = mongo_q
		self._fn=fn
		self.skipgram_fn=skipgram_fn
		self.vocab_size=vocab_size
		self.num_skips_wanted=num_skips_wanted

	@property
	def run_str(self):
		run_l = [x for x in self.fn.split('.') if x.startswith('run')]
		if not run_l: return None
		return ''.join([x for x in run_l[0] if x.isdigit()])

	@property
	def run_num(self):
		#if not '.run_' in self.fn: return None
		#return int(self.fn.split('.run_')[1].split('.')[0])
		#run_l = [x for x in self.fn.split('.') if x.startswith('run')]
		#if not run_l: return None
		return int(self.run_str)



	def model(self, num_workers=8, min_count=10, num_dimensions=100, sg=1, num_epochs=None):
		"""
		Train the model!
		"""

		if not self.skipgram_fn:
			print('>> skipgram generator: SkipgramsCorpus')
			skips = SkipgramsCorpus(corpus=self.corpus, n=self.skipgram_n)
		elif not self.num_skips_wanted:
			print('>> skipgram generator: LineSentence')
			skips = gensim.models.word2vec.LineSentence(self.skipgram_fn)
		else:
			skips = SkipgramsSampler(self.skipgram_fn, self.num_skips_wanted)

		self._gensim = gensim.models.Word2Vec(skips, workers=num_workers, sg=sg, min_count=min_count, size=num_dimensions, window=self.skipgram_n)
		return self._gensim

	@property
	def exists(self):
		return os.path.exists(self.fnfn)

	@property
	def fn(self):
		if not self._fn:
			fn_txt='word2vec.{0}.skipgram_n={1}.model.txt'.format(self.name, self.skipgram_n)
			fn_gz=fn_txt+'.gz'
			self._fn=fn_gz
		return self._fn

	@property
	def fn_vocab(self):
		#if not hasattr(self,'_fn_vocab'):
		#	fn_txt='word2vec.{0}.skipgram_n={1}.model.vocab.txt'.format(self.name, self.skipgram_n)
		#	self._fn_vocab=fn_txt
		#return self._fn_vocab
		return os.path.basename(self.fnfn).replace('.txt.gz','.vocab.txt')

	@property
	def fnfn(self):
		return os.path.join(self.path,self.fn)

	@property
	def fnfn_vocab(self): return os.path.join(self.path,self.fn_vocab)

	@property
	def path(self):
		return self.path_model



	@property
	def named(self):
		d={}
		for i,x in enumerate(self.name.split('.')):
			d['model_name_'+str(i+1)]=x
		return d

	def save(self, odir=None, gensim_format=True, word2vec_format=True):
		if not odir: odir=self.path
		#self.gensim.save(os.path.join(odir,self.fn))
		#self.gensim.init_sims(replace=True)
		self.gensim.save_word2vec_format(os.path.join(odir,self.fn), os.path.join(odir,self.fn_vocab))

	def load(self):
		if not self._gensim is None: return True
		then=time.time()
		if os.path.exists(self.fnfn_vocab):
			print('>> loading word2vec model [{0}] [{1}]'.format(self.fnfn,os.path.basename(self.fnfn_vocab)))
			try:
				self._gensim=gensim.models.KeyedVectors.load_word2vec_format(self.fnfn, self.fnfn_vocab, unicode_errors='ignore')
			except ValueError:
				self._gensim=gensim.models.KeyedVectors.load_word2vec_format(self.fnfn, self.fnfn_vocab, unicode_errors='ignore',binary=True)
		else:
			print('>> loading word2vec model [{0}]'.format(self.fnfn))
			try:
				self._gensim=gensim.models.KeyedVectors.load_word2vec_format(self.fnfn, unicode_errors='ignore')
			except ValueError:
				self._gensim=gensim.models.KeyedVectors.load_word2vec_format(self.fnfn, unicode_errors='ignore',binary=True)
		self._gensim.init_sims(replace=True)
		now=time.time()
		print('>> done loading word2vec model:',os.path.basename(self.fnfn),'['+str(round(now-then,1))+' seconds]')

	@property
	def gensim(self):
		if self._gensim is None: self.load()
		return self._gensim

	def unload(self):
		self._gensim=None
		for k in ['_freqs','_vocab','_vocabset','_word2index','_word2array','_word2word_pos','_num_words']:
			if hasattr(self,k):
				delattr(self,k)

	@property
	def freqs(self):
		if not hasattr(self,'_freqs'):
			self._freqs=d={}
			for word,vocabobj in list(self.gensim.vocab.items()):
				count=vocabobj.count
				if not count: continue
				d[word]=count
		return self._freqs

	@property
	def vocab(self):
		if not hasattr(self,'_vocab'):
			#self._vocab=set([w for w,c in sorted(self.freqs.items(),key=lambda _lt: -_lt[1])[:self.vocab_size]])
			self._vocab=[w for w,c in sorted(list(self.freqs.items()),key=lambda _lt: -_lt[1])]
		return self._vocab

	@property
	def vocabset(self):
		if not hasattr(self,'_vocabset'):
			self._vocabset=set(self.vocab)
		return self._vocabset

	def reset_vocab(self):
		self._freqs=d={}
		for word,vocabobj in list(self.gensim.vocab.items()):
			d[word]=vocabobj.count
		self._vocab=[w for w,c in sorted(list(self.freqs.items()),key=lambda _lt: -_lt[1])]
		self._vocabset=set(self.vocab)


	@property
	def word2index(self):
		if not hasattr(self,'_word2index'):
			self._word2index=w2i={}
			for word,vocabobj in list(self.gensim.vocab.items()):
				index=vocabobj.index
				w2i[word]=index
		return self._word2index

	@property
	def word2array(self):
		if not hasattr(self,'_word2array'):
			self._word2array=w2a={}
			for word,index in list(self.word2index.items()):
				#w2a[word]=self.gensim.syn0[index]
				w2a[word]=self.gensim[word]
		return self._word2array

	def align(self,other,words=None):
		"""
		1) Intersect vocabularty with other word2vec model so that only shared words remain
		2) Procrustes-align other's word2vec array to this one so that wordX can be compared across self and other
		"""
		smart_procrustes_align_gensim(self.gensim, other.gensim, words=words)
		self.reset_vocab()
		other.reset_vocab()

	def reject_words(self,words=[]):
		reject_words_from_matrix(self.gensim, words=words)

	def reject_binaries(self,binaries=[]):
		reject_binaries_from_matrix(self.gensim, binaries=binaries)

	def limit_by_fpm(self,min_fpm=1):
		return self.limit_vocab(fpm_cutoff=min_fpm,n=None)

	def limit_by_rank(self,max_rank):
		return self.limit_vocab(n=max_rank,fpm_cutoff=None)

	def limit_by_count(self,min_count=100):
		return self.limit_vocab(min_count=min_count,fpm_cutoff=None,n=None)

	def mfw_in_model(self,n=None):
		for i,(w,c) in enumerate(sorted(list(self.freqs.items()), key=lambda _t: -_t[1])):
			if n and i>=n: break
			yield w

	def remove_non_english(self):
		from lltk import tools
		eng = tools.get_wordlist(lang=self.corpus.lang)
		logging.info('>> removing non-english words...')
		self.limit_vocab(words=eng)

	def limit_vocab(self,n=None,fpm_cutoff=1.0,words=None,english=False,min_count=None):
		#words = set(list(self.mfw())) if not words else set(words)
		if words:
			pass
		elif english:
			return self.remove_non_english()
		elif min_count:
			words = set([w for w in self.gensim.vocab if self.gensim.vocab[w].count>=min_count])
		elif fpm_cutoff:
			words = set([w for w in self.gensim.vocab if self.gensim.vocab[w].count/float(self.num_words)*1000000 >= fpm_cutoff])
		elif n:
			words = set(list(self.mfw_in_model(n=n))) if not words else set(words)
		else:
			print('!! neither number of words (n) nor fpm_cutoff specified.')
			return

		m=self.gensim

		vocab=self.vocabset
		logging.info('>> model [{0}] originally has {1} words.'.format(self.name, len(vocab)))
		logging.info('>> wordset to check against has {0} words.'.format(len(words)))
		# Find the common vocabulary
		common_vocab = vocab & words
		logging.info('>> size of vocab in common is {0} words'.format(len(common_vocab)))

		# What are we removing?
		#for wrd in vocab-words:
			#logging.debug('>> removing word: '+str(wrd))



		# If no reduction necessary
		if not vocab-common_vocab:
			return


		# Otherwise sort by frequency (summed for both)
		common_vocab = list(common_vocab)
		common_vocab.sort(key=lambda w: m.vocab[w].count,reverse=True)

		# Replace old syn0norm array with new one (with common vocab)
		indices = [m.vocab[w].index for w in common_vocab]
		old_arr = m.syn0norm if hasattr(m,'syn0norm') else m.syn0
		new_arr = np.array([old_arr[index] for index in indices])
		m.syn0norm = m.syn0 = new_arr

		# Replace old vocab dictionary with new one (with common vocab)
		# and old index2word with new one
		m.index2word = common_vocab
		old_vocab = m.vocab
		new_vocab = {}
		for new_index,word in enumerate(common_vocab):
			old_vocab_obj=old_vocab[word]
			new_vocab[word] = gensim.models.word2vec.Vocab(index=new_index, count=old_vocab_obj.count)
		m.vocab = new_vocab

		self.reset_vocab()
		return

	def semantic_network(self,**attrs):
		from lltk.model.semnet import SemanticNetwork
		"""
		- Words selected using self.mfw() [opts set by self.mfw_d] unless manually supplied (words)
		- Draw a network between all words meeting the conditions:
		-- minimum cosine similarity between words A and B (cosine_cut=0.7)
		-- whether to allow other words (allow_other_words=True) besides those in the MFW
		"""

		sn=SemanticNetwork(model=self)
		#sn.gen(**attrs)
		return sn

	def synonyms(self,words=None,mfw_d=None,cosine_cut=0.7,save=True):
		if not mfw_d: mfw_d=self.mfw_d
		m = self.gensim
		c = self.corpus
		mfw_d=mfw_d if mfw_d else self.mfw_d
		mfw = words if words else c.mfw(**mfw_d)

		old=[]
		for word in mfw:
			real_synonyms=[]
			synonyms = self.similar(word,topn=1000)
			for wordB,similarity in synonyms:
				if similarity>=cosine_cut:
					real_synonyms+=[wordB]

			wdx={'word':word, 'num_synonyms':len(real_synonyms), 'synonyms':real_synonyms}
			old+=[wdx]

		if save: tools.write2('num_synonyms.{0}.txt'.format(self.name), old)
		return old

	def mfw(self,**attrs):
		"""
		For now, force mfw in model
		"""

		# Create mfw option dictionary
		mfw_d=dict(list(self.mfw_d.items()))
		for k,v in list(attrs.items()): mfw_d[k]=v

		"""
		For now, force mfw in model
		"""
		return self.mfw_in_model(n=mfw_d['n'])

		n=mfw_d['n']
		if 'special' in mfw_d:
			special=mfw_d['special']
			del mfw_d['special']
		else:
			special=[]



		print('>> generating mfw for model [{0}]: [{1}]'.format(self.name,mfw_d))
		then=time.time()

		# Get results
		res=[]
		i=0
		#vocabset=set(self.vocab)
		for x in self.corpus.mfw(**mfw_d):
			#if not x in vocabset: continue
			i+=1
			res+=[x]
			if i>=mfw_d['n']: break

		# Add special words
		for w in special:
			if w and not w in res:
				res+=[w]


		now=time.time()
		#print '>> done loading mfw for model [{0}] [{1}]'.format(self.name,str(round(now-then,1))+' seconds')
		return res


	def compare_jaccard(self,other,words=None,topn=100):
		if not words: words=self.mfw()
		wordset=set(words)
		M1=self.gensim
		M2=other.gensim
		old=[]
		for rank,word in enumerate(words):
			print(rank,word,'...')
			if not word in M1 or not word in M2: continue
			l1=M1.most_similar(word,topn=topn*100)
			l2=M2.most_similar(word,topn=topn*100)
			l1=[w for w in l1 if w in wordset][:topn]
			l1=[w for w in l2 if w in wordset][:topn]
			set1=set(l1)
			set2=set(l2)
			jacc = len(set1&set2) / float(len(set1|set2))
			odx={'word':word,'rank':rank+1,'jaccard':jacc}
			old+=[odx]

		tools.write2(self.fn.replace('word2vec.','data.word2vec.jaccard_model_comparison'), old)





	def random_vectors(self,word_set,num=100):
		model=self.gensim
		import random
		vd={}
		for i in range(num):
			a=random.choice(word_set)
			b=random.choice(word_set)

			vd[b+' <> '+a] = model[a] - model[b]
		return vd

	def essential_vectors(self):
		keys = ['Concreteness <> Abstractness', 'Complex Substance (Locke) <> Mixed Modes (Locke)','Private <> Public', 'Virtue <> Vice', 'Woman <> Man', 'Simplicity <> Refinement']
		vd=self.interesting_vectors()
		for badkey in set(vd.keys()) - set(keys): del vd[badkey]
		return vd


	def abstract_vectors(self,only_major=True,include_social=False):
		model = self.gensim
		vd={} # vector dictionary

		from lltk.tools.freqs import get_fields
		fields = get_fields()

		vd['Complex Substance (Locke) <> Mixed Modes (Locke)'] = self.centroid(fields['Locke.MixedMode']) - self.centroid(fields['Locke.ComplexIdeaOfSubstance'])
		vd['Concrete (HGI) <> Abstract (HGI)'] = self.centroid(fields['HGI.Abstract']) - self.centroid(fields['HGI.Concrete'])
		vd['Human (VG)'] = self.centroid(fields['VG.Human'])
		vd['Object (VG) <> Human (VG)'] = self.centroid(fields['VG.Human']) - self.centroid(fields['VG.Object'])
		vd['Animal (VG) <> Human (VG)'] = self.centroid(fields['VG.Human']) - self.centroid(fields['VG.Animal'])
		vd['Vice (HGI) <> Virtue (HGI)']=self.centroid(fields['HGI.Moral.Virtue']) - self.centroid(fields['HGI.Moral.Vice'])
		vd['Human (Man+Woman+Boy+Girl)']=self.centroid(['man','woman','boy','girl'])

		return vd

	def interesting_vectors(self):

		model = self.gensim

		vd={} # vector dictionary

		# Centroids
		vd['Concreteness <> Abstractness']=self.centroid(tools.fields['AbstractValues']) - self.centroid(tools.fields['HardSeed'])
		vd['Concreteness (NN) <> Abstractness (NN)']=self.centroid(tools.fields['AbstractValues_NN']) - self.centroid(tools.fields['HardSeed_NN'])
		vd['Pre1150 <> Post1150'] = self.centroid(tools.fields['WordOrigin_1150-1700']) - self.centroid(tools.fields['WordOrigin_0850-1150'])
		vd['Pre1150 (NN) <> Post1150 (NN)'] = self.centroid(tools.fields['WordOrigin_NN_1150-1700']) - self.centroid(tools.fields['WordOrigin_NN_0850-1150'])
		vd['Complex Substance (Locke) <> Mixed Modes (Locke)'] = self.centroid(tools.fields['Locke_MixedModes']) - self.centroid(tools.fields['Locke_ComplexIdeasOfSubstance'])
		vd['Simple Modes (Locke) <> Mixed Modes (Locke)'] = self.centroid(tools.fields['Locke_MixedModes']) - self.centroid(tools.fields['Locke_SimpleModes'])
		vd['Specific Complex Substance (Locke) <> General Complex Substance (Locke)'] = self.centroid(tools.fields['Locke_ComplexIdeasOfSubstance_General']) - self.centroid(tools.fields['Locke_ComplexIdeasOfSubstance_Specific'])
		#vd['Man+Horse+Dog+Cat+Mouse']=self.centroid(['man','horse','dog','cat','mouse'])

		vd['Vices <> Virtues']=self.centroid(tools.fields['Virtues']) - self.centroid(tools.fields['Vices'])

		# Opp
		oppositions_NOT_included="""
		Queen <> King
		Spotless <> Hollow
		Courteousness <> Formality
		Knowledge <> Integrity
		Bank <> River
		Bank <> Banker
		South <> North
		East <> West
		England <> Scotland
		England <> Europe
		England <> America
		Home <> Abroad
		English <> Oriental
		European <> Oriental
		England <> Asia
		Europe <> Asia
		"""


		oppositions="""
		Woman <> Man
		Passion <> Reason
		Simplicity <> Refinement
		Tradition <> Revolution
		Judgment <> Invention
		Genius <> Learning
		Virtue <> Riches
		Virtue <> Honour
		Virtue <> Vice
		Private <> Public
		Folly <> Wisdom
		Romances <> Novels
		Body <> Mind
		Human <> Divine
		Pity <> Fear
		Comedy <> Tragedy
		Tyranny <> Liberty
		Law <> Liberty
		Marvellous <> Common
		Whig <> Tory
		Parliament <> King
		Grief <> Joy
		Sublime/Sublimity <> Beauty/Beautiful
		Ancient/Ancients <> Modern/Moderns
		Passion <> Wit
		Affection <> Imagination
		Wit <> Learning
		Catholic <> Protestant
		England/English <> France/French
		Man/Woman/Person <> Mankind/Community/Society
		Argue/Interfere/Decide <> Argument/Interference/Decision
		Labour <> Work
		Cheerfulness <> Levity
		"""

		nonopps="""
		Eyes
		Man
		Woman
		Genius
		Learning
		"""

		for oppstr in oppositions.strip().split('\n'):
			oppstr=oppstr.strip()
			#print [oppstr]
			if not oppstr: continue
			a,b=oppstr.split(' <> ')
			a=[x for x in a.lower().split('/')]# if x in model]
			b=[x for x in b.lower().split('/')]# if x in model]
			if not a or not b: continue
			av=self.centroid(a)
			bv=self.centroid(b)
			if av is None or bv is None: continue
			vd[oppstr] = bv - av

			"""
			print oppstr
			print a, b
			print vd[oppstr]
			print
			"""

		for wordstr in nonopps.strip().split('\n'):
			wordstr=wordstr.strip()
			wordset=[x.strip() for x in wordstr.lower().split('/')]# if x in model]
			v=self.centroid(wordset)

			#try:
			if v is None or None in v or np.any(np.isnan(v)): continue
			#except TypeError:
			#	print '??',[v]
			vd[wordstr]=v

		return vd

	def cosine_word(self,word,vectord):
		model=self.gensim
		odx={'word':word}
		for vname,vector in list(vectord.items()):
			#print vname
			#print model[word]
			#print vector
			odx[vname]=1 - cosine(model[word], vector)
		return odx

	@property
	def word2word_pos(self):
		if not hasattr(self,'_word2word_pos'):
			from collections import defaultdict
			self._word2word_pos=d=defaultdict(list)
			for word in self.vocab:
				d[word.split('_')[0]]+=[word]
		return self._word2word_pos

	def vector(self,word):
		if word in self.gensim: return self.gensim[word]
		if word in self.word2word_pos: return self.centroid(self.word2word_pos[word])
		return None

	def centroid(self,words):
		vectors=[v for v in [self.vector(w) for w in set(words)] if v is not None]
		if not vectors: return None
		return np.mean(vectors,0)

	def analogy(self, a,b,c,cosmul=True):
		"""
		gensim analogy helper function

		gensim analogy syntax:
		model.most_similar(positive=['woman', 'king'], negative=['man'])
		"""

		model=self.gensim

		neg=[x for x in a] if type(a)==list else [a]
		pos=[]
		pos+=[x for x in b] if type(b)==list else [b]
		pos+=[x for x in c] if type(c)==list else [c]

		if cosmul:
			return model.most_similar_cosmul(positive=pos, negative=neg)

		return model.most_similar(positive=pos, negative=neg)

	def similar(self,words,topn=10):
		words = words if not type(words) in {set,list,tuple} else [w for w in words if w in self.gensim]
		try:
			return self.gensim.most_similar(words,topn=topn)
		except KeyError:
			return []

	def similarity(self,word1,word2_or_model2):
		"""
		Return cosine similarity between word1 and word2
		Unless word2 is actually a second model
			--in which case the distance is between word1 in model1, and word1 in model2
			--this requires the models to be aligned first
		"""
		if type(word2_or_model2) in [Word2Vec]:
			model2=word2_or_model2
			return 1-cosine(self.word2array[word1], model2.word2array[word1])
		else:
			word2=six.text_type(word2_or_model2)
			return self.gensim.similarity(word1,word2)

	@property
	def num_words(self):
		if not hasattr(self,'_num_words'):
			self._num_words = sum([self.gensim.vocab[w].count for w in self.gensim.vocab])
		return self._num_words

	def model_words(self,words=None,mfw_d=None,save=True,vectord={},interesting_vectors=False,abstract_vectors=True,model_lm=False,odir='.',force=False):
		# already written?
		ofn='data.word2vec.words.' + self.fn.replace('.gz','')
		ofnfn=os.path.join(odir,ofn)
		if os.path.exists(ofnfn):
			if save:
				if not force:
					return # already saved
				else:
					pass # let function play out
			else: # getting not putting
				if not force:
					return tools.read_ld(ofnfn)  # return as saved
				else:
					pass  # let function play out

		import time
		mfw_d=mfw_d if mfw_d else self.mfw_d
		model=self.gensim
		now=time.time()
		if interesting_vectors:
			for k,v in list(self.interesting_vectors().items()):
				vectord[k]=v
		if abstract_vectors:
			for k,v in list(self.abstract_vectors().items()):
				vectord[k]=v
		words = words if words else self.mfw(**mfw_d)
		words = list(words)
		#print(len(words),'!')
		old=[]
		words_sofar=set()
		i=0
		for word in words:
			if not word in model: continue
			i+=1
			odx=self.cosine_word(word,vectord)
			#if not i%1000:print('>> model_words: {} of {}...'.format(i+1,len(words)))
			odx['rank']=i
			odx['model_rank'],odx['model_count']=self.gensim.vocab[word].index,self.gensim.vocab[word].count
			#odx['model_tf']=float(odx['model_count']) / self.num_words
			old+=[odx]

		# Rank by vector
		"""
		for vkey in vectord:
			old.sort(key=lambda _d: _d[vkey],reverse=True)
			for i,d in enumerate(old):
				d[vkey+'_rank']=i+1
		"""

		if save:
			ofn='data.word2vec.words.' + self.fn.replace('.gz','')
			ofnfn=os.path.join(odir,ofn)
			tools.write2(ofnfn, old, toprint=False)

			nownow=time.time()
			print('>> saved %s in %s seconds' % (ofn, round(nownow-now,1)))

			if model_lm: self.model_lm(ofn, top_words=len(old))

		return old

	def model_worddb(self,fn='data.worddb.xlsx',word_key='word',ofn='data.worddb.with-vectors.txt',save=True,vectord={},interesting_vectors=True,model_lm=False):
		import tools
		db_ld=tools.read_ld(fn)
		db_dd=tools.ld2dd(db_ld,word_key)
		words = list(db_dd.keys())

		model_ld = self.model_words(words=words,save=False,vectord=vectord,interesting_vectors=interesting_vectors,model_lm=model_lm)
		for dx in model_ld:
			for k,v in list(db_dd.get(dx['word'],{}).items()): dx[k]=v

		if save:
			tools.write2(ofn.replace('.txt','.'+self.name+'.txt'),model_ld)

		return model_ld



	def model_lm(self,fn='data.word2vec.singnouns.ECCO-TCP.txt',top_words=1000):

		ld=tools.tsv2ld(fn)
		ld = [d for d in ld if d['rank']<=top_words]
		keys = list(ld[0].keys())
		keys.remove('word')
		poss=[(k1,k2) for k1,k2 in tools.product(keys,keys) if k1!=k2]
		numposs=len(poss)
		old=[]
		for i,(k1,k2) in enumerate(poss):
			#print numposs-i,k1,k2
			dx={'v1':k1, 'v2':k2}
			vals1 = [d[k1] for d in ld]
			vals2 = [d[k2] for d in ld]
			try:
				dx['lm_a'],dx['lm_b'],dx['lm_RR'] = tools.linreg(vals1,vals2)
				dx['pearson_r'],dx['pearson_p']=pearsonr(vals1,vals2)
				old+=[dx]
			except TypeError:
				continue

		tools.write2(fn.replace('.txt','.linreg-results.txt'), old)

	def model_lm_net(fn='data.word2vec.singnouns.ECCO-TCP.linreg-results.txt'):
		ld=tools.tsv2ld(fn)
		import networkx as nx
		g=nx.Graph()
		M=model(r=True)
		vectord=interesting_vectors(M)
		for d in ld:
			v1,v2=d['v1'],d['v2']
			is_pos=d['pearson_r']>=0
			if not is_pos:
				keystr=' <> '
				if not keystr in v2: continue
				v2_a,v2_b=v2.split(keystr)
				v2=v2_b+keystr+v2_a
			for v in [v1,v2]:
				if not g.has_node(v):
					g.add_node(v,node_type='random vector' if not v in vectord else 'interesting vector')

			g.add_edge(v1,v2,
				weight=d['lm_RR'],
				slope=d['lm_b'],
				pearson_r=abs(d['pearson_r']),
				pearson_p=d['pearson_p'],
				edge_type='Positive' if d['pearson_r']>0 else 'Negative')

		nx.write_graphml(g, fn.replace('.txt','.v2.graphml'))


	def model_lm2_net(fn='data.word2vec.nouns.entropy3.cor-results.txt'):
		ld=tools.tsv2ld(fn)
		import networkx as nx
		g=nx.Graph()
		M=model(r=True)
		vectord=interesting_vectors(M)
		for d in ld:
			if d['v1']==d['v2']: continue
			if abs(d['pearson_r'])<0.5: continue
			for v in [d['v1'],d['v2']]:
				if not g.has_node(v):
					g.add_node(v,node_type='random vector' if not v in vectord else 'interesting vector')

			g.add_edge(d['v1'],d['v2'],
				weight=abs(d['pearson_r']),
				pearson_r=d['pearson_r'],
				edge_type='Positive' if d['pearson_r']>0 else 'Negative')

		nx.write_graphml(g, fn.replace('.txt','.graphml'))

	def load_dist(self,fn=None,return_gen=False):
		fn='dists.word-word-cosine-sim.'+self.name+'.txt' if not fn else fn
		if not os.path.exists(fn): return None
		return tools.tsv2ld(fn) if not return_gen else tools.readgen(fn)

	def dist(self,words=[], to_similarity=False, to_zscore=False, use_nan=False):

		# Helper functions
		def to_sim(dist):
			for i,word in enumerate(M_words):
				for ii,word2 in enumerate(M_words):
					dist[i][ii]=1-dist[i][ii]

		def to_z(dist):
			mean=np.mean(dist)
			std=np.std(dist)
			for i,word in enumerate(M_words):
				for ii,word2 in enumerate(M_words):
					dist[i][ii]=(dist[i][ii]-mean)/std

		# Main
		words=list(self.mfw()) if not words else list(words)
		print('>> generating cosine similarities between '+str(len(words))+' words...')

		M = []
		M_words=[]
		for word in words:
			if not word in self.gensim:
				if not use_nan:
					# just keep going
					continue
				# otherwise, add a row of nan's
				row = [np.nan for i in range(self.gensim.vector_size)]
			else:
				row=[float(x) for x in self.gensim[word]]

			M_words+=[word]
			M+=[row]

		M=np.array(M)
		dist = squareform(pdist(M,'cosine'))
		#dist = squareform(cdist(M,'cosine'))

		if to_zscore:
			to_sim(dist)
			to_z(dist)
		elif to_similarity:
			to_sim(dist)

		print('>> done')
		return dist,M_words

	def nearest_neighbors(self,words=[],n=3,allow_other_words=False):
		words=self.mfw() if not words else words
		words=set(words)
		for word1 in words:
			neighbors=[]
			for word2,val in self.similar(word1,topn=1000):
				if len(neighbors)>=n: break
				if not allow_other_words and not word2 in words: continue
				neighbors+=[(word2,val)]
			yield (word1, tuple(neighbors))



	def top_connections(self,words=[],n=None,special=[]):
		words=self.mfw() if not words else words
		dist,dist_words=self.dist(words,to_similarity=True)
		special=set(special)

		## Get possible vals
		edge2val={}
		vals=[]
		for i1,w1 in enumerate(dist_words):
			for i2,w2 in enumerate(dist_words):
				if i1>=i2: continue
				#if special and not special & set([w1,w2]): continue
				val=dist[i1,i2]
				edge=tuple(sorted([w1,w2]))
				edge2val[edge]=val

		## Get minimum value
		for (w1,w2),val in sorted(list(edge2val.items()),key=lambda _t: -_t[1])[:n]:
			yield (w1,w2,val)

	def top_connections_by_word(self,words=[],topn=100,cosine_cut=None,special=[]):
		words=self.mfw() if not words else words
		words=[w for w in words if w in self.gensim]
		word2conns=dict((w,[]) for w in words)
		word2numconns=dict((w,0) for w in words)

		if special and topn:
			words=set(words)
			for w in special:
				if not w in self.gensim: continue
				"""
				print w,'...'
				dists = [(w2,self.gensim.similarity(w,w2)) for w2 in words]
				dists.sort(key=lambda _tup: -_tup[1])
				word2conns[w]=dists[:topn]
				"""
				matches=[]
				for i,(w2,csim) in enumerate(self.similar(w,topn=topn*10)):
					if w2 in words:
						matches+=[(w2,csim)]
						if len(matches)>=topn: break
				word2conns[w]=matches
			return word2conns

		for a,b,w in self.top_connections(words=words,special=special):
			if not topn or word2numconns[a]<topn:
				#if not special or a in special:
				word2conns[a]+=[(b,w)]
				word2numconns[a]+=1
			if not topn or word2numconns[b]<topn:
				#if not special or b in special:
				word2conns[b]+=[(a,w)]
				word2numconns[b]+=1
			if topn and min(word2numconns.values())>=topn: break
			if cosine_cut and w<=cosine_cut: break

		return word2conns





	def gen_dist(self,words=[],num_mfw=2000,fn=None,unload=False):
		if not words: words=list(self.mfw(n=num_mfw))
		dist,M_words = self.dist(words=words)

		#"""
		def writegen():
			for i,word in enumerate(M_words):
				dx={'rownamecol':word}
				for ii,word2 in enumerate(M_words):
					dx[word2]=dist[i][ii]
				yield dx

		fn='dists.word-word-cosine-sim.'+self.name+'.txt' if not fn else fn
		tools.writegen(fn,writegen)
		if unload: self.unload()


	"""
	Code related to semantic fields
	"""

	def fields_default(self):
		fields = {}
		fieldwords=set()
		for k,v in list(tools.fields.items()):
			if '_' in k and k.split('_')[0] in ['AbstractValues','HardSeed']:
				fwords=set([x for x in v if x in self.vocab and not x in fieldwords])
				fields[k]=fwords
				fieldwords|=fwords
		return fields


	def model_dists_fields(self,fields={},ofn='data.model_dists_fields.txt'):
		"""
		Get distances betweeen the words in semantic fields.

		Fields are given by the dictionary `fields`:
			{'fieldname' = ['word1', 'word2', ...]
			...}
		"""
		# Get fields
		fields=self.fields_default() if not fields else fields
		field_words=[x for k,v in sorted(fields.items()) for x in v]
		field_fields=[k for k,v in sorted(fields.items()) for x in v]
		word2field=dict(list(zip(field_words,field_fields)))

		# Get distances
		dist,dist_words = self.dist(words=field_words)

		## T-sne of distances
		from sklearn.manifold import TSNE
		model = TSNE(n_components=2, random_state=0)
		fit = model.fit_transform(dist)

		# Data-Frame
		old=[]
		for i1,word1 in enumerate(dist_words):
			dx={'word1':word1, 'field':word2field[word1], 'tsne_V1':fit[i1][0], 'tsne_V2':fit[i1][1]}
			for i2,word2 in enumerate(dist_words):
				dx2=dict(list(dx.items()))
				dx2['word2']=word2
				dx2['cosine_similarity']=dist[i1,i2]
				old+=[dx2]

		ofn=ofn.replace('.txt', '.'+self.name+'.txt')
		tools.write2(ofn, old)

	def model_dists_tsne(self,words=[],words_ld=[],save=True,n_components=2,k=24,ofn=None):
		if not words_ld:
			words_ld=self.mfw(return_ld=True)
		words = set(d['word'] for d in words_ld)
		word2d=tools.ld2dd(words_ld,'word')
		dist,dist_words = self.dist(words=words)

		from sklearn.manifold import TSNE
		from sklearn.cluster import KMeans
		model = TSNE(n_components=n_components, random_state=0)
		fit = model.fit_transform(dist)

		model_kclust = KMeans(n_clusters=k).fit(dist)
		labels = model_kclust.labels_
		word2label = dict(list(zip(dist_words, labels)))

		#return fit_kclust

		old=[]
		for i,word in enumerate(dist_words):
			dx={'model':self.name, 'word':word}
			for k,v in list(word2d[word].items()): dx[k]=v
			dx['cluster']=word2label.get(word,'')
			for ii in range(n_components):
				dx['tsne_V'+str(ii+1)]=fit[i][ii]
			old+=[dx]
		if save:
			ofn = 'dists.tsne.{0}.txt'.format(self.name) if not ofn else ofn
			tools.write2(ofn, old)
		return old


	def kclust(self,k=24,words=[],save=True):
		words_ld=self.mfw(return_ld=True)
		words = set(d['word'] for d in words_ld)
		word2d=tools.ld2dd(words_ld,'word')
		dist,dist_words = self.dist(words=words)

		from sklearn.cluster import KMeans
		model_kclust = KMeans(n_clusters=k).fit(dist)
		#fit_kclust = model_kclust.fit_transform(dist)

		return model_kclust

		old=[]
		for i,word in enumerate(dist_words):
			dx={'model':self.name, 'word':word}
			for k,v in list(word2d[word].items()): dx[k]=v
			for ii in range(n_components):
				dx['tsne_V'+str(ii+1)]=fit[i][ii]
			old+=[dx]
		if save: tools.write2('dists.tsne.{0}.txt'.format(self.name), old)
		return old

	def has_word(self,word):
		return word in self.gensim.vocab



































def do_model1(model,mfw,topn=25):
	model.load()
	old=[]
	wordset=set(mfw)
	for i,w in enumerate(mfw):
		print(i,w,'...')
		if not w in model.gensim: continue
		top_tuples = model.gensim.most_similar(w,topn=topn*1000)
		ii=0
		for word,cos in top_tuples:
			if not word in wordset: continue
			ii+=1
			if ii>topn: break
			dx={'word1':w, 'word1_rank': i+1,'word2':word, 'closeness_rank':ii, 'closeness_cosine':cos, 'model':model.name}
			for i,x in enumerate(model.name.split('.')): dx['model_'+str(i)]=x
			#yield dx
			old+=[dx]
	model.unload()
	return old

def do_model2(model,mfw=None,topn=100):
	model.load()
	old=[]
	mfw=model.mfw() if not mfw else mfw
	for i,w in enumerate(mfw):
		print(i,w,'...')
		if not w in model.gensim: continue

		word2sim={}

		for word in mfw:
			if word==w: continue
			if not word in model.gensim: continue

			word2sim[word]=model.gensim.similarity(w,word)

		for ii,word in enumerate(sorted(word2sim,key=lambda _k: -word2sim[_k])):
			if ii>=topn: break
			dx={'word1':w, 'word1_rank': i+1,'word2':word, 'closeness_rank':ii+1, 'closeness_cosine':word2sim[word], 'model':model.name}
			for i,x in enumerate(model.name.split('.')): dx['model_'+str(i)]=x
			old+=[dx]

	model.unload()
	return old










def compare_dists(ns=[2,5,10,15,20,25], num_mfw=1000, topn=100):
	c = ECCO()
	mfw_ld=c.mfw(n=10000,remove_stopwords=True,return_ld=True)
	mfw=[d['word'] for d in mfw_ld]
	w2pos=dict([(d['word'],d['pos']) for d in reversed(mfw_ld)])

	models={}
	for n in ns:
		print('>> loading model:',n,'...')
		models['skipgram_n'+str(n)]=gensim.models.Word2Vec.load('word2vec.ECCO-TCP.skipgram_n={0}.model'.format(n))

	print('>> mfw:',len(mfw))
	mfw = [w for w in mfw if not False in [w in model for model in list(models.values())]]
	print('>> mfw:',len(mfw))

	mfw = mfw[:num_mfw]

	old=[]
	for i,w in enumerate(mfw):
		print(i,w,'...')
		for mname,model in list(models.items()):
			top_tuples = model.most_similar(w,topn=topn)
			for ii,(word,cos) in enumerate(top_tuples):
				dx={'word1':w, 'word1_rank': i+1,'word2':word, 'closeness_rank':ii+1, 'closeness_cosine':cos, 'model':mname, 'word1_pos':w2pos.get(w,"?"), 'word2_pos':w2pos.get(word,"?")}
				old+=[dx]
	tools.write2('data.model-comparison.txt', old)

def compare_models(model_type='corpus_size', num_mfw=1000, topn=100):
	model_fns=[fn for fn in os.listdir('.') if model_type in fn and fn.endswith('.model')]
	for fn in model_fns:
		print(fn)


	c = ECCO()
	mfw_ld=c.mfw(n=10000,remove_stopwords=True,return_ld=True)
	mfw=[d['word'] for d in mfw_ld]
	w2pos=dict([(d['word'],d['pos']) for d in reversed(mfw_ld)])



	models={}
	for model_fn in model_fns:
		print('>> loading model:',model_fn,'...')
		models[model_fn]=gensim.models.Word2Vec.load(model_fn)

	print('>> mfw:',len(mfw))
	mfw = [w for w in mfw if not False in [w in model for model in list(models.values())]]
	print('>> mfw:',len(mfw))

	mfw = mfw[:num_mfw]

	old=[]
	for i,w in enumerate(mfw):
		print(i,w,'...')
		for mname,model in list(models.items()):
			top_tuples = model.most_similar(w,topn=topn)
			for ii,(word,cos) in enumerate(top_tuples):
				dx={'word1':w, 'word1_rank': i+1,'word2':word, 'closeness_rank':ii+1, 'closeness_cosine':cos, 'model':mname, 'word1_pos':w2pos.get(w,"?"), 'word2_pos':w2pos.get(word,"?")}
				old+=[dx]
	tools.write2('data.model-comparison.{0}.txt'.format(model_type), old)



def entropy_gen_data(num_mfw=2000):
	M = model(r=True)
	E = ECCO()
	mfw=[]
	for x in E.mfw(n=num_mfw*2,only_pos='NN',remove_stopwords=True,only_abstract=True,pos_regex=False):
		if not x in mfw:
			mfw+=[x]

	mfw=[w for w in mfw if w in M][:num_mfw]

	vectord1=interesting_vectors(M)
	vectord2=random_vectors(M, mfw, 1000)

	old=[]
	for i,word in enumerate(mfw):
		print('>>',i,word,'...')
		odx1=model_cosine_word(M,word,vectord1)
		odx2=model_cosine_word(M,word,vectord2)
		odx=dict(list(odx1.items()) + list(odx2.items()))
		odx['rank']=i
		old+=[odx]
	tools.write2('data.word2vec.nouns.entropy3.txt', old)

def entropy(fn='data.word2vec.nouns.entropy3.txt'):
	import numpy,scipy
	ld=tools.tsv2ld(fn)
	dl=tools.ld2dl(ld)
	M=model(r=True)
	vectord=interesting_vectors(M)

	old=[]
	for i,vectorname in enumerate(dl):
		print(i,vectorname,'...')
		if vectorname in ['word']:
			continue
		vector_israndom = False if vectorname in vectord else True
		vals = dl[vectorname]
		vals_z=tools.zfy(vals)
		num_above1std = len([v for v in vals_z if abs(v)>=1.0])
		num_above_point_1 = len([v for v in vals if abs(v)>=0.1])

		num_above_0 = len([v for v in vals if v>=0])
		num_below_0 = len([v for v in vals if v<0])


		variance = numpy.var(vals)
		normalness = scipy.stats.mstats.normaltest(vals)
		odx={
		'vector':vectorname,
		'vector_israndom':vector_israndom,
		'variance':variance,
		'normalness_k2':normalness[0],
		'normalness_p':normalness[1],
		'num_above1std':num_above1std,
		'perc_above1std':num_above1std/float(len(vals)),
		'num_above_point_1':num_above_point_1,
		'perc_above_point_1':num_above_point_1 /float(len(vals)),
		'relative_num_above_0':num_above_0-num_below_0,
		'relative_num_above_0_ABS':abs(num_above_0-num_below_0)
		}
		old+=[odx]

	tools.write2('data.word2vec.nouns.entropy3.analysis.txt', old)








### PROCRUSTRES ALIGNMENT CODE ####

def intersection_align_gensim(m1,m2, words=None):
	"""
	Intersect two gensim word2vec models, m1 and m2.
	Only the shared vocabulary between them is kept.
	If 'words' is set (as list or set), then the vocabulary is intersected with this list as well.
	Indices are re-organized from 0..N in order of descending frequency (=sum of counts from both m1 and m2).
	These indices correspond to the new syn0 and syn0norm objects in both gensim models:
		-- so that Row 0 of m1.syn0 will be for the same word as Row 0 of m2.syn0
		-- you can find the index of any word on the .index2word list: model.index2word.index(word) => 2
	The .vocab dictionary is also updated for each model, preserving the count but updating the index.
	"""

	# Get the vocab for each model
	vocab_m1 = set(m1.vocab.keys())
	vocab_m2 = set(m2.vocab.keys())

	# Find the common vocabulary
	common_vocab = vocab_m1&vocab_m2
	if words: common_vocab&=set(words)

	# If no alignment necessary because vocab is identical...
	if not vocab_m1-common_vocab and not vocab_m2-common_vocab:
		return (m1,m2)

	# Otherwise sort by frequency (summed for both)
	common_vocab = list(common_vocab)
	common_vocab.sort(key=lambda w: m1.vocab[w].count + m2.vocab[w].count,reverse=True)

	# Then for each model...
	for m in [m1,m2]:
		# Replace old syn0norm array with new one (with common vocab)
		indices = [m.vocab[w].index for w in common_vocab]
		old_arr = m.syn0norm # if hasattr(m,'syn0norm') else m.syn0
		new_arr = np.array([old_arr[index] for index in indices])
		m.syn0norm = m.syn0 = new_arr

		# Replace old vocab dictionary with new one (with common vocab)
		# and old index2word with new one
		m.index2word = common_vocab
		old_vocab = m.vocab
		new_vocab = {}
		for new_index,word in enumerate(common_vocab):
			old_vocab_obj=old_vocab[word]
			new_vocab[word] = gensim.models.word2vec.Vocab(index=new_index, count=old_vocab_obj.count)
		m.vocab = new_vocab

	return (m1,m2)

def smart_procrustes_align_gensim(base_embed, other_embed, words=None):
	"""Procrustres align two gensim word2vec models (to allow for comparison between same word across models).
	Code ported from HistWords <https://github.com/williamleif/histwords> by William Hamilton <wleif@stanford.edu>.
		(With help from William. Thank you!)

	First, intersect the vocabularies (see `intersection_align_gensim` documentation).
	Then do the alignment on the other_embed model.
	Replace the other_embed model's syn0 and syn0norm numpy matrices with the aligned version.
	Return other_embed.

	If `words` is set, intersect the two models' vocabulary with the vocabulary in words (see `intersection_align_gensim` documentation).
	"""

	# make sure vocabulary and indices are aligned
	in_base_embed, in_other_embed = intersection_align_gensim(base_embed, other_embed, words=words)

	# get the embedding matrices
	base_vecs = in_base_embed.syn0norm
	other_vecs = in_other_embed.syn0norm

	# just a matrix dot product with numpy
	m = other_vecs.T.dot(base_vecs)
	# SVD method from numpy
	u, _, v = np.linalg.svd(m)
	# another matrix operation
	ortho = u.dot(v)
	# Replace original array with modified one
	# i.e. multiplying the embedding matrix (syn0norm)by "ortho"
	other_embed.syn0norm = other_embed.syn0 = (other_embed.syn0norm).dot(ortho)
	return other_embed


###








import numpy as np
def normalize(v):
	'''normalize' a vector, in the traditional linear algebra sense.'''
	norm=np.linalg.norm(v)
	if norm==0:
		return v
	return v/norm

def reject(A,B):
	'''Create a 'projection', and subract it from the original vector'''
	project = np.linalg.linalg.dot(A, normalize(B)) * normalize(B)
	return A - project

def reject_words(model, A, words, return_vector=True):
	'''returns most_similar for word A, while rejecting words with meanings closer to B.
	Seems to work better than just giving in negative words.
	'''
	from gensim import matutils

	for word in words:
		r = reject(model.syn0[A] if type(A)==int else model[A], model[word])
	if return_vector: return r

	dists = np.linalg.linalg.dot(model.syn0, r)
	best  = matutils.argsort(dists, topn = 10, reverse = True)
	result = [(model.index2word[sim], float(dists[sim])) for sim in best]
	return result

def reject_binaries(model, A, binaries, return_vector=True):
	from gensim import matutils

	r=model.syn0[A] if type(A)==int else model[A]
	for B,C in binaries:
		if type(B) in [str,six.text_type]:
			if not B in model: continue
			print('>> B:',B)
			B = model[B]
		if type(C) in [str,six.text_type]:
			if not C in model: continue
			print('>> C:',C)
			C = model[C]
		if type(B) in [list,tuple]:
			B=[model[x] for x in B if x in model]
			if not B: continue
			B=np.mean(B,0)
		if type(C) in [list,tuple]:
			C=[model[x] for x in C if x in model]
			if not C: continue
			C=np.mean(C,0)

		r = reject(model.syn0[A] if type(A)==int else model[A], B - C)
	if return_vector: return r

	dists = np.linalg.linalg.dot(model.syn0, r)
	best  = matutils.argsort(dists, topn = 100, reverse = True)
	result = [(model.index2word[sim], float(dists[sim])) for sim in best]
	return result


#model = get_model().gensim
def reject_binaries_from_matrix(model,binaries=None):
	m=model
	old_arr = m.syn0norm if hasattr(m,'syn0norm') else m.syn0
	new_arr = []
	for i in range(len(old_arr)):
		new_arr+=[reject_binaries(model, i, binaries, return_vector=True)]
	new_arr=np.array(new_arr)
	m.syn0=m.syn0norm=new_arr
	return m

def reject_words_from_matrix(model,words=[]):
	m=model
	old_arr = m.syn0norm if hasattr(m,'syn0norm') else m.syn0
	new_arr = []
	for i in range(len(old_arr)):
		new_arr+=[reject_words(model, i, words, return_vector=True)]
	new_arr=np.array(new_arr)
	m.syn0=m.syn0norm=new_arr
	return m



class SkipgramsCorpus(object):
	def __init__(self, corpus, n):
		self.corpus=corpus
		self.n=n

	def slice(self,text,lowercase=True):
		words=text.text_plain().strip().split()
		words=[tools.noPunc(w.lower()) if lowercase else tools.noPunc(w) for w in words if True in [x.isalpha() for x in w]]
		for slice_i,slice in enumerate(tools.slice(words,slice_length=self.n,runts=False)):
			yield slice

	def __iter__(self):
		for text in self.corpus.texts():
			for slice in self.slice(text):
				yield slice



class SkipgramsMongo(object):
	def __init__(self, corpus, n, mongo_q={}):
		self.corpus=corpus
		self.n=n
		self.mongo_q=mongo_q

	def __iter__(self):
		i=0
		for skip in self.corpus.skipgrams_mongo(self.mongo_q,skipgram_n=self.n):
			i+=1
			#if i>1000000: break
			yield skip



class SkipgramsSampler(object):
	def __init__(self, fn, num_skips_wanted):
		self.fn=fn
		self.num_skips_wanted=num_skips_wanted
		self.num_skips=self.get_num_lines()
		self.line_nums_wanted = set(random.sample(list(range(self.num_skips)), num_skips_wanted))

	def get_num_lines(self):
		then=time.time()
		print('>> [SkipgramsSampler] counting lines in',self.fn)
		with gzip.open(self.fn,'rb') if self.fn.endswith('.gz') else open(self.fn) as f:
			for i,line in enumerate(f):
				pass
		num_lines=i+1
		now=time.time()
		print('>> [SkipgramsSampler] finished counting lines in',self.fn,'in',int(now-then),'seconds. # lines =',num_lines,'and num skips wanted =',self.num_skips_wanted)
		return num_lines

	def __iter__(self):
		i=0
		with gzip.open(self.fn,'rb') if self.fn.endswith('.gz') else open(self.fn) as f:
			for i,line in enumerate(f):
				line = line.decode('utf-8') if self.fn.endswith('.gz') else line
				if i in self.line_nums_wanted:
					yield line.split()


class MultiSkip(object):
	def __init__(self, skips,labels=[]):
		self.skips=skips
		self.labels=labels

	def __iter__(self):
		for i,skip in enumerate(self.skips):
			if self.labels and i<len(self.labels):
				label=self.labels[i]
				for x in skip:
					skip2=[w+'_'+label for w in x]
					yield skip2
			else:
				for x in skip:
					yield x

### WORD2VEC

# STONE
def gen_word2vec_model_from_skipgrams(path_to_skipgram_file_or_files,results_dir='./',skipgram_size=10,num_runs=1,
									num_skips_wanted=None, num_workers=8, min_count=100, num_dimensions=100, sg=1, num_epochs=None,labels=[]):

	"""
	STONE CERTIFIED
	Throw this at skipgram files to make word2vec models.
	"""
	import gensim,logging
	logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

	# Load skipgrams
	for run in range(num_runs):
		if type(path_to_skipgram_file_or_files) not in {list,tuple}:
			path_to_skipgram_file = path_to_skipgram_file_or_files
			skips = gensim.models.word2vec.LineSentence(path_to_skipgram_file) if not num_skips_wanted else SkipgramsSampler(path_to_skipgram_file, num_skips_wanted)
		else:
			skips = MultiSkip([(gensim.models.word2vec.LineSentence(path_to_skipgram_file) if not num_skips_wanted else SkipgramsSampler(path_to_skipgram_file, num_skips_wanted)) for path_to_skipgram_file in path_to_skipgram_file_or_files],
							 labels=labels)
			path_to_skipgram_file = path_to_skipgram_file_or_files[0]


		# Generate model
		model = gensim.models.Word2Vec(skips, workers=num_workers, sg=sg, min_count=min_count, size=num_dimensions, window=skipgram_size)

		# Output filename
		ofn_l = [os.path.splitext(os.path.basename(path_to_skipgram_file))[0]]
		if run: ofn_l+=['run=%s' % str(run).zfill(2)]
		ofn = '.'.join(ofn_l) + '.txt'
		ofnfn=os.path.join(results_dir,'word2vec_models',ofn)
		ofnfn_vocab=os.path.splitext(ofnfn)[0]+'.vocab.txt'
		ofolder=os.path.dirname(ofnfn)
		ofnfn=ofnfn+'.gz'                       # add gzip compression for model
		if not os.path.exists(ofolder):
			try:
				os.makedirs(ofolder)
			except OSError:
				pass

		# Save model
		model.wv.save_word2vec_format(ofnfn, ofnfn_vocab)
		print(">> saved:",ofnfn)
		print(">> saved:",ofnfn_vocab)







#### CONSOLIDATING
def model_words(path_model):
	import pandas as pd
	from lltk.model.word2vec import Word2Vec
	from scipy.stats import zscore
	m = Word2Vec(fn=path_model)
	ld=m.model_words(save=False)
	df=pd.DataFrame(ld).drop(['model_rank','model_count'],1).set_index('word')
	dfz=df.apply(zscore)
	df2=df.join(dfz,rsuffix='_z')
	df2['word']=df2.index
	return df2.to_dict('records')








#### RANKS


def do_model_rank(model,words,topn=50,special=None):
	old=[]
	model_words = [w for w in model.vocab]

	for word1 in words:
		try:
			top = model.most_similar(word1,topn=topn)
			for ii,(word2,cosine) in enumerate(top):
				try:
					dx={'word1':word1, 'word2':word2}
					dx['word1_rank']=model_words.index(word1)
					dx['word2_rank']=model_words.index(word2)
					dx['closeness_cosine']=cosine
					dx['closeness_rank']=ii+1
				except KeyError:
					continue
				old+=[dx]
		except KeyError:
			continue
	return old

def model_ranks(model_fns,words,topn=50,special=None):
	for mfn in model_fns:
		print('>>',mfn,'...')
		model = gensim.models.KeyedVectors.load_word2vec_format(mfn)
		for dx in do_model_rank(model,words,topn=topn,special=special):
			dx['model']=os.path.basename(mfn.replace('.txt.gz','').replace('.txt',''))
			for i,x in enumerate(dx['model'].split('.')):
				if x in {'model','txt','gz'}: continue
				dx['model_name_'+str(i+1)]=x
			yield dx
