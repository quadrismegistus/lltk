from __future__ import absolute_import
from __future__ import print_function
import os,gensim,logging,time,numpy as np,random
from lltk.model import Model
from lltk.model.word2vec import Word2Vec,KEYWORDS
from lltk.tools import tools
from lltk.tools import stats
from scipy.spatial.distance import cosine,pdist,squareform
from scipy.stats import pearsonr,spearmanr
import multiprocessing as mp,gzip,random,time
from collections import defaultdict,Counter
import codecs
#from pathos.multiprocessing import ProcessingPool as Pool
import multiprocessing as mp
import six
from six.moves import range
from six.moves import zip

TOP2000=dict(n=2000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP5000=dict(n=5000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP10000=dict(n=10000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP2000ABSNOUN=dict(n=2000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=True)
TOP1000ABSNOUN=dict(n=1000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=True)
TOP50000ALLWORDS = dict(n=50000,only_pos=None,pos_regex=False,remove_stopwords=False,only_abstract=None)
TOP2000SINGNOUN=dict(n=2000,only_pos='NN',pos_regex=False,remove_stopwords=True,only_abstract=None)

DEFAULT_MFWD=TOP5000


class Word2Vecs(Model):
	def __init__(self,corpus,fns,periods,skipgram_n=10,name=None):
		self.fns=fns
		self.periods=periods
		self.corpus=corpus
		self.name=name
		self.w2v = self.models = [Word2Vec(fn=fn,period=period,skipgram_n=skipgram_n,corpus=corpus) for fn,period in zip(self.fns,self.periods)]
		self.mfw_d=DEFAULT_MFWD
		self.aligned=False
		self.modeld = dict((m.name.replace(self.corpus.name+'.',''),m) for m in self.models)
		self.models.sort(key=lambda _m: _m.name)

	"""
	@property
	def models(self):
		return self.w2v
	"""

	@property
	def period2models(self):
		if not hasattr(self,'_period2models'):
			self._period2models=p2m={}
			for model in self.models:
				p=model.period
				if not p in p2m: p2m[p]=[]
				p2m[p]+=[model]
		return self._period2models

	def get_models(self,sample_per_period=None):
		return self.models if not sample_per_period else self.models_sample(sample_per_period)

	def models_sample(self,n_per_period=10):
		return [model for period in sorted(self.period2models) for model in self.period2models[period][:n_per_period]]

	@property
	def statd(self):
		if not hasattr(self,'_statd'):
			statfn=os.path.join(self.corpus.path_model,'data','data.%s.stats.txt' % self.name)
			if not os.path.exists(statfn):
				self._statd={}
			else:
				ld=tools.read_ld(statfn)
				self._statd=tools.ld2dd(ld,'period')
		return self._statd

	def mfw(self,use_first_model=False,use_last_model=True,intersection=True,**attrs):
		mfw_d=self.mfw_d
		for k,v in list(attrs.items()): mfw_d[k]=v
		mfw=list(self.corpus.mfw_simple(**mfw_d))
		if mfw: return mfw

		# @TODO
		mfw=set()
		if use_first_model: return list(self.models[0].mfw(**attrs))
		if use_last_model: return list(self.models[-1].mfw(**attrs))
		for m in self.models:
			this_mfw=set(list(m.mfw(**attrs)))
			if not mfw:
				mfw=this_mfw
			elif intersection:
				mfw&=this_mfw
			else:
				mfw|=this_mfw
		return list(mfw)

	@property
	def vocabset(self):
		vocabset=self.models[0].vocabset
		for m in self.models[1:]:
			vocabset&=m.vocabset
		return vocabset



	def load(self):
		for m in self.models:
			m.load()

	def save(self,odir=None, gensim_format=False, word2vec_format=True):
		for model in self.models:
			model.save(odir=None, gensim_format=gensim_format, word2vec_format=word2vec_format)

	def limit_by_rank(self,max_rank):
		for m in self.models: m.limit_vocab(n=max_rank,fpm_cutoff=None)

	def limit_vocab_and_save(self,odir='.',n=None,fpm_cutoff=1.0,overwrite=False,english=False):
		if not os.path.exists(odir): os.makedirs(odir)
		for model in self.models:
			ofnfn=os.path.join(odir,model.fn)
			if not overwrite and os.path.exists(ofnfn):
				print('>> already a model in directory "{0}" named "{1}" and overwrite set to False. skipping...'.format(odir,model.fn))
				continue

			model.load()
			model.limit_vocab(n=n,fpm_cutoff=fpm_cutoff,english=english)
			model.save(odir=odir, gensim_format=False, word2vec_format=True)
			model.unload()

	def align(self):
		vocab=list(self.vocabset)
		for i1,m1 in enumerate(self.models):
			for i2,m2 in enumerate(self.models):
				if i1>=i2: continue
				m1.align(m2,words=vocab)
		self.aligned=True

	def align_bigrams(self,use_mfw=False, mfw_d={}):
		words=None
		if use_mfw:
			words=self.mfw(**mfw_d)
			print('Using MFW: # words:',len(words),words[:3],'...')

		for m1,m2 in tools.bigrams(self.models):
			m1.load()
			m2.load()
			m1.align(m2,words=words)
		self.aligned=True


	def stats(self,dist=True):
		for model in self.models:
			dist,dist_words=model.dist()
			print(model, np.median(dist))


	def model_words(self, mp=True, **kwargs):
		#words=self.mfw() if not words else words
		if not mp:
			for w2v in self.models:
				w2v.model_words(**kwargs)
				w2v.unload()
		else:
			tools.crunch(self.models, 'model_words',kwargs=kwargs)

		## consolidate
		self.consolidate_model_words()

	def consolidate_model_words(self,idir='.',name=None):
		if not name: name=self.name
		old1=[]
		old2=[]
		#for w2v in self.models:
		for fn in sorted(os.listdir(idir)):
			if not fn.startswith('data.word2vec.words.'): continue
			fnfn1=os.path.join(idir,fn)
			fnfn2=fnfn1.replace('.txt','.linreg-results.txt').replace('.txt.gz','.txt')
			print('>> consolidating:',fnfn1,fnfn2)
			ld1=tools.tsv2ld(fnfn1)
			ld2=tools.tsv2ld(fnfn2)

			# period
			period_l = [x for x in fnfn1.split('.') if x.split('-')[0].isdigit()]
			period = period_l[0] if period_l else ''


			for ld in [ld1,ld2]:
				for d in ld:
					#d['model']=w2v.name
					d['model']=fn
					d['period']=period
					for i,x in enumerate(fn.split('.')):
						d['model_name_'+str(i+1)]=x
			old1+=ld1
			old2+=ld2


		word2ld=tools.ld2dld(old1,'word')
		old3=[]
		for word,wld in list(word2ld.items()):
			period2ld=tools.ld2dld(wld,'model_name_2')
			#wld.sort(key=lambda _d: _d['model_name_2'])
			vecs=[k for k in list(wld[0].keys()) if '<>' in k]
			for vec in vecs:
				try:
					Y = [np.mean([d[vec] for d in period2ld[period]]) for period in sorted(period2ld)]
				except KeyError:
					continue
				X = list(range(len(Y)))

				a,b,RR = tools.linreg(X,Y)
				pr,pp=pearsonr(X,Y)
				odx={'word':word, 'vector':vec}
				for period,y in zip(sorted(period2ld.keys()), Y):
					odx[period]=y
				odx['linreg_RR']=RR
				odx['pearson_R']=pr
				odx['pearson_P']=pp
				odx['diff_firstlast_perc']=(Y[0]-Y[-1])/Y[0]
				odx['diff_firstlast_subt']=np.mean(Y[-2:]) - np.mean(Y[:2])
				old3+=[odx]


		ofn1='data.word2vec.consolidated.words.'+name+'.txt'
		ofn2='data.word2vec.consolidated.words.'+name+'.linreg-results.txt'
		ofn3='data.word2vec.consolidated.words.'+name+'.linreg-words.txt'
		tools.write2(ofn1,old1)
		tools.write2(ofn2,old2)
		tools.write2(ofn3,old3)

	def gen_dists(self,words=[],num_mfw=2000,sample_per_period=None,ofolder=None,nprocs=1):
		#mfw=self.mfw(n=num_mfw)
		models = self.models if not sample_per_period else self.models_sample(sample_per_period)

		import os
		if not ofolder: ofolder='dists_'+self.name
		if not os.path.exists(ofolder): os.makedirs(ofolder)
		cwd=os.getcwd()
		os.chdir(ofolder)

		if nprocs>1:
			tools.crunch(models, 'gen_dist', kwargs={'words':words,'num_mfw':num_mfw,'unload':True}, nprocs=nprocs)
		else:
			for model in models:
				model.gen_dist(words=words, num_mfw=num_mfw)
				model.unload()

		os.chdir(cwd)

	def model_dists(self,use_ranks=True,sample_per_period=None): #,norm=True): <-- it doesn't look like normalizing really makes sense?
		from scipy.stats import rankdata,spearmanr

		name2model={}
		name2dist={}
		name2words={}
		name2numpy={}

		models = self.models if not sample_per_period else self.models_sample(sample_per_period)

		for model in models:
			print('>>',model.name)
			dist=model.load_dist(return_gen=True)
			if not dist: continue
			name=model.name
			d1=next(dist)
			name2words[name]=set(d1.keys())
			dist=model.load_dist(return_gen=True)
			name2model[name]=model
			name2dist[name]=dist


		## Prune
		words=set.intersection(*list(name2words.values()))
		for name,dist in sorted(name2dist.items()):
			print('>>',name,'...')
			#dist=[d for d in dist if d['rownamecol'] in words]
			rows=[]
			for di,d in enumerate(dist):

				if not d['rownamecol'] in words: continue

				for k in list(d.keys()):
					if not k in words:
						del d[k]
				array=np.array([d[k] for k in sorted(d.keys()) if not k in ['rownamecol']])
				if use_ranks:
					array=rankdata(array)

				#if norm: array=pystats.zfy(array)
				print(name, di, array[:4],'...')
				rows+=[array]

			M=np.array(rows)

			name2numpy[name]=M

		old=[]
		for name1 in name2dist:
			dx={'rownamecol':name1}
			for name2 in name2dist:
				# Frobenius distance
				A=name2numpy[name1]
				B=name2numpy[name2]
				dx[name2]=np.sqrt(np.sum((A-B)**2))
				#dx[name2]=spearmanr(A,B)
			old+=[dx]

		tools.write2('dists.comparison-fro.'+self.name+'.txt', old)

	def model_dists_tsne(self,fn=None,save=True):
		if not fn: fn='dists.comparison.'+self.name+'.txt'
		ld=tools.tsv2ld(fn)
		M=np.array([[v for k,v in sorted(d.items()) if k!='rownamecol'] for d in ld])

		from sklearn.manifold import TSNE
		model = TSNE(n_components=2, random_state=0)
		fit = model.fit_transform(M)

		old=[]
		for i,d in enumerate(ld):
			dx={'model':d['rownamecol']}
			for ii,xx in enumerate(dx['model'].split('.')): dx['model_'+str(ii)]=xx
			dx['tsne_V1'],dx['tsne_V2']=fit[i]
			old+=[dx]
		if save: tools.write2(fn.replace('.txt','.tsne.txt'), old)
		return old



	def save_mfw_by_ranks(self,ofn=None):
		word2rank=defaultdict(list)

		def do_model_get_ranks(model):
			model.load()
			r=list(model.word2index.items())
			model.unload()
			return r


		for res in Pool().map(do_model_get_ranks, self.models):
			for (word,index) in res:
				word2rank[word]+=[index]

		for word in word2rank:
			word2rank[word]=np.median(word2rank[word])

		if not ofn: ofn='data.mfw.%s.txt' % self.name
		with codecs.open(ofn,'w',encoding='utf-8') as of:
			for word in sorted(word2rank,key = lambda w: word2rank[w]):
				of.write(word+'\n')
		print('>> saved:',ofn)




	def model_ranks(self,words=[],special=None,topn=50,periods=None,num_runs=None):
		models = self.models if not periods else [m for m in self.models if m.period in periods]
		models = models if not num_runs else [m for m in models if m.run_num<=num_runs]
		models = [m for m in models if m.exists]
		print('>> MODELS:',[m.name for m in models])
		#return
		periods=set(periods) if periods else set(self.periods)
		special = KEYWORDS if not special else special
		if not words:
			words=list(self.mfw())
			wordset=set(words)
			for x in special:
				if not x in wordset:
					words+=[x]

		print(">> getting ranks for {} words, where ranks are calculated against {} words...".format(len(special), len(words)))
		#print words
		#return

		def writegen():
			ww2periods=defaultdict(set)
			pool = mp.Pool()
			args = [(model,words,topn,special) for model in models]
			#for dx in (dx for res in pool.imap(do_model_rank,args) for dx in res):
			for ld in pool.imap(do_model_rank,args):
				for dx in ld:
					yield dx
					ww=(dx['word1'],dx['word2'])
					ww2periods[ww]|={dx['model_name_2']}
			for ww in ww2periods:
				for missingperiod in periods-ww2periods[ww]:
					yield {'word1':ww[0], 'word2':ww[1], 'closeness_rank':666, 'closeness_cosine':0, 'model_name_2':missingperiod}

		tools.writegen('data.word2vec.consolidated.ranks.{0}.txt'.format(self.name), writegen)

	def model_ranks_lm(self,fn=None,max_rank=100):
		if not fn: fn='data.word2vec.consolidated.ranks.{0}.txt'.format(self.name)

	    ## Build necessary data structure
		from collections import defaultdict
		wordpair2period2ranks={}
		for d in tools.readgen(fn):
			wordpair=(d['word1'],d['word2'])
			period=d['model_name_2']
			rank=float(d['closeness_rank'])
			if max_rank and rank>max_rank: continue
			if not wordpair in wordpair2period2ranks: wordpair2period2ranks[wordpair]=defaultdict(list)
			wordpair2period2ranks[wordpair][period]+=[rank]

		def writegen():
			numwordpairs=len(wordpair2period2ranks)
			for i,wordpair in enumerate(wordpair2period2ranks):
				if not i%100:
					print('>>',i,numwordpairs,'...')
				X,Y=[],[]
				for period in sorted(wordpair2period2ranks[wordpair]):
					x=int(period.split('-')[0])
					for y in wordpair2period2ranks[wordpair][period]:
						Y+=[y]
						X+=[x]
					#wordpair2period2ranks[wordpair][period]=np.median(wordpair2period2ranks[wordpair][period])
					#Y+=[wordpair2period2ranks[wordpair][period]]
				#X=list(range(len(Y)))

				if len(set(X))<2: continue

				a,b,RR = tools.linreg(X,Y)
				pr,pp=pearsonr(X,Y)

				odx={}
				odx['word1'],odx['word2']=wordpair
				odx['num_periods']=len(set(X))
				odx['linreg_RR']=RR
				odx['pearson_R']=pr
				odx['pearson_P']=pp
				odx['linreg_slope']=a
				odx['rank_diff']=np.mean(Y[-2:]) - np.mean(Y[:2])
				yield odx

		tools.writegen(fn.replace('.txt','.linreg.txt'), writegen)


	def semantic_displacement(self,words=None,model_pairs=[('1750-','1790-')], ofn='data.semantic_displacement.3.txt',min_count=None,neighborhood_size=10):
		if not words: words=set(self.mfw())

		def _writegen():
			vectord=None
			for m1key,m2key in model_pairs:
				m1s=[m for m in self.models if m1key in m.name]
				m2s=[m for m in self.models if m2key in m.name]

				for m1,m2 in zip(m1s,m2s):
					m1.load()
					m2.load()

					if min_count: m1.limit_by_count(min_count)
					if min_count: m2.limit_by_count(min_count)


					word1stat,word2stat={},{}
					words_ok=set(words) & set(m1.gensim.vocab.keys()) & set(m2.gensim.vocab.keys())
					for word in words_ok: word1stat[word]=(m1.gensim.vocab[word].index,m1.gensim.vocab[word].count)
					for word in words_ok: word2stat[word]=(m2.gensim.vocab[word].index,m2.gensim.vocab[word].count)

					m1.align(m2,words=words)

					vectord1=m1.abstract_vectors(include_social=True,only_major=True)
					vectord2=m2.abstract_vectors(include_social=True,only_major=True)

					for word in words_ok:
						try:
							cos=m1.similarity(word,m2)
						except KeyError:
							continue

						## Major stats
						odx={'model1':m1.name, 'model2':m2.name, 'word':word, 'cosine_similarity':cos}
						for k,v in list(m1.named.items()): odx[k+'_1']=v
						for k,v in list(m2.named.items()): odx[k+'_2']=v
						odx['model_rank_1'],odx['model_count_1']=word1stat[word]
						odx['model_rank_2'],odx['model_count_2']=word2stat[word]
						neighborhood1 = [w for w,c in m1.similar(word,neighborhood_size)]
						neighborhood2 = [w for w,c in m2.similar(word,neighborhood_size)]
						odx['neighborhood_1']=', '.join(neighborhood1)
						odx['neighborhood_2']=', '.join(neighborhood2)

						## Get displacement measure #2 (noun based)
						wordset1=set(neighborhood1)
						wordset2=set(neighborhood2)
						all_words = list(wordset1|wordset2)
						# DO I NEED THIS? -->
						#all_words = [x for x in all_words if x in m1.gensim.vocab and x in m2.gensim.vocab and m1.gensim.vocab[x].index<=min_rank and m2.gensim.vocab[x].index<=min_rank]
						#if len(all_words)<3: return {}
						vector1 = [m1.gensim.similarity(word,word2) for word2 in all_words]
						vector2 = [m2.gensim.similarity(word,word2) for word2 in all_words]
						odx['cosine_similarity_by_neighborhood']=1-cosine(vector1,vector2)

						## Vector stats on abstract vectors (optional)
						vectors = set(vectord1.keys()) & set(vectord2.keys())
						for k,v in list(m1.cosine_word(word,vectord1).items()): odx['vec_'+k+'_1']=v
						for k,v in list(m2.cosine_word(word,vectord2).items()): odx['vec_'+k+'_2']=v
						for vec in vectors: odx['vec_'+vec+'_2-1']=odx['vec_'+vec+'_2'] - odx['vec_'+vec+'_1']


						"""
						# jaccard
						list1=[w for w,c in m1.similar(word,topn=25)]
						list2=[w for w,c in m2.similar(word,topn=25)]
						set1=set(list1)
						set2=set(list2)
						odx['jaccard']=len(set1&set2) / float(len(set1|set2))
						odx['neighborhood_1_not_2']=', '.join(sorted(set1-set2,key=lambda w: list1.index(w)))
						odx['neighborhood_2_not_1']=', '.join(sorted(set2-set1,key=lambda w: list2.index(w)))
						"""

						yield odx


					m1.unload()
					m2.unload()

		# also do summary

		def _writegen_meta(key_str=('model_name_2_1','model_name_2_2','word')):
			WordMeta={}
			for dx in tools.writegengen(ofn, _writegen):
				key=tuple([dx[k] for k in key_str])
				if not key in WordMeta: WordMeta[key]=defaultdict(list)
				#for k in ['cosine_similarity','jaccard','model_rank_1','model_rank_2','model_count_1','model_count_2','neighborhood_1_not_2','neighborhood_2_not_1']:
				for k in dx:
					#if 'model' in k: continue
					WordMeta[key][k]+=[dx[k]]

			for key in WordMeta:
				metadx=dict(list(zip(key_str,key)))
				metadx['num_records']=len(WordMeta[key]['cosine_similarity'])
				for k in WordMeta[key]:
					try:
						metadx[k]=np.median(WordMeta[key][k])
					except (ValueError,TypeError) as e:
						if not 'neighborhood' in k: continue
						vals = [w for liststr in WordMeta[key][k] for w in liststr.split(', ')]
						cntr = Counter(vals)
						newstr = ', '.join(['{} ({})'.format(a,b) for a,b in cntr.most_common(10)])
						metadx[k]=newstr
				yield metadx

		ofn_summary=ofn.replace('.txt','.summarized.txt')


		for dx in tools.writegengen(ofn_summary, _writegen_meta): yield dx







	def gen_semantic_networks(self,k_core=None):
		"""
		Compare the semantic networks across the separate W2V models in this W2Vs object.
		"""

		name=self.corpus.name
		goog_url=SEMANTIC_NETWORK_GOOG_URLS[name]
		cluster_ld = tools.tsv2ld(goog_url)
		cluster_id2d=tools.ld2dd(cluster_ld,'ID')
		node_ld = tools.tsv2ld(self.fn.replace('.graphml','.analysis-with-modularity.txt'))
		id2ld =tools.ld2dld(node_ld,'partition_id')

		def writegen():
			pool=mp.Pool(processes=4)
			for model in self.models:
				model.mfw_d=self.mfw_d

			proc = [pool.apply_async(do_semantic_network, args=(model,k_core)) for model in self.models]
			for gen in proc:
				for dx in gen.get():
					yield dx

		tools.writegen('word2vec.comparison.semantic_networks.'+self.name+'.txt', writegen)

	def load_meta_dist(self,fn_dist=None,fn_dist_words=None):
		if not fn_dist: fn_dist = 'data.meta_dist.%s.npy' % self.name
		if not fn_dist_words: fn_dist_words = 'data.meta_dist_words.%s.npy' % self.name
		dist = np.load(fn_dist)
		dist_words = np.load(fn_dist_words)
		return dist,dist_words


	def meta_dist(self,words,models=None,sample_per_period=None,apply_median=True,apply_mean=False,min_count=None,max_rank=None,save=False):
		"""
		Make a 'meta' distance matrix. Only works by supplying a list of words.
		This will get the cosine distance matrix for each model in self.models,
		then return a numpy array of depth 3 (2 x 2 for words) x 1 matrix per model.
		"""

		pool=mp.Pool()
		models = self.get_models(sample_per_period) if not models else models
		#models = [m for m in self.models if m.period=='1790-1799']
		args = [(m,words,min_count,max_rank) for m in models]
		results=pool.map(do_meta_dist, args)
		dists,dists_words=list(zip(*results))
		DIST = np.dstack(dists)

		if apply_median:
			DIST = np.nanmedian(DIST,axis=2)
		elif apply_mean:
			DIST = np.nanmean(DIST,axis=2)

		if save:
			np.save('data.meta_dist.%s.npy' % self.name,DIST)
			np.save('data.meta_dist_words.%s.npy' % self.name,dists_words[-1])

		return DIST,dists_words[-1]

	def gen_meta_semantic_network(self,words,models=None):
		"""
		Make a 'meta' semantic network. Only works by supplying a list of words.
		This will get the cosine distance matrix for each model in self.models,
		then median the distances, and then pass that off to semnet.py to make a network.
		"""
		from lltk.model.semnet import SemanticNetwork
		M,Mw = self.meta_dist(words,models=models,apply_median=True)
		SN=SemanticNetwork(self)
		SN.gen_from_dist(M,Mw,num_edge_factor=2,cosine_cut=None,save=True,k_core=None,giant_component=False)
		return SN

	def semantic_network(self,**attrs):
		from lltk.model.semnet import SemanticNetwork
		sn=SemanticNetwork(model=self)
		return sn


	def gen_meta_tsne(self,dist=None,dist_words=None,save=True,n_components=2,k=24,ofn=None):
		if not dist or not dist_words: dist,dist_words=self.load_meta_dist()

		print(len(dist_words))


		#from sklearn.manifold import TSNE
		from MulticoreTSNE import MulticoreTSNE as TSNE
		from sklearn.cluster import KMeans
		model = TSNE(n_components=n_components, random_state=0, n_jobs=8)
		fit = model.fit_transform(dist)

		word2label={}
		#model_kclust = KMeans(n_clusters=k).fit(dist)
		#labels = model_kclust.labels_
		#word2label = dict(zip(dist_words, labels))

		old=[]
		for i,word in enumerate(dist_words):
			dx={'model':self.name, 'word':word}
			#for k,v in word2d[word].items(): dx[k]=v
			dx['cluster']=word2label.get(word,'')
			for ii in range(n_components):
				dx['tsne_V'+str(ii+1)]=fit[i][ii]
			old+=[dx]
		if save:
			ofn = 'dists.tsne.{0}.txt'.format(self.name) if not ofn else ofn
			tools.write2(ofn, old)
		return old


	"""
	@property
	def vocab(self):
		vocab=self.models[0].vocab
		return vocab
		# @TODO what is this function doing and where?
		# was being used in mfw to check against
		# would rather not do that

		if not self.aligned:
			vocab_set=set(vocab)
			for model in self.models[1:]:
				vocab_set&=set(model.vocab)
			vocab_list=list(vocab_set)
			vocab_list.sort(key=lambda x: vocab.index(x))
			vocab=vocab_list
		return vocab
	"""

	## Rates of change
	def rate_of_change(self,words=None,topn=100):
		if not self.aligned:
			print('>> Rate of Change requires that the word2vec models have been aligned. Run align() first.')
			return

		if not words: words=self.mfw()
		num_words=len(words)

		def writegen():
			for i,word in enumerate(words):
				print('>>',num_words-i,word,'..')
				old=[]
				for i1,m1 in enumerate(self.models):
					for i2,m2 in enumerate(self.models):
						if i1<=i2: continue
						## jaccard with top N
						res1=m1.similar(word,topn=topn)
						res2=m2.similar(word,topn=topn)
						words1,csim1=list(zip(*res1))
						words2,csim2=list(zip(*res2))
						wordset1=set(words1)
						wordset2=set(words2)
						jacc=float(len(wordset1 & wordset2)) / float(len(wordset1 | wordset2))

						## spearman with all
						"""
						assert len(m1.vocabset) == len(m2.vocabset)
						vocsize=len(m1.vocabset)
						res1=m1.similar(word,topn=vocsize)
						res2=m2.similar(word,topn=vocsize)
						res1.sort()
						res2.sort()
						words1,csim1=zip(*res1)
						words2,csim2=zip(*res2)
						sp_r,sp_p = spearmanr(csim1,csim2)
						#"""
						sp_r,sp_p=None,None

						sim=m1.similarity(word,m2)
						dist=1-sim
						m1name=m1.name.replace(m1.corpus.name+'.','')
						m2name=m2.name.replace(m2.corpus.name+'.','')
						odx1={'word':word, 'model1':m1name, 'model2':m2name, 'cosine_distance':dist, 'spearman_r':sp_r, 'spearman_p':sp_p, 'jaccard':jacc, 'words_only_in_model1':', '.join(wordset1-wordset2), 'words_only_in_model2':', '.join(wordset2-wordset1), 'is_keyword':word in KEYWORDS}
						odx2={'word':word, 'model1':m2name, 'model2':m1name, 'cosine_distance':dist, 'spearman_r':sp_r, 'spearman_p':sp_p, 'jaccard':jacc, 'words_only_in_model1':', '.join(wordset2-wordset1), 'words_only_in_model2':', '.join(wordset1-wordset2), 'is_keyword':word in KEYWORDS}
						yield odx1
						yield odx2


		tools.writegen('data.rate_of_change.txt', writegen)

	## Rates of change
	def rate_of_change_cosine(self,words=None):
		if not self.aligned:
			print('>> Rate of Change requires that the word2vec models have been aligned. Run align() first.')
			return

		if not words: words=self.mfw()

		def writegen():
			for word in words:
				old=[]
				for i1,m1 in enumerate(self.models):
					for i2,m2 in enumerate(self.models):
						sim=m1.similarity(word,m2)
						dist=1-sim
						m1name=m1.name.replace(m1.corpus.name+'.','')
						m2name=m2.name.replace(m2.corpus.name+'.','')
						odx1={'word':word, 'model1':m1name, 'model2':m2name, 'cosine_distance':dist}
						odx2={'word':word, 'model1':m2name, 'model2':m1name, 'cosine_distance':dist}
						yield odx1
						yield odx2


		tools.writegen('data.rate_of_change.txt', writegen)

	## Rates of change
	def rate_of_change_bigrams(self,words=None):
		if not self.aligned:
			print('>> Rate of Change requires that the word2vec models have been aligned. Run align() first.')
			return

		if not words: words=self.mfw()

		def writegen():
			for word in words:
				old=[]
				for m1,m2 in m1_m2s:
					if not word in m1.vocabset or not word in m2.vocabset: continue
					sim=m1.similarity(word,m2)
					dist=1-sim
					odx={'word':word, 'model1':m1.name, 'model2':m2.name, 'cosine_distance':dist}
					old+=[odx]

				X=list(range(len(old)))
				Y=[dx['cosine_distance'] for dx in old]
				r,p = spearmanr(X,Y)
				for dx in old:
					dx['spearman_r'],dx['spearman_p']=r,p
					yield dx


		tools.writegen('data.rate_of_change.txt', writegen)


# words=[],special=[],
def do_model_rank(xxx_todo_changeme):
	#model.mfw_d['special']=special
	(model,words,topn,special) = xxx_todo_changeme
	word2topconn=model.top_connections_by_word(words=words,topn=topn,special=special)
	old=[]

	for word1 in sorted(word2topconn):
		for ii,(word2,w) in enumerate(word2topconn[word1]):
			try:
				dx={'word1':word1, 'word2':word2}
				dx['word1_rank']=model.freqs[word1]
				dx['word2_rank']=model.freqs[word2]
				dx['closeness_cosine']=w
				dx['closeness_rank']=ii+1
				dx['model']=model.name
				for i,x in enumerate(model.name.split('.')):
					dx['model_name_'+str(i+1)]=x
			except KeyError:
				continue
			old+=[dx]
	model.unload()
	return old


def do_meta_dist(xxx_todo_changeme1):
	(model,words,min_count,max_rank) = xxx_todo_changeme1
	model.load()
	if min_count: model.limit_by_count(min_count)
	if max_rank: model.limit_by_rank(max_rank)
	result=model.dist(words=words,use_nan=True)
	model.unload()
	return result









#### FUNCTIONS

import gensim

def load_model(model_or_path):
	if type(model_or_path) in {str,six.text_type}:
		return gensim.models.KeyedVectors.load_word2vec_format(fnfn)
	else:
		return model

def limit_words(model,num_top_words=None,min_count=None):
	if not num_top_words and not min_count:
		raise Exception('Please specify either num_top_words, min_count, or both.')

	vocab = model.wv.vocab


def semantic_displacement(self,models1,models2,ofn='data.semantic_displacement.txt',min_count=None,neighborhood_size=10,use_all_possible_pairs=False):
	"""
	Measure semantic displacement
	@TODO doc
	"""

	# Generate pairings
	pairs = []
	if use_all_possible_pairs:
		for m1 in models1:
			for m2 in models2:
				pairs+=[(m1,m2)]
	else:
		min_len = min([len(models1),len(models2)])
		for i in range(min_len):
			pairs+=[(models1[i],models2[i])]


	def _writegen():
		vectord=None
		for m1,m2 in pairs:
			m1=load_model(m1)
			m2=load_model(m2)

			if min_count: m1.limit_by_count(min_count)
			if min_count: m2.limit_by_count(min_count)

			word1stat,word2stat={},{}
			words_ok=set(words) & set(m1.gensim.vocab.keys()) & set(m2.gensim.vocab.keys())
			for word in words_ok: word1stat[word]=(m1.gensim.vocab[word].index,m1.gensim.vocab[word].count)
			for word in words_ok: word2stat[word]=(m2.gensim.vocab[word].index,m2.gensim.vocab[word].count)

			m1.align(m2,words=words)

			vectord1=m1.abstract_vectors(include_social=True,only_major=True)
			vectord2=m2.abstract_vectors(include_social=True,only_major=True)

			for word in words_ok:
				try:
					cos=m1.similarity(word,m2)
				except KeyError:
					continue

				## Major stats
				odx={'model1':m1.name, 'model2':m2.name, 'word':word, 'cosine_similarity':cos}
				for k,v in list(m1.named.items()): odx[k+'_1']=v
				for k,v in list(m2.named.items()): odx[k+'_2']=v
				odx['model_rank_1'],odx['model_count_1']=word1stat[word]
				odx['model_rank_2'],odx['model_count_2']=word2stat[word]
				neighborhood1 = [w for w,c in m1.similar(word,neighborhood_size)]
				neighborhood2 = [w for w,c in m2.similar(word,neighborhood_size)]
				odx['neighborhood_1']=', '.join(neighborhood1)
				odx['neighborhood_2']=', '.join(neighborhood2)

				## Get displacement measure #2 (noun based)
				wordset1=set(neighborhood1)
				wordset2=set(neighborhood2)
				all_words = list(wordset1|wordset2)
				# DO I NEED THIS? -->
				#all_words = [x for x in all_words if x in m1.gensim.vocab and x in m2.gensim.vocab and m1.gensim.vocab[x].index<=min_rank and m2.gensim.vocab[x].index<=min_rank]
				#if len(all_words)<3: return {}
				vector1 = [m1.gensim.similarity(word,word2) for word2 in all_words]
				vector2 = [m2.gensim.similarity(word,word2) for word2 in all_words]
				odx['cosine_similarity_by_neighborhood']=1-cosine(vector1,vector2)

				## Vector stats on abstract vectors (optional)
				vectors = set(vectord1.keys()) & set(vectord2.keys())
				for k,v in list(m1.cosine_word(word,vectord1).items()): odx['vec_'+k+'_1']=v
				for k,v in list(m2.cosine_word(word,vectord2).items()): odx['vec_'+k+'_2']=v
				for vec in vectors: odx['vec_'+vec+'_2-1']=odx['vec_'+vec+'_2'] - odx['vec_'+vec+'_1']


				"""
				# jaccard
				list1=[w for w,c in m1.similar(word,topn=25)]
				list2=[w for w,c in m2.similar(word,topn=25)]
				set1=set(list1)
				set2=set(list2)
				odx['jaccard']=len(set1&set2) / float(len(set1|set2))
				odx['neighborhood_1_not_2']=', '.join(sorted(set1-set2,key=lambda w: list1.index(w)))
				odx['neighborhood_2_not_1']=', '.join(sorted(set2-set1,key=lambda w: list2.index(w)))
				"""

				yield odx


			m1.unload()
			m2.unload()

	# also do summary

	def _writegen_meta(key_str=('model_name_2_1','model_name_2_2','word')):
		WordMeta={}
		for dx in tools.writegengen(ofn, _writegen):
			key=tuple([dx[k] for k in key_str])
			if not key in WordMeta: WordMeta[key]=defaultdict(list)
			#for k in ['cosine_similarity','jaccard','model_rank_1','model_rank_2','model_count_1','model_count_2','neighborhood_1_not_2','neighborhood_2_not_1']:
			for k in dx:
				#if 'model' in k: continue
				WordMeta[key][k]+=[dx[k]]

		for key in WordMeta:
			metadx=dict(list(zip(key_str,key)))
			metadx['num_records']=len(WordMeta[key]['cosine_similarity'])
			for k in WordMeta[key]:
				try:
					metadx[k]=np.median(WordMeta[key][k])
				except (ValueError,TypeError) as e:
					if not 'neighborhood' in k: continue
					vals = [w for liststr in WordMeta[key][k] for w in liststr.split(', ')]
					cntr = Counter(vals)
					newstr = ', '.join(['{} ({})'.format(a,b) for a,b in cntr.most_common(10)])
					metadx[k]=newstr
			yield metadx

	ofn_summary=ofn.replace('.txt','.summarized.txt')


	for dx in tools.writegengen(ofn_summary, _writegen_meta): yield dx
