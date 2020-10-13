from __future__ import absolute_import
import gensim
from lltk import tools
import six
from six.moves import range
from six.moves import zip

def load_model(model_or_path):
	if type(model_or_path) in {str,six.text_type}:
		return gensim.models.KeyedVectors.load_word2vec_format(fnfn)
	else:
		return model

def limit_words(model,num_top_words=None,min_count=None):
	if not num_top_words and not min_count:
		raise Exception('Please specify either num_top_words, min_count, or both.')

	vocab = model.wv.vocab
	words = list(vocab.keys())
	for word in words:
		if min_count and vocab[word].count<min_count:
			del vocab[word]
		elif num_top_words and vocab[word].index>num_top_words:
			del vocab[word]


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
