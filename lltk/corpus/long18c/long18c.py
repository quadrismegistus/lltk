from __future__ import absolute_import
from __future__ import print_function
# -*- coding: utf-8 -*-

from lltk.corpus.corpus import Corpus
from lltk.text.text import Text
import os

class Long18C(Corpus):
	TEXT_CLASS=Text
	PATH_TXT = 'long18c/_txt_long18c'
	PATH_XML = 'long18c/_xml_long18c'
	PATH_METADATA = 'long18c/corpus-metadata.Long18C.xlsx'

	def __init__(self):
		super(Long18C,self).__init__('Long18C',path_txt=self.PATH_TXT,path_xml=self.PATH_XML,path_metadata=self.PATH_METADATA)
		self.path = os.path.dirname(__file__)
		self.year_start = 1600
		self.year_end = 1840

	# def word2vec_by_period(self,bin_years_by=10,word_size=None,skipgram_n=10, year_min=None, year_max=None,partial=True):
	# 	"""NEW word2vec_by_period using skipgram txt files
	# 	DOES NOT YET IMPLEMENT word_size!!!
	# 	"""
	# 	from lltk.model.word2vec import Word2Vec,Word2Vecs
	#
	# 	if not year_min: year_min=self.year_start
	# 	if not year_max: year_max=self.year_end
	#
	# 	path_model = os.path.join(self.path_model,'partial_models') if partial else os.path.join(self.path_model,'full_models')
	# 	model_fns = os.listdir(path_model)
	# 	model_fns2=[]
	#
	# 	for mfn in model_fns:
	# 		if not mfn.endswith('.model.txt.gz'): continue
	# 		mfn_l = mfn.split('.')
	# 		corpus_name=mfn_l[1]
	# 		if corpus_name!=self.name: continue
	# 		period=mfn_l[2]
	# 		if not '-' in period: continue
	# 		period_start,period_end=period.split('-')
	# 		period_start=int(period_start)
	# 		period_end=int(period_end)+1
	# 		if period_end-period_start!=bin_years_by: continue
	# 		if period_start<year_min: continue
	# 		if period_end>year_max: continue
	#
	# 		model_fns2+=[mfn]
	#
	# 	print '>> loading:', sorted(model_fns2)
	#
	# 	name_l=[self.name, 'by_period', str(bin_years_by)+'years']
	# 	if word_size: name_l+=[str(word_size / 1000000)+'Mwords']
	# 	w2vs_name = '.'.join(name_l)
	# 	W2Vs=Word2Vecs(corpus=self, fns=model_fns2, skipgram_n=skipgram_n, name=w2vs_name, partial=partial)
	# 	return W2Vs

	def symlink_word2vec_models(self, model_idir='/Users/ryan/Dropbox/PHD/DH/18C/sherlock/word2vec/models',model_odir='/Users/ryan/Dropbox/PHD/DH/18C/word2vec_models/long18c/bydecade/full',skipgram_n=10):
		"""
		Make symbolic links in the word2vec folder
		to the models in /DH/18C/word2vec/sherlock/models.

		For now, ignore the decade-spanning ones.
		"""

		#path_model=os.path.join(e)

		for fn in os.listdir(model_idir):
			if not fn.endswith('.txt.gz'): continue
			if self.name in fn:
				## then 'lltk' format
				fnfn=os.path.join(model_idir,fn)
				fnfn_vocab=fnfn.replace('.txt.gz','.vocab.txt')
				newfn=fn
				newfnfn=os.path.join(model_odir,newfn)
				newfnfn_vocab=newfnfn.replace('.txt.gz','.vocab.txt')

				print(newfnfn,'-->',fnfn)
				print(newfnfn_vocab,'-->',fnfn_vocab)
				print()
				#continue


				if os.path.exists(newfnfn): os.remove(newfnfn)
				if os.path.exists(newfnfn_vocab): os.remove(newfnfn_vocab)
				os.symlink(fnfn,newfnfn)
				os.symlink(fnfn_vocab,newfnfn_vocab)
			else:
				# sherlock format
				filename_pieces = fn.split('.')
				corpus = filename_pieces[1]
				decade = filename_pieces[2]
				if '-' in decade: continue
				if not corpus in ['ecco','eebo','ncco']: continue
				dec=int(decade)
				if dec<self.year_start: continue
				if dec>=self.year_end: continue

				fnfn=os.path.join(model_idir,fn)
				fnfn_vocab=fnfn.replace('.txt.gz','.vocab.txt')
				newfn='word2vec.Long18C.'+decade+'-'+str(dec+9)+'.skipgram_n='+str(skipgram_n)+'.model.txt.gz'
				newfnfn=os.path.join(model_odir,newfn)
				newfnfn_vocab=newfnfn.replace('.txt.gz','.vocab.txt')

				print(newfnfn,'-->',fnfn)
				print(newfnfn_vocab,'-->',fnfn_vocab)
				print()
				#continue

				#"""
				if os.path.exists(newfnfn): os.remove(newfnfn)
				if os.path.exists(newfnfn_vocab): os.remove(newfnfn_vocab)
				os.symlink(fnfn,newfnfn)
				os.symlink(fnfn_vocab,newfnfn_vocab)
				#"""
