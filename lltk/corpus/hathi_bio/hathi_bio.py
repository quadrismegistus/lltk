import os
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
from tqdm import tqdm
from lltk import tools
import gzip,tarfile,sys
try:
	import ujson as json
except ImportError:
	import json

CORPUS_URL="https://wiki.htrc.illinois.edu/display/COM/Word+Frequencies+in+English-Language+Literature%2C+1700-1922"

CORPUS_DOWNLOAD_URL="https://www.ideals.illinois.edu/bitstream/handle/2142/99554/data_for_chapter1.tar.gz"
CORPUS_METADATA_URL="https://raw.githubusercontent.com/tedunderwood/horizon/master/chapter1/metadata/hathigenremeta.csv"

PATH_HERE=os.path.abspath(__file__)
PATH_HERE_DIRNAME=os.path.dirname(PATH_HERE)



########################################################################################################################
# [HathiBio]
# (1) Text Class
########################################################################################################################

class TextHathiBio(Text):
	pass





########################################################################################################################
# (2) Corpus Class
########################################################################################################################


def htid2id(htid):
	prefix,body=htid.split('.',1)
	body_prefix,body_body=body[:3],body[3:]
	return '/'.join([prefix,body_prefix,body_body])

def freq_tsv2dict(freq_str):
	from nltk import word_tokenize
	from collections import Counter
	
	d=Counter()
	for ln in freq_str.strip().split('\n'):
		try:
			word,count=ln.split('\t')
			for word_token in word_tokenize(clean_text(word.strip().lower())):
				d[word_token]+=int(count)
		except ValueError:
			pass
	return d


def untar_to_freqs_folder(args):
	#print(args)
	fnfn,path_freqs,position=args
	with gzip.GzipFile(fnfn) as f:
		with tarfile.open(fileobj=f) as tf:
			members=tf.getmembers()
			for member in tqdm(members,position=position,desc='untarring a file'):
				if not member.name.endswith('.tsv'): continue
				if os.path.basename(member.name).startswith('.'): continue
				try:
					ofnfn=os.path.join(path_freqs, htid2id(os.path.splitext(os.path.basename(member.name))[0]) + '.json')
				except ValueError:
					continue # not hathi?

				if os.path.exists(ofnfn): continue
				f = tf.extractfile(member)
				if f is not None:
					content = f.read().decode('utf-8',errors='ignore')
					freq_dict = freq_tsv2dict(content)

					if not os.path.exists(os.path.dirname(ofnfn)):
						#print(ofnfn)
						os.makedirs(os.path.dirname(ofnfn))
					with open(ofnfn,'w') as of:
						json.dump(freq_dict, of)



class HathiBio(Corpus):
	TEXT_CLASS=TextHathiBio


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################


	def compile_download(self):
		if not os.path.exists(self.path_raw): os.makedirs(self.path_raw)
		url=CORPUS_DOWNLOAD_URL
		ofnfn=os.path.join(self.path_raw,os.path.basename(url))
		tools.download(url,ofnfn,overwrite=False)
		#tools.unzip(ofnfn)

	def compile_metadata(self,force=False):
		if not os.path.exists(self.path_metadata) or force:
			tools.download(CORPUS_METADATA_URL, self.path_metadata)

	def save_metadata(self):
		self.compile_metadata(force=True)

		import pandas as pd
		df=pd.read_csv(self.path_metadata)
		df['id']=df.docid.apply(htid2id)
		df['year']=df['date']
		#print(df.shape)
		first_cols=['id','docid']
		other_cols=[col for col in df.columns if not col in first_cols]
		df[first_cols + other_cols].to_csv(self.path_metadata,index=False)


	def compile_data(self,parallel=1,sbatch=False,sbatch_hours=1):
		import tarfile,gzip
		import multiprocessing as mp
		import time
		if not parallel: parallel=1

		filenames = [os.path.join(self.path_raw,fn) for fn in os.listdir(self.path_raw) if fn.endswith('.tar.gz')]
		objects = [(fn,self.path_freqs,i%int(parallel)) for i,fn in enumerate(filenames)]
		#print(objects)
		if int(parallel)<2 and not sbatch:
			for i,object in enumerate(tqdm(objects,desc='looping over files')):
				untar_to_freqs_folder(object)
		elif parallel and not sbatch:
			pool=mp.Pool(int(parallel))
			pool.map(untar_to_freqs_folder,objects)
		elif sbatch:
			for object in objects:
				cmd=f'python -c \\"import sys; sys.path.insert(0,\'{PATH_HERE_DIRNAME}\'); import {self.id} as mod; object={object}; mod.untar_to_freqs_folder(object)\\"'
				sbatch_min=sbatch_hours*60
				sbatch_cmd=f'sbatch -p hns -t {sbatch_min} --wrap="{cmd}"'
				print('>>',sbatch_cmd)
				os.system(sbatch_cmd)
				time.sleep(1)







	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		self.compile_download()
		self.compile_metadata()
		self.compile_data(parallel=attrs.get('parallel'), sbatch=attrs.get('sbatch'))


	def preprocess(self,parts=['metadata','txt','freqs','mfw','dtm'],force=False,**attrs):
		"""
		This function is used to boot the corpus, taking it from its raw (just downloaded) to refined condition:
			- metadata: Save metadata (if necessary)
			- txt: Save plain text versions (if necessary)
			- freqs: Save json frequency files per text
			- mfw: Save a long list of all words sorted by frequency
			- dtm: Save a document-term matrix
		"""
		return super().install(parts=parts,force=force,**attrs)


	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']='Biography'
		return meta#.query(f'{self.MIN_YEAR}<=year<{self.MAX_YEAR}')	



















########################################################################################################################
