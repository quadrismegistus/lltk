from lltk.imports import *
from lltk.corpus.hathi import htid2id

CORPUS_URL="https://wiki.htrc.illinois.edu/display/COM/Word+Frequencies+in+English-Language+Literature%2C+1700-1922"

CORPUS_DOWNLOAD_URLS="""
http://data.analytics.hathitrust.org/genre/fiction_metadata.csv
http://data.analytics.hathitrust.org/genre/fiction_yearly_summary.csv
http://data.analytics.hathitrust.org/genre/fiction_contextual_corrections.csv
http://data.analytics.hathitrust.org/genre/fiction_1700-1799.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1800-1834.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1835-1869.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1870-1879.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1880-1889.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1890-1894.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1895-1899.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1900-1904.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1905-1909.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1910-1914.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1915-1919.tar.gz
http://data.analytics.hathitrust.org/genre/fiction_1920-1922.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_metadata.csv
http://data.analytics.hathitrust.org/genre/poetry_yearly_summary.csv
http://data.analytics.hathitrust.org/genre/poetry_contextual_corrections.csv
http://data.analytics.hathitrust.org/genre/poetry_1700-1799.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1800-1834.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1835-1869.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1870-1879.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1880-1889.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1890-1894.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1895-1899.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1900-1904.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1905-1909.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1910-1914.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1915-1919.tar.gz
http://data.analytics.hathitrust.org/genre/poetry_1920-1922.tar.gz
http://data.analytics.hathitrust.org/genre/drama_metadata.csv
http://data.analytics.hathitrust.org/genre/drama_yearly_summary.csv
http://data.analytics.hathitrust.org/genre/drama_contextual_corrections.csv
http://data.analytics.hathitrust.org/genre/drama_1700-1799.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1800-1834.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1835-1869.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1870-1879.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1880-1889.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1890-1894.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1895-1899.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1900-1904.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1905-1909.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1910-1914.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1915-1919.tar.gz
http://data.analytics.hathitrust.org/genre/drama_1920-1922.tar.gz
""".strip().split()

PATH_HERE=os.path.abspath(__file__)
PATH_HERE_DIRNAME=os.path.dirname(PATH_HERE)


class TextHathiEngLit(Text): pass


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
	fnfn,path_freqs,position=args
	with gzip.GzipFile(fnfn) as f:
		with tarfile.open(fileobj=f) as tf:
			members=tf.getmembers()
			for member in tqdm(members,position=position,desc='untarring a file'):
				ofnfn=os.path.join(path_freqs, htid2id(os.path.splitext(os.path.basename(member.name))[0]) + '.json')
				if os.path.exists(ofnfn): continue
				f = tf.extractfile(member)
				if f is not None:
					content = f.read().decode('utf-8')
					freq_dict = freq_tsv2dict(content)

					if not os.path.exists(os.path.dirname(ofnfn)): os.makedirs(os.path.dirname(ofnfn))
					with open(ofnfn,'w') as of:
						json.dump(freq_dict, of)



class HathiEngLit(Corpus):
	TEXT_CLASS=TextHathiEngLit


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################


	def compile_download(self):
		if not os.path.exists(self.path_raw): os.makedirs(self.path_raw)
		for url in tqdm(CORPUS_DOWNLOAD_URLS,position=1):
			ofnfn=os.path.join(self.path_raw,os.path.basename(url))
			if os.path.exists(ofnfn): continue
			tools.download(url,ofnfn)

	def compile_metadata(self):
		all_meta=[]
		for fn in os.listdir(self.path_raw):
			if fn.endswith('metadata.csv'):
				fdf=read_df(os.path.join(self.path_raw,fn))
				fdf['genre']=fn.split('_')[0].title()
				all_meta+=[fdf]
		import pandas as pd
		df=pd.concat(all_meta)
		df['id']=df.htid.apply(htid2id)
		df['year']=pd.to_numeric(df['date'],errors='coerce')
		df=fix_meta(df).set_index('id')
		df.to_csv(self.path_metadata)
		return df 

	def compile_data(self,parallel=1,sbatch=False,sbatch_hours=1):
		if not parallel: parallel=DEFAULT_NUM_PROC
		filenames = [os.path.join(self.path_raw,fn) for fn in os.listdir(self.path_raw) if fn.endswith('.tar.gz')]
		objects = [(fn,self.path_freqs,i%int(parallel)) for i,fn in enumerate(filenames)]
		pmap(untar_to_freqs_folder,objects,num_proc=parallel)







	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		self.compile_download()
		self.compile_metadata()
		self.compile_data(parallel=attrs.get('parallel'), sbatch=attrs.get('sbatch'))


	def download(self,**attrs):
		"""
		This function is used to download the corpus. Leave as-is to use built-in LLTK download system.
		Provide a

		So far, downloadable data types (for certain corpora) are:
			a) `txt` files
			b) `xml` files
			c) `metadata` files
			d) `freqs` files

		If you have another zip folder of txt files you'd like to download,
		you can specify with `url_txt` (i.e. url_`type`, where type is in `quotes` in (a)-(d) above):
			corpus.download(url_txt="https://www.etcetera.com/etc.zip")
		"""
		return super().download(**attrs)

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
		import numpy as np
		meta=super().load_metadata()
		meta['year']=[int(x) if x.isdigit() else np.nan for x in meta['startdate']]
		return meta



















########################################################################################################################
