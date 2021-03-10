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



########################################################################################################################
# [HathiEngLit]
# (1) Text Class
########################################################################################################################

class TextHathiEngLit(Text):
	"""
	A vanilla text class, useful for customization of the most important methods from the root Text class.
	"""


	####################################################################################################################
	# (1.1) Methods for generating plain text #
	####################################################################################################################


	# (a) Getting plain text from text files
	def text_plain_from_txt(self):
		"""
		This function returns the plain text file, assuming there is a text file copy available.
		It's already doing what it needs to -- loading the text -- in the parent, Text version.
		"""

		# Load text if available
		plain_text_string = super().text_plain_from_txt()

		# Customize this?
		# plain_text_string = plain_text_string.replace('&mdash;',' -- ')

		# make sure to return
		return plain_text_string



	# (b) Getting plain text from XML files
	def text_plain_from_xml(self):
		"""
		In this function, you will need to convert the XML (if it exists) to a plain text version of the text.
		Every XML format is different, so this will need to be custom made.

		Here's an example, though, using the XML parser BeautifulSoup4:

			import bs4                                     # import beautifulsoup
			dom = bs4.BeautifulSoup(self.xml,'lxml')       # read xml (available at self.xml) with beautifulsoup parser
			plain_text_lines=[]                            # new list for lines
			for doc in dom.find_all('body'):               # loop over all <body> tags
				for tag in doc.find_all():                 # loop over all tags
					if tag.name in {'p','l'}:              # skip all except <p> and <l> tags
						plain_text_lines.append(tag.text)  # append the non-html text in these tags as new lines to list

			plain_text_string='\n'.join(plain_text_lines)  # convert to string
			return plain_text_string                       # return string

		Feel free to delete this function if you do not need it (e.g. are not working with XML).
		"""

		return ''



	# (c) Getting plain text, deciding between (a) and (b) above
	def text_plain(self, force_xml = False):
		"""
		This function returns a plain text copy of the text.
		It is an umbrella function which decides between the TXT and XML functions above.
		You probably don't need to modify it.

		By default, it calls:
		    a) text_plain_from_txt() if there is a text file copy
		    b) text_plain_from_xml() if there is an XML file copy
		       (unless force_xml==True, in which case run XML no matter what)
		"""

		# 1) Get plain text from default behavior described just above
		plain_text_string = super().text_plain(force_xml = force_xml)

		# 2) Modify the plain_text here for any reason?
		# plain_text_string = plain_text_string.replace('&mdash;',' -- ')

		# 3) Return the plain_text
		return plain_text_string





	####################################################################################################################
	# (1.2) Methods for generating metadata
	####################################################################################################################


	# (a) Get metadata from the saved corpus metadata csv/tsv/excel file
	def get_meta_from_corpus_metadata(self):
		"""
		If available, this function asks the corpus to load its metadata csv/tsv/excel file (if it hasn't already),
		and then returns the metadata row for this text as a dictionary.
		"""
		# 1) Use default behavior
		meta_dict=super().get_meta_from_corpus_metadata()

		# 2) Customize?
		# ...

		# 3) Return
		return meta_dict



	# (b) Get metadata from XML or other file
	def get_meta_from_file(self):
		"""
		This function returns a metadata dictionary based solely on the files of this text.
		This is particularly useful for corpora built on XML files which include both the metadata
		and the body of the text. Since all XML is different, this function needs to be custom written.
		But here is an example:

			import bs4                                     # import beautifulsoup
			dom = bs4.BeautifulSoup(self.xml,'lxml')       # read xml with beautifulsoup parser
			meta_dict={}                                   # start a new dictionary
			metadata_xml_tags =  ['documentID','ESTCID']   # set a list of xml tags to grab
			for xml_tag in metadata_xml_tags:              # loop over these xml tags
				xml_tag_obj = dom.find(x.lower())          # get xml tag in bs4
				if xml_tag_obj:                            # if one was there
					meta_dict[xml_tag] = xml_tag_obj.text  # store that metadatum
			return meta_dict                               # return dictionary

		Feel free to delete this function if you do not need it (e.g. are not working with XML).
		"""
		meta_dict={}
		#...
		#import bs4
		#dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		#...
		#
		return meta_dict


	# (c) Get metadata as a dictionary, deciding between (d) or (e) just above
	def get_metadata(self):
		"""
		This function is called by the Corpus class to construct...
			Corpus.meta:          a list of dictionaries
			Corpus.metad:         a dictionary, keyed {text_id:text_metadata dictionary}
			Corpus.metadata:      a pandas dataframe

		On the base Text claas, this function attempts to load the corpus metadata by calling in order:
			(a) Text.get_meta_from_corpus_metadata()     # (if from_metadata) metadata from saved corpus metadata file
			(b) Text.get_meta_from_file()                # (if from_files) metadata from files (e.g. XML files)

		If you customize this function by uncommenting step (2) below, your code will run *after* step (b) above.

		If you need to customize step (b) above (i.e. the process of gathering the metadata from files),
		customize the function get_meta_from_file() just above.
		"""
		# (0) Check if we already have it: if so, just return that
		if hasattr(self,'_meta_dict'): return self._meta_dict

		# (1) make sure to call the original function
		self._meta_dict = meta_dict = super().get_metadata(from_metadata=True,from_files=True)

		# (2) add other features? e.g.:
		# meta_dict['new_thing'] = 11
		#if meta_dict.get('author_dob','').isdigit():
		#	meta_dict['author_born_antebellum'] = int(meta['author_dob'])<1861

		# (3) then, return metadata dictionary
		return meta_dict









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
	from lltk.text import clean_text
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
	print(args)
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
			tools.download(url,ofnfn,overwrite=False)

	def compile_metadata(self):
		all_meta_ld=[]
		for fn in os.listdir(self.path_raw):
			if fn.endswith('metadata.csv'):
				all_meta_ld+=tools.read_ld(os.path.join(self.path_raw,fn))
		import pandas as pd
		df=pd.DataFrame(all_meta_ld)
		df['id']=df.htid.apply(htid2id)
		print(df.shape)
		first_cols=['id','htid']
		other_cols=[col for col in df.columns if not col in first_cols]
		df[first_cols + other_cols].to_csv(self.path_metadata,sep='\t')

	def compile_data(self,parallel=1,sbatch=False,sbatch_hours=1):
		import tarfile,gzip
		import multiprocessing as mp
		import time


		filenames = [os.path.join(self.path_raw,fn) for fn in os.listdir(self.path_raw) if fn.endswith('.tar.gz')]
		objects = [(fn,self.path_freqs,i%int(parallel)) for i,fn in enumerate(filenames)]
		if int(parallel)<2 and not sbatch:
			for i,object in enumerate(tqdm(objects,desc='looping over files')):
				untar_to_freqs_folder(objects)
		elif parallel and not sbatch:
			pool=mp.Pool(parallel)
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
