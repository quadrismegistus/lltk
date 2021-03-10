from lltk.imports import *
import internetarchive as ia
from internetarchive import search_items


DEFAULT_COLLECTION='19thcennov'
class TextInternetArchive(Text): pass
class InternetArchive(Corpus):
	TEXT_CLASS=TextInternetArchive
	def get_collection_ids(self,collection=DEFAULT_COLLECTION,iter_as_items=False):
		# search
		idl=[]
		search = search_items('collection:'+collection)
		total=search.num_found
		if iter_as_items: search=search.iter_as_items()
		print(f'>> [{self.name}] scanning',total,f'items in collection {collection}')
		# loop
		for i,result in enumerate(tqdm(search,total=total)):
			yield result['identifier'] if not iter_as_items else result

	def compile_txt(self,collection=DEFAULT_COLLECTION):
		"""
		This will download the txt files from IA.
		"""
		# make sure exists
		import shutil
		if not os.path.exists(self.path_txt): os.makedirs(self.path_txt)
		os.chdir(self.path_txt)

		# getting ids
		print(f'>> [{self.name}] downloading txt files, using custom function...')
		id_list=self.get_collection_ids(collection=collection)

		# download txt
		for i,idx in enumerate(tqdm(id_list,position=1)):
			if os.path.exists(idx+'.txt'): continue
			ia.download(idx,silent=True,glob_pattern='*.txt',ignore_existing=True)
			
			# get files and rename]
			if not os.path.exists(idx): continue
			for fn in os.listdir(idx):
				if not fn.endswith('.txt'): continue
				fnfn=os.path.join(idx,fn)
				os.rename(fnfn, idx+'.txt')
			if os.path.exists(idx): shutil.rmtree(idx)


	def compile_metadata(self,collection=DEFAULT_COLLECTION,force=False):
		if force or not os.path.exists(self.path_metadata):
			def _writegen():
				for item in tqdm(list(self.get_collection_ids(collection=collection,iter_as_items=True)),desc='Compiling metadata'):
					dx=item.metadata
					try:
						dx['id']=dx['identifier']+'/'+dx['identifier']+'_djvu'
					except KeyError:
						print('??? no identifier ???',dx,'\n')
					#print(dx)
					yield dx
			tools.iter_move(self.path_metadata,prefix='bak/')
			tools.writegen(self.path_metadata, _writegen)
		


	def compile(self):
		self.compile_metadata()
		self.compile_txt()


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


	def boot(self,parts=['metadata','txt','freqs','mfw','dtm'],force=False,**attrs):
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
		meta['year']=[int(str(x)[:4]) if str(x)[:4].isdigit() else np.nan for x in meta['date']]
		meta['author']=meta['creator']
		return meta















########################################################################################################################
