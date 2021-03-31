from lltk.imports import *

class TextChicago(Text): pass

class Chicago(Corpus):
	TEXT_CLASS=TextChicago
	RAW_FN_METADATA_CSV_AUTHORS = 'AUTHORS_METADATA.csv'
	RAW_FN_METADATA_CSV_TEXTS = 'NOVELS_METADATA.csv'
	RAW_FOLDER_TEXTS='Texts'
	EXT_TXT = '.txt'


	def compile(self):
		path_author_metadata=os.path.join(self.path_raw,self.RAW_FN_METADATA_CSV_AUTHORS)
		path_novels_metadata=os.path.join(self.path_raw,self.RAW_FN_METADATA_CSV_TEXTS)
		path_texts = os.path.join(self.path_raw,self.RAW_FOLDER_TEXTS)

		if any([not os.path.exists(_path) for _path in [path_author_metadata,path_novels_metadata,path_texts]]):
			print(f"""Instructions for compiling the Chicago Novels corpus.

			First, place into the folder {self.path_raw} the following files:
				* AUTHORS_METADATA.csv
				* NOVELS_METADATA.csv
				* Texts (a folder of 9,089 text files, numbered 00000001.txt to 00026233.txt)
			""")
		else:
			self.compile_txt()
			self.compile_metadata()


	def compile_txt(self,ask=True,default='move'):
		import shutil
		from tqdm import tqdm

		# ask whether move or copy?
		if ask:
			do_move=input(f'>> Move or copy the text files from {self.path_raw} --> {self.path_txt} ?\n[Move/copy] ')
			if not do_move: do_move=default
			if do_move.strip().lower().startswith('m'): do_move='move'
			if do_move.strip().lower().startswith('c'): do_move='copy'
		else:
			do_move = defualt

		path_texts = os.path.join(self.path_raw,self.RAW_FOLDER_TEXTS)
		if not os.path.exists(self.path_txt): os.makedirs(self.path_txt)
		for fn in tqdm(os.listdir(path_texts)):
			if not fn.endswith('.txt') or not fn.startswith('0'): continue
			fnfn=os.path.join(path_texts,fn)
			ofnfn=os.path.join(self.path_txt,fn)

			if do_move=='move':
				shutil.move(fnfn,ofnfn)
			elif do_move=='copy':
				shutil.copyfile(fnfn,ofnfn)
			#print(fnfn,'-->',ofnfn)

	def compile_metadata(self):
		from lltk.tools import read_ld,ld2dd,write_ld
		"""
		Generates a single metadata table from AUTHORS_METADATA.csv and NOVELS_METADATA.csv
		"""
		auth_ld = read_ld(os.path.join(self.path_raw,self.RAW_FN_METADATA_CSV_AUTHORS))
		text_ld = read_ld(os.path.join(self.path_raw,self.RAW_FN_METADATA_CSV_TEXTS))

		auth_dd = ld2dd(auth_ld,'AUTH_ID')
		for d in text_ld:
			auth_id = d['AUTH_ID']
			for k,v in list(auth_dd.get(auth_id,{}).items()): d[k]=v
			d['id']=d['FILENAME'].replace('.txt','')

			# lowercase keys
			keys=list(d.keys())
			for k in keys:
				if not k: continue
				k2=k.lower()
				if k2!=k:
					d[k2]=d[k]
					del d[k]

			# add year and author
			d['year']=d['publ_date']
			d['author']=d['auth_last']+', '+d['auth_first']

		#write_ld(self.path_metadata, text_ld)
		import pandas as pd
		df=pd.DataFrame(text_ld)
		df.to_csv(self.path_metadata, sep='\t')

	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']='Fiction'
		#meta['id']=[str(int(x)).zfill(8) for x in meta.id]
		return meta