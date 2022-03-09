import os
from lltk.text.text import BaseText
from lltk.corpus.corpus import BaseCorpus
from lltk import tools
from tqdm import tqdm
import shutil


# constants
genre2folder={'SPOK':'text_spoken_kde'}
genre2folder['NEWS']='text_newspaper_lsp'
genre2folder['MAG']='text_magazine_qch'
genre2folder['ACAD']='text_academic_rpe'
genre2folder['FIC']='text_fiction_awq'

folder2genre=dict((v,k) for k,v in genre2folder.items())




########################################################################################################################
# [COCA]
# (1) Text Class
########################################################################################################################

class TextCOCA(BaseText):
	pass


########################################################################################################################
# (2) Corpus Class
########################################################################################################################

class COCA(BaseCorpus):
	TEXT_CLASS=TextCOCA


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################

	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""


		path_to_txt = os.path.join(self.path_raw,'COCA Text')
		path_to_sources = os.path.join(self.path_raw,'coca-sources_2017_12.txt')

		if not os.path.exists(path_to_txt) or not os.path.exists(path_to_sources):
			print(f'Place in {self.path_raw} the following files:\n  * COCA Text\n  * coca-sources_2017_12.txt')
			return

		#txt
		self.compile_txt()
		# metadata
		self.compile_metadata()

	def compile_metadata(self):
		path_to_sources = os.path.join(self.path_raw,'coca-sources_2017_12.txt')

		def row2id(row):
			return row['genre']+'/'+str(int(row['year']))+'/'+row['textID'] #+'.txt'

		import pandas as pd
		df=pd.read_csv(path_to_sources,sep='\t',encoding='latin1',dtype=str).drop(0).drop('Unnamed: 7',1)
		df['id']=[row2id(row) for index,row in df.iterrows()]
		df.to_csv(self.path_metadata,sep='\t',index=False)

	def compile_txt(self):
		path_to_txt = os.path.join(self.path_raw,'COCA Text')
		all_files = list(os.walk(path_to_txt))
		for root,dirs,files in tqdm(all_files,position=0):
			for fn in tqdm(files,position=1):
				if fn.endswith('.txt'):
					ifnfn=os.path.join(root,fn)
					#print(ifnfn)
					try:
						total=tools.get_num_lines(ifnfn)
					except FileNotFoundError:
						#print('!?',ifnfn)
						continue

					with open(ifnfn) as f:
						for ln in tqdm(f,position=2,total=total):
							ln=ln.strip()
							if not ln.startswith('@@') and not ln.startswith('##'):
								#print('!?!?!',ln[:100],'...')
								continue
							idx=ln[2:].split()[0]


							#old_fnfn=ifnfn
							fldr= os.path.basename(root)
							genre=folder2genre.get(fldr)
							year=''.join([y for y in fn if y.isdigit()])
							if not genre:
								print('!?',genre)
								stop

							new_fnfn=os.path.join(self.path_txt, genre, year,idx+'.txt')

							#print(new_fnfn)
							#stop
							if not os.path.exists(os.path.dirname(new_fnfn)): os.makedirs(os.path.dirname(new_fnfn))
							#shutil.copyfile(old_fnfn,new_fnfn)

							otxt=ln[2:].replace('@ @ @ @ @ @ @ @ @ @','\n\n').replace('<p>','      ')
							with open(new_fnfn,'w') as of:
								of.write(otxt+'\n')


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




















########################################################################################################################
