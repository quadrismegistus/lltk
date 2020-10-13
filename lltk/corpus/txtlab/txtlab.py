import os
from lltk.text import Text
from lltk.corpus import Corpus

########################################################################################################################
# [txtLAB450]
# (1) Text Class
########################################################################################################################

class TexttxtLAB450(Text):
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

class txtLAB450(Corpus):
	TEXT_CLASS=TexttxtLAB450


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################

	def compile(self,zipdir_name='txtlab450-master',**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""

		# move to raw folder
		if not os.path.exists(self.path_raw): os.makedirs(self.path_raw)
		if not os.path.exists(self.path_txt): os.makedirs(self.path_txt)
		os.chdir(self.path_raw)

		# download file
		from lltk import tools
		ofn=f'_tmp_{self.id}_raw.zip'
		if not os.path.exists(ofn):
			print('>> downloading:',self.url_raw,'to',ofn)
			tools.download(self.url_raw, ofn)

		# custom unzip: remove internal directories to unzip directly to txt folder
		print('>> unzipping:',ofn)
		from zipfile import ZipFile
		from tqdm import tqdm
		import shutil
		# Open your .zip file
		with ZipFile(ofn) as zip_file:
			namelist=zip_file.namelist()
			# Loop over each file
			for member in tqdm(iterable=namelist, total=len(namelist)):
				# copy file (taken from zipfile's extract)
				source = zip_file.open(member)
				filename = os.path.basename(member)
				if not filename: continue
				if not os.path.splitext(filename)[1]: continue
				#print([member,source,filename,self.path_txt])
				if filename.endswith('.txt'):
					opath=os.path.join(self.path_txt, filename)
				elif filename=='README.md':
					opath=os.path.join(self.path_root, filename)
					tools.iter_move(opath,prefix='bak/')
				elif filename=='metadata.csv':
					tools.iter_move(self.path_metadata,prefix='bak/')
					mdir,mfn=os.path.split(self.path_metadata)
					if not mfn.endswith('.csv'): mfn=os.path.splitext(mfn)[0]+'.csv'
					opath=os.path.join(mdir,mfn)
				else:
					continue

				# write!
				with open(opath, "wb") as target:
					with source, target:
						shutil.copyfileobj(source, target)

				if not filename.endswith('.txt'): print('>> saved:',opath)

		#if not os.path.exists(self.path_root): shutil.move('README.md',self.path_root)
		#if not os.path.exists(self.path_metadata): os.rename('metadata.csv',self.path_metadata)

		## lastly, fix the metadata real quick
		import pandas as pd
		df=pd.read_csv(self.path_metadata)
		df['id_orig']=[idx for idx in df['id']]
		df['id']=[os.path.splitext(fn)[0] for fn in df['filename']]
		header = ['id','id_orig'] + [col for col in df.columns if col not in {'id','id_orig'}]
		df[header].to_csv(self.path_metadata,index=None)
		return


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

	def install(self,exclude=['metadata','txt'],force=False,**attrs):
		"""
		This function is used to boot the corpus, taking it from its raw (just downloaded) to refined condition:
			- metadata: Save metadata (if necessary)
			- txt: Save plain text versions (if necessary)
			- freqs: Save json frequency files per text
			- mfw: Save a long list of all words sorted by frequency
			- dtm: Save a document-term matrix
		"""
		return super().install(exclude=exclude,force=force,**attrs)




















########################################################################################################################
