import os,random
import lltk
from lltk import tools
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
from lltk import tools
import json,re
import shutil
import pandas as pd
from htrc_features import Volume
from urllib.error import HTTPError


########################################################################################################################
# [Hathi]
# (1) Text Class
########################################################################################################################

class TextHathi(Text):
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

class Hathi(Corpus):
	TEXT_CLASS=TextHathi
	ID='hathi'
	NAME='Hathi'
	LANGS=['eng']
	SEARCH_TERMS=[]
	METADATA_HEADER='htid	access	rights	ht_bib_key	description	source	source_bib_num	oclc_num	isbn	issn	lccn	title	imprint	rights_reason_code	rights_timestamp	us_gov_doc_flag	rights_date_used	pub_place	lang	bib_fmt	collection_code	content_provider_code	responsible_entity_code	digitization_agent_code	access_profile_code	author'.split('\t')

	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################

	def __init__(self,*x,**y):
		super().__init__(*x,**y)
		# if not os.path.exists(self.path_root): os.makedirs(self.path_root)
		self.path_metadata=os.path.join(self.path_root,'metadata.csv')

	@property
	def path_full_metadata(self):
		import lltk
		hc=lltk.load('Hathi')
		return os.path.join(hc.path_data,'metadata_full.txt.gz')

	def download_full_metadata(self):
		if not os.path.exists(self.path_full_metadata):
			if not os.path.exists(os.path.dirname(self.path_full_metadata)):
				os.makedirs(os.path.dirname(self.path_full_metadata))

			import lltk
			hc=lltk.load('Hathi')
			tools.download(hc.url_full_metadata, self.path_full_metadata)

	

	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		from tqdm import tqdm
		
		if not os.path.exists(self.path_root): os.makedirs(self.path_root)
		if not os.path.exists(self.path_home): os.makedirs(self.path_home)

		
		if not os.path.exists(self.path_metadata):
			self.download_full_metadata()
			print('>>',self.path_metadata)


			print('>> finding metadata matching search terms:',self.SEARCH_TERMS)
			import gzip
			i_title=self.METADATA_HEADER.index('title')
			with gzip.open(self.path_full_metadata) as f, open(self.path_metadata,'w') as of:
				of.write('\t'.join(self.METADATA_HEADER) +'\n')
				for ln in tqdm(f):
					ln=ln.decode()
					dat=ln.split('\t')
					title=dat[i_title]
					if not title: continue
					title=title.strip().lower()
					titlewords=set(re.findall(r"[\w']+|[.,!?;]", title))
					match = bool(self.SEARCH_TERMS & titlewords)
					if not match: continue
					of.write(ln)
				
			# fix!
			df=pd.read_csv(self.path_metadata,sep='\t',error_bad_lines=False)
			df=df[df.lang.isin(self.LANGS)]
			df['id']=df['htid'].apply(lambda x: x.split('.',1)[0]+'/'+x.split('.',1)[1])
			df.to_csv(self.path_metadata)



		# get IDs
		print('>> loading metadata')
		df=pd.read_csv(self.path_metadata,error_bad_lines=False)
		df['year']=df['imprint'].apply(get_date)
		df['period']=df['year']//1*1
		ids=list(set(df.groupby('period').sample(n=10,replace=True).id))
		random.shuffle(ids)

		# compile!
		tools.pmap(compile_text, ids, num_proc=6)


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

def get_date(imprint):
	for x in tools.ngram(str(imprint),4):
		x=''.join(x)
		try:
			return int(x)
		except Exception:
			return x

class HathiSubcorpus(Hathi):
	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']=self.GENRE
		meta['year']=meta['imprint'].apply(get_date)
		return meta


class HathiSermons(HathiSubcorpus):
	ID='hathi_sermons'
	NAME='HathiSermons'
	SEARCH_TERMS={'sermon','sermons'}
	GENRE='Sermon'


class HathiProclamations(HathiSubcorpus):
	ID='hathi_proclamations'
	NAME='HathiProclamations'
	SEARCH_TERMS={'proclamation','proclamation'}
	GENRE='Proclamation'


class HathiEssays(HathiSubcorpus):
	ID='hathi_essays'
	NAME='HathiEssays'
	SEARCH_TERMS={'essay','essays'}
	GENRE='Essay'


class HathiLetters(HathiSubcorpus):
	ID='hathi_letters'
	NAME='HathiLetters'
	SEARCH_TERMS={'letter','letters'}
	GENRE='Letters'

class HathiTreatises(HathiSubcorpus):
	ID='hathi_treatises'
	NAME='HathiTreatises'
	SEARCH_TERMS={'treatise','treatises'}
	GENRE='Treatise'


class HathiTales(HathiSubcorpus):
	ID='hathi_tales'
	NAME='HathiTales'
	SEARCH_TERMS={'tale','tales'}
	GENRE='Tale'

class HathiNovels(HathiSubcorpus):
	ID='hathi_novels'
	NAME='HathiNovels'
	SEARCH_TERMS={'novel','novels'}
	GENRE='Novel'

class HathiStories(HathiSubcorpus):
	ID='hathi_stories'
	NAME='HathiStories'
	SEARCH_TERMS={'story','stories'}
	GENRE='Story'




def compile_text(idx,by_page=False):
	try:
		path_freqs = os.path.join(lltk.load('Hathi').path_freqs,idx+'.json')
		path_freqs_dir = os.path.dirname(path_freqs)
		if os.path.exists(path_freqs): return
		if not os.path.exists(path_freqs_dir):
			try:
				os.makedirs(path_freqs_dir)
			except FileExistsError:
				pass

		# print('compiling!')
		htid=idx.replace('/','.',1)
		# print('Getting: ',htid)
		vol=Volume(htid)
		vol_freqs=vol.term_volume_freqs(pos=False,case=True)
		vol_freqs_d=dict(zip(vol_freqs['token'],vol_freqs['count']))
		with open(path_freqs,'w') as of:
			json.dump(vol_freqs_d, of)

	except (HTTPError,FileNotFoundError,KeyError) as e:
		# print('!!',e)
		pass





# def recompile_text(htid,by_page=False):
# 	id_hash = tools.hash(htid)[:9]
# 	id_split = '/'.join(tools.slice(id_hash,slice_length=3))
# 	id_text = f'''hathi/{id_split}''' #/{htid.replace('/','_')}'''
# 	path_text = os.path.join(
# 		lltk.load('Hathi').path_texts,
# 		*id_text.split('/')[1:]
# 	)

# 	# old path
# 	path_freqs=os.path.join(path_text,'freqs.json').replace('/home/ryan/lltk_data/corpora/hathi','/home/ryan/DH/backups/hathi')
# 	if not os.path.exists(path_freqs): return

# 	# get new path
# 	htidx=htid.split('.',1)[0] + '/' + htid.split('.',1)[1]
# 	new_path_freqs = os.path.join(lltk.load('Hathi').path_freqs, htidx+'.json')
# 	if os.path.exists(new_path_freqs): return
	
# 	# save new
# 	new_path_freqs_dir = os.path.dirname(new_path_freqs)
# 	try:
# 		if not os.path.exists(new_path_freqs_dir): os.makedirs(new_path_freqs_dir)
# 	except FileExistsError:
# 		pass
# 	shutil.copyfile(path_freqs,new_path_freqs)
# 	# print(path_freqs,'-->',new_path_freqs)



# def compile_text(htid,by_page=False):
# 	try:
# 		from htrc_features import Volume
# 		from urllib.error import HTTPError
		
# 		id_hash = tools.hash(htid)[:9]
# 		id_split = '/'.join(tools.slice(id_hash,slice_length=3))
		
# 		id_text = f'''hathi/{id_split}''' #/{htid.replace('/','_')}'''
# 		path_text = os.path.join(
# 			lltk.load('Hathi').path_texts,
# 			*id_text.split('/')[1:]
# 		)
# 		if os.path.exists(path_text) and os.listdir(path_text): return
# 		# print('compiling!')
# 		vol=Volume(htid)
# 		vol_meta = vol.parser.meta
# 		vol_df = vol.tokenlist(case=True,pos=False)
# 		vol_df = vol_df.reset_index()

# 		if not os.path.exists(path_text): os.makedirs(path_text)
# 		# print(id_text)
# 		# print(path_text)
# 		path_meta=os.path.join(path_text,'meta.json')
# 		vol_meta['_id']=id_text
# 		vol_meta['_type']='volume'
# 		vol_meta['htid']=htid
# 		with open(path_meta,'w') as of:
# 			json.dump(vol_meta,of,indent=4,sort_keys=True)

# 		if by_page:

# 			for page_num,page in vol_df.groupby('page'):
# 				page_counts=dict(zip(page['token'],page['count']))
# 				id_section = os.path.join(id_text,str(page_num).zfill(4))
# 				path_section = os.path.join(path_text,str(page_num).zfill(4))
# 				if not os.path.exists(path_section): os.makedirs(path_section)
# 				path_section_freqs = os.path.join(path_section,'freqs.json')
# 				with open(path_section_freqs,'w') as of:
# 					json.dump(page_counts,of,indent=4,sort_keys=True)
# 				path_section_meta = os.path.join(path_section,'meta.json')
# 				with open(path_section_meta,'w') as of:
# 					json.dump({
# 						'page':page_num,
# 						'_id':id_section,
# 						'_type':'page'
# 					},of,indent=4,sort_keys=True)
		
# 		# save total freqs too
# 		vol_freqs=vol.term_volume_freqs(pos=False,case=True)
# 		vol_freqs_d=dict(zip(vol_freqs['token'],vol_freqs['count']))
# 		with open(os.path.join(path_text,'freqs.json'),'w') as of:
# 			json.dump(vol_freqs_d, of)
# 		vol_df.to_csv(os.path.join(path_text,'freqs.csv'))

# 	except (HTTPError,FileNotFoundError,KeyError) as e:
# 		print('!!',e)










########################################################################################################################
