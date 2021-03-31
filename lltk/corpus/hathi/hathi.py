from lltk.imports import *

HATHI_FULL_META_NUMLINES = 17430652

class TextHathi(Text): pass

class Hathi(Corpus):
	TEXT_CLASS=TextHathi
	ID='hathi'
	NAME='Hathi'
	LANGS=['eng']
	SEARCH_TERMS=[]
	METADATA_HEADER='htid	access	rights	ht_bib_key	description	source	source_bib_num	oclc_num	isbn	issn	lccn	title	imprint	rights_reason_code	rights_timestamp	us_gov_doc_flag	rights_date_used	pub_place	lang	bib_fmt	collection_code	content_provider_code	responsible_entity_code	digitization_agent_code	access_profile_code	author'.split('\t')

	@property
	def path_full_metadata(self):
		if not hasattr(self,'_pfm'):
			H=load_corpus('Hathi')
			self._pfm=H.path_metadata
		return self._pfm

	def stream_full_meta(self):
		self.download_full_metadata()
		yield from readgen_csv(self.path_full_metadata, num_lines=HATHI_FULL_META_NUMLINES, desc='Scanning through giant Hathi Trust CSV file')

	def download_full_metadata(self):
		if not os.path.exists(self.path_full_metadata):
			if not os.path.exists(os.path.dirname(self.path_full_metadata)):
				os.makedirs(os.path.dirname(self.path_full_metadata))
			#tools.download(hc.url_full_metadata, self.path_full_metadata)
			H.download(parts=['metadata'])
	
	def load_metadata(self,*x,**y):
		df=super().load_metadata()
		df['year']=df['imprint'].apply(get_date)
		return df

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
			print('>> finding metadata matching search terms:',self.SEARCH_TERMS)
			old=[]
			for dx in self.stream_full_meta():
				title=dx.get('title')
				if not title: continue
				title=title.strip().lower()
				titlewords=set(re.findall(r"[\w']+|[.,!?;]", title))
				match = bool(self.SEARCH_TERMS & titlewords)
				if not match: continue
				old+=[dx]

			# fix!
			df=pd.DataFrame(old)
			df=df[df.lang.isin(self.LANGS)]
			df['id']=df['htid'].apply(lambda x: x.split('.',1)[0]+'/'+x.split('.',1)[1])
			df.to_csv(self.path_metadata)

		# get IDs
		print('>> loading metadata')
		df=pd.read_csv(self.path_metadata,error_bad_lines=False)
		# df['year']=df['imprint'].apply(get_date)
		# df['period']=df['year']//1*1
		# ids=list(set(df.groupby('period').sample(n=10,replace=True).id))
		# random.shuffle(ids)

		# compile!
		tools.pmap(compile_text, df.id, num_proc=6)


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
	# def __init__(self,id,name,search_terms,genre='',**y):
	# 	self.id=self.ID=id
	# 	self.name=self.NAME=name
	# 	self.SEARCH_TERMS=search_terms
	# 	self.GENRE=genre
	# 	super().__init__()

	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['genre']=self.GENRE
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

class HathiAlmanacs(HathiSubcorpus):
	ID='hathi_almanacs'
	NAME='HathiAlmanacs'
	SEARCH_TERMS={'almanac','almanack','almanach'}
	GENRE='Almanac'




def compile_text(idx,by_page=False):
	from htrc_features import Volume
	from urllib.error import HTTPError

	try:
		path_freqs = os.path.join(load_corpus('Hathi').path_freqs,idx+'.json')
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


