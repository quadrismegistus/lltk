import os
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
from lltk import tools

########################################################################################################################
# [DTA]
# (1) Text Class
########################################################################################################################

class TextDTA(Text):
	# (b) Getting plain text from XML files
	def text_plain_from_xml(self):
		import bs4
		with open(self.path_xml) as f:
			txt=f.read()
			dom=bs4.BeautifulSoup(txt,'lxml')
			text = dom('text')[0].text
			text = text.replace(u'ſ',u's').replace(u'oͤ',u'ö').replace(u'aͤ',u'ä').replace(u'uͤ',u'ü')
			return text

	# (b) Get metadata from XML or other file
	@property
	def path_xml_meta(self):
		return os.path.join(self.corpus.path_xml_meta, self.id + '.xml')

	def get_meta_from_file(self):
		# read xml from meta
		if not os.path.exists(self.path_xml_meta): return {}
		with open(self.path_xml_meta) as f: xml=f.read()

		# parse
		import bs4
		dom=bs4.BeautifulSoup(xml,'lxml')

		# get all dc tags
		meta={}
		for tag in dom():
			if tag.name.startswith('dc:'):
				meta[tag.name[3:]]=tag.text
		if 'rights' in meta: del meta['rights']
		return meta

	def get_metadata(self):
		meta=super().get_metadata()
		genre=str(meta.get('subject',''))
		genrel=genre.lower()
		if genrel.endswith('roman') or 'novelle' in genrel or 'märchen' in genrel:
			meta['genre']='Fiction'
			meta['medium']='Prose'   # check this assumption
		elif genrel.endswith('drama'):
			meta['genre']='Drama'
			meta['medium']='Unknown'
		elif 'lyrik' in genrel:
			meta['genre']='Poetry'
			meta['medium']='Verse'
		else:
			meta['genre']='Other'
			meta['medium']='Unknown'

		try:
			meta['year']=int(meta['date'])
		except ValueError:
			import numpy as np
			meta['year']=np.nan

		meta['author']=meta['creator']
		return meta



########################################################################################################################
# (2) Corpus Class
########################################################################################################################

class DTA(Corpus):
	TEXT_CLASS=TextDTA


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################

	def compile(self,**attrs):
		self.compile_xml()
		self.compile_metadata()

	def compile_xml(self):
		# download if nec
		downloaded_to = self.download_raw_xml()
		# unzip xml
		tools.unzip(downloaded_to, dest=self.path_xml, flatten=True, overwrite=False, replace_in_filenames={'.TEI-P5.':'.'})

	def compile_metadata(self):
		# download if nec
		downloaded_to = self.download_raw_metadata()
		# unzip
		tools.unzip(downloaded_to, dest=self.path_xml_meta, flatten=True, overwrite=False, replace_in_filenames={'.oai_dc.':'.'})


	def download_raw_xml(self):
		# download file
		from lltk import tools
		ofnfn=os.path.join(self.path_raw, f'_tmp_{self.id}_raw.zip')
		if not os.path.exists(ofnfn):
			print('>> downloading:',self.url_raw,'to',ofnfn)
			tools.download(self.url_raw, ofnfn)
		return ofnfn

	def download_raw_metadata(self):
		ofnfn=os.path.join(self.path_raw, f'_tmp_{self.id}_raw_metadata.zip')
		if not os.path.exists(ofnfn):
			print('>> downloading:',self.url_raw_metadata,'to',ofnfn)
			tools.download(self.url_raw_metadata, ofnfn)
		return ofnfn


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



"""
Info:

In [4]: C.df.subject.value_counts()
Out[4]:
Zeitung                                               767
Gebrauchsliteratur: Leichenpredigt                    334
Belletristik: Roman                                   187
Belletristik: Novelle                                 121
Belletristik: Lyrik                                   116
Gebrauchsliteratur: Gelegenheitsschrift:Tod           109
Gebrauchsliteratur: Gesellschaft                       92
Belletristik: Drama                                    82
Wissenschaft: Philosophie                              70
Belletristik: Prosa                                    63
Wissenschaft: Geographie                               50
Wissenschaft: Jura                                     50
Wissenschaft: Historiographie                          45
Wissenschaft: Biologie                                 45
Wissenschaft: Medizin                                  44
Gebrauchsliteratur: Theologie                          40
Wissenschaft: Psychologie                              38
Wissenschaft: Kunstgeschichte                          33
Wissenschaft: Physik                                   29
Gebrauchsliteratur: Erbauungsliteratur                 28
Wissenschaft: Literaturwissenschaft                    28
Wissenschaft: Ökonomie                                 26
Belletristik: Briefe                                   24
Wissenschaft: Technik                                  24
Wissenschaft: Sprachwissenschaft                       24
Wissenschaft: Mathematik                               23
Wissenschaft: Naturgeschichte                          21
Belletristik: Reiseliteratur                           21
Wissenschaft: Philologie                               21
Gebrauchsliteratur: Brief                              21
                                                     ...
Gebrauchsliteratur: Rezension                           1
Wissenschaft: Politik, Ökonomie                         1
Gebrauchsliteratur: Schulbuch                           1
Belletristik: Briefroman                                1
Gebrauchsliteratur: Astrologie                          1
Belletristik: Lied                                      1
Gebrauchsliteratur: Kriminalistik                       1
Gebrauchsliteratur: Kolportageliteratur                 1
Gebrauchsliteratur: Kunst                               1
Belletristik: Dialog                                    1
Wissenschaft: Politik, Anstandsliteratur                1
Gebrauchsliteratur: Pflanzenbuch                        1
Gebrauchsliteratur: Gelegenheitsschrift:Fest            1
Gebrauchsliteratur: Naturwissenschaft                   1
Gebrauchsliteratur: Gelegenheitsschrift:Tod; Lyrik      1
Wissenschaft: Alchemie, Medizin                         1
Wissenschaft: Kameralwissenschaft                       1
Wissenschaft: Glasherstellung                           1
Wissenschaft: Sonstiges                                 1
Belletristik: Städtelob                                 1
Wissenschaft: Archäologie                               1
Belletristik: Schäferdichtung                           1
Wissenschaft: Brief                                     1
Wissenschaft: Ordensliteratur:Jesuiten                  1
Belletristik: Religiöse Reimpaarerzählung               1
Belletristik: Lyrik, Epigramm                           1
Belletristik: Epik                                      1
Belletristik: Lyrik; Drama; Prosa                       1
Gebrauchsliteratur: Biologie                            1
Gebrauchsliteratur: Bibelübersetzung                    1
Name: subject, Length: 142, dtype: int64





"""
