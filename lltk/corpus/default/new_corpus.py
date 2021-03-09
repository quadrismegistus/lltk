from lltk.imports import *


class TextNewCorpus(Text):
	def xml2txt(self,xml_fn):
		"""
		Overwrite this if you have a custom function for parsing XML files to TXT files
		"""
		# This is the default function 
		txt = super().xml2txt(xml_fn)
		return txt


class NewCorpus(Corpus):
	TEXT_CLASS=TextNewCorpus
	COL_ID = 'id'

	def compile(self):
		"""
		This is a custom installation function, downloading raw data
		and processing it into a form which minimally contains
		a metadata file stored at `metadata.csv` which has a column `self.COL_ID`
		"""
		# This will download any file attached to `url_raw` on the corpus manifest
		# and unzip it to `path_raw` (usually ~/lltk_data/corpora/[corpusroot]/raw)
		# self.compile_download(unzip=True)

		# Other code here to somehow cobble together from the raw data in `path_raw`
		# a metadata file at `metadata.csv`
		# ...

		return

	def preprocess_txt(self,*args,**attrs):
		"""
		Custom function here to produce txt files at `path_txt`
		from xml files at `path_xml`. By default this will map
		TextNewCorpus.xml2txt(xml_fn) over files at `path_xml`.
		"""
		# By default will
		return super().preprocess_txt(*args,**attrs)

	def load_metadata(self,*args,**attrs):
		"""
		Magic attribute loading metadata, and doing any last minute customizing
		"""
		# This will save to `self.path_metadata`, set in the manifest path_metadata
		meta=super().load_metadata(*args,**attrs)
		# ?
		return meta

