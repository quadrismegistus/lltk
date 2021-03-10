import os,json,bs4
import numpy as np

import lltk
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
from zipfile import ZipFile
from tqdm import tqdm
import shutil


class TextJstorDFR(Text):
	pass



class JstorDFR(Corpus):
	TEXT_CLASS=TextJstorDFR

	@property
	def path_xml_meta(self): return os.path.join(self.path_root,'xml_meta')
	@property
	def path_dataset(self): return os.path.join(self.path_raw,'dataset.zip')


	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		if not os.path.exists(self.path_dataset):
			dl_url=input('Please paste download URL for DFR dataset:\n').strip()
			if not dl_url.startswith('http'): return
			if not os.path.exists(self.path_raw): os.makedirs(self.path_raw)
			lltk.download_wget(dl_url,self.path_dataset)

		
		# Open your .zip file
		with ZipFile(self.path_dataset) as zip_file:
			namelist=zip_file.namelist()
			# Loop over each file
			for member in tqdm(iterable=namelist, total=len(namelist)):
				filename = os.path.basename(member)
				ext=os.path.splitext(filename)[1]
				idx=os.path.splitext(filename)[0].split('-ngram')[0].replace('-','/')

				if not ext in {'.xml','.txt'}: continue
				
				# open
				# just copy
				if ext=='.xml' and member.startswith('metadata'):
					with zip_file.open(member) as source:
						opath=os.path.join(self.path_xml_meta,idx+'.xml')
						if os.path.exists(opath): continue
						opath_dir=os.path.dirname(opath)
						if not os.path.exists(opath_dir):os.makedirs(opath_dir)
						with open(opath, "wb") as target:
							with source, target:
								shutil.copyfileobj(source, target)
				# if freqs, convert to json
				elif ext=='.txt' and member.startswith('ngram'):
					opath=os.path.join(self.path_freqs,idx+'.json')
					if os.path.exists(opath): continue

					sourcetxt=zip_file.read(member).decode('utf-8')
					dx={}
					for ln in sourcetxt.split('\n'):
						try:
							word,count = ln.strip().split('\t')
							count=count.strip()
							dx[word]=int(count) if not word in dx else int(dx[word])+int(count)
						except (TypeError,ValueError) as e:
							continue
					opath_dir=os.path.dirname(opath)
					if not os.path.exists(opath_dir): os.makedirs(opath_dir)
					with open(opath,'w') as of:
							json.dump(dx,of)

	def load_metadata(self,*x,**y):
		"""
		Magic attribute loading metadata, and doing any last minute customizing
		"""
		meta=super().load_metadata()
		# ?
		return meta

	def preprocess_metadata(self,**attrs):
		import pandas as pd
		objs=[]
		for root,dirs,fns in os.walk(self.path_xml_meta):
			for fn in fns:
				if not fn.endswith('.xml'): continue
				objs.append(os.path.join(self.path_xml_meta,root,fn))
		meta=lltk.pmap(parse_meta_from_xml,objs)
		df=pd.DataFrame(meta)
		df.to_csv(self.path_metadata,index=False)
			

def parse_meta_from_xml(xmlfnfn):
	with open(xmlfnfn) as f: xml=f.read()
	dom=bs4.BeautifulSoup(xml,'lxml')
	idx=os.path.splitext(xmlfnfn.split('/xml_meta/')[-1])[0]
	meta={'id':idx,'rootcat':idx.split('/')[0]}
	for any_tag in dom.find_all():
		children=list(any_tag.children)
		if children and len(children)==1 and type(children[0])==bs4.element.NavigableString:
			tagtxt=any_tag.text.strip()
			meta[any_tag.name]=tagtxt
	return meta


# 		x=[list(tag.children)[0] for tag in dom.find_all() if type(list(tag.children)[0])==bs4.element.NavigableString]
# >>> x[0]
# '1'
# >>> type(x)
# <class 'list'>
# >>> x.parent
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
# AttributeError: 'list' object has no attribute 'parent'
# >>> type(x[0])
# <class 'bs4.element.NavigableString'>
# >>> type(x[0])
# KeyboardInterrupt
# >>> x[0].parent
# <goodbye>1</goodbye>
# >>> x[0].parent.name


class PMLA(JstorDFR):
	def load_metadata(self,*x,**y):
		meta=super().load_metadata()
		meta['title']=meta['article-title']
		meta['author']=meta['surname']
		meta['genre']='Journal'
		# meta=meta[meta['journal-title']=='PMLA']
		# meta['year']=[int(x) if x.isdigit() else np.nan for x in meta['year']]
		meta=meta[meta['rootcat']=='journal'].query('1881<=year<=2020') # length of PMLA (so far)
		return meta