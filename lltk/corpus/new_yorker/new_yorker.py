import os
from lltk.text.text import Text
from lltk.corpus.corpus import Corpus
import tarfile
import bs4
import lltk
import pandas as pd
########################################################################################################################
# [NewYorker]
# (1) Text Class
########################################################################################################################

class TextNewYorker(Text):
	pass


def xml2str(xmlstr,min_words=3,must_start_alpha=True):
	dom=bs4.BeautifulSoup(xmlstr,'lxml')
	
	lines=[]
	for para in dom('paragraph'):
		for line in para('line'):
			words=[w.text for w in line('word') if w.text and w.text[0].isalnum()]
			if min_words and len(words)<min_words: continue

			linestr=' '.join(words).strip().replace('\n',' ')
			if not linestr: continue
			if must_start_alpha==True and not linestr[0].isalpha(): continue

			if 'NEW YORKER' in linestr: continue

			yield linestr#lines.append(linestr)
		yield ''#lines.append('')
		# lines.append('')
	# return lines #'\n'.join(lines)

def compile_tar(obj):
	tarfnfn,path_txt=obj
	date=os.path.splitext(os.path.basename(tarfnfn))[0]
	meta = {'date':date, 'year':date[:4], 'id':date}
	opath_txt=os.path.join(path_txt,meta['id']+'.txt')
	try:
		if not os.path.exists(opath_txt):
			with tarfile.open(tarfnfn) as archive:
				with open(opath_txt,'w') as of:
					for member in sorted(archive,key=lambda member: member.name):
						if 'xml/page' in member.name and member.name.endswith('.xml'):
							with archive.extractfile(member) as f:
								content=f.read()
								for line in xml2str(content):
									of.write(line+'\n')
	except tarfile.ReadError:
		pass
	return meta

class NewYorker(Corpus):
	TEXT_CLASS=TextNewYorker


	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		# should be tar files in raw
		objs=[(os.path.join(self.path_raw,fn),self.path_txt) for fn in sorted(os.listdir(self.path_raw)) if fn[:4].isdigit()]
		print('>> saving plain texts:',self.path_metadata)
		meta_ld=lltk.pmap(compile_tar, objs)
		df=pd.DataFrame(meta_ld)
		df.to_csv(self.path_metadata,index=False)
		print('>> saved:',self.path_metadata)

	def load_metadata(self,*x,**y):
		"""
		Magic attribute loading metadata, and doing any last minute customizing
		"""
		meta=super().load_metadata()
		meta['genre']='Periodical'
		meta['title']=meta['date']
		meta['author']='The New Yorker'
		return meta





########################################################################################################################
