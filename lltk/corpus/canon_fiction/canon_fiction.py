from lltk.imports import *

class TextCanonFiction(Text): pass
class CanonFiction(Corpus):
	TEXT_CLASS=TextCanonFiction
	CORPORA_TO_START_WITH = ['Chadwyck','MarkMark']
	def compile(self,**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""

		metadata = []

		import lltk
		text_num=0
		for c in self.CORPORA_TO_START_WITH:
			C=lltk.load(c)
			for text in C.texts():
				text_num+=1
				author=text.author.split(',')[0].strip().replace('.','')
				title=''.join([x for x in text.title if x.isalpha() or x==' '])
				while '  ' in title: title=title.replace('  ',' ')
				title=title[:25].strip().replace(' ','_')
				year=str(text.year).replace('.','')
				tnum=str(text_num).zfill(4)
				cnamel=C.id
				idx=f'{cnamel}.{author}.{title}.{tnum}'

				meta_d=text.meta
				meta_d['corpus_source']=c
				meta_d['id']=idx
				meta_d['id_orig']=text.id
				del meta_d['corpus']
				del meta_d['_lltk_']
				metadata += [meta_d]


				ofn=os.path.join(self.path_txt, idx+'.txt')
				if os.path.exists(ofn): continue
				with open(ofn,'w') as of:
					of.write(text.txt)
					print('>> saved:',ofn)
		# extend by
		metadata += [d for d in self.meta if not d['corpus_source']]
		import pandas as pd
		df=pd.DataFrame(metadata)
		meta_fn = os.path.splitext(self.path_metadata)[0]+'_init.csv'
		header_custom = ['id','author','title','year','canon_genre','canon_name']
		header = header_custom + list([x for x in df.columns if not x in set(header_custom)])
		if '_lltk_' in header: header.remove('_lltk_')
		if 'corpus' in header: header.remove('corpus')
		df[header].to_csv(meta_fn,index=False)

	def load_metadata(self):
		meta=super().load_metadata()
		meta['genre']='Fiction'
		return meta