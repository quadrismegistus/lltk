from lltk.imports import *

class SOTU(BaseCorpus):
	def load_metadata(self, *x, **y):
		df = super().load_metadata(*x, **y)
		df['genre_raw'] = 'Political speech'
		df['genre'] = 'Speech'
		return df

	def compile(self):
		ld=[]
		for fn in os.listdir(self.path_txt):
			idx=os.path.splitext(fn.replace('.gz',''))[0]
			year=idx.split("_")[1]
			author=idx.split('_')[0]
			dx={
				'id':idx,
				'author':author,
				'title':f'State of the Union Address ({year})',
				'year':year
			}
			ld.append(dx)
		df=pd.DataFrame(ld)
		save_df(df,self.path_metadata)
