from lltk.imports import *

class TextTxtLab(Text): pass

class TxtLab(Corpus):
	TEXT_CLASS=TextTxtLab


	####################################################################################################################
	# (2.1) Installation methods
	####################################################################################################################

	def compile(self,zipdir_name='txtlab450-master',**attrs):
		"""
		This is a custom installation function. By default, it will simply try to download itself,
		unless a custom function is written here which either installs or provides installation instructions.
		"""
		# download file
		self.compile_download(unzip=False)
		# extract
		self.compile_extract()
		# clean
		self.compile_metadata()
		
		

	# Extract once downloaded
	def compile_extract(self):
		# custom unzip: remove internal directories to unzip directly to txt folder
		zipfn=self.path_zip('raw')
		if not os.path.exists(zipfn): return

		with ZipFile(zipfn) as zip_file:
			namelist=zip_file.namelist()
			# Loop over each file
			for member in tqdm(iterable=namelist, total=len(namelist), desc=f'[{self.name}] Extracting texts from {os.path.basename(zipfn)}'):
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
				odir=os.path.dirname(opath)
				if not os.path.exists(odir): os.makedirs(odir)
				with open(opath, "wb") as target:
					with source, target:
						shutil.copyfileobj(source, target)

				# if not filename.endswith('.txt'): print('>> saved:',opath)
				if not filename.endswith('.txt'):
					self.log(f'Saved: {os.path.basename(opath)}')
		#if not os.path.exists(self.path_root): shutil.move('README.md',self.path_root)
		#if not os.path.exists(self.path_metadata): os.rename('metadata.csv',self.path_metadata)

		



	def compile_metadata(self):
		df=pd.read_csv(self.path_metadata)
		if not 'id_orig' in df.columns:
			df['id_orig']=[idx for idx in df['id']]
		df['id']=[os.path.splitext(fn)[0] for fn in df['filename']]
		df['year']=pd.to_numeric(df['date'],errors='coerce')
		df=fix_meta(df)
		header = ['id','id_orig'] + [col for col in df.columns if col not in {'id','id_orig'}]
		save_df(df[header],self.path_metadata,index=False)


	def load_metadata(self):
		meta=super().load_metadata()
		if 'date' in meta.columns and not 'year' in meta.columns:
			meta['year']=meta['date']
			meta=clean_meta(meta)
		return meta



########################################################################################################################
















########################################################################################################################
