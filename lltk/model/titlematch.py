# # @TODO




# 	## MATCHING TITLES

# 	def combine_matches(self):
# 		for idx in self._metad:
# 			"""
# 			for match_meta in self.matchd.get(idx,[]):
# 				if not match_meta: continue
# 				for k,v in match_meta.items():
# 					if k in self._metad[idx]: k+='_'+match_meta['corpus']
# 					self._metad[idx][k]=v
# 			"""
# 			for other_text in self.matchd.get(self.name,{}).get(idx,[]):
# 				match_meta=other_text.meta
# 				if not match_meta: continue
# 				for k,v in list(match_meta.items()):
# 					#print [k,v,self._metad[idx].get(k,'')]

# 					if 'reprint' in k: # HACK HACK HACK HACK
# 						k+='_'+other_text.corpus.name
# 					elif k in self._metad[idx]:
# 						if self._metad[idx][k]==v: continue
# 						k+='_'+other_text.corpus.name

# 					self._metad[idx][k]=v
# 				#print

# 	@property
# 	def matchd(self):
# 		if not hasattr(self,'_matchd'):
# 			match_corp2id2ids={}
# 			match_id2ids={}
# 			match_fns=[]
# 			for fn in os.listdir(self.path_matches):
# 				if (fn.endswith('.xls') or fn.endswith('.xlsx') or fn.endswith('.txt') or fn.endswith('.csv')) and fn.startswith('matches.'):
# 					match_fns+=[fn]

# 			for match_fn in match_fns:
# 				c1name,c2name=match_fn.split('.')[1].split('--')
# 				#if not self.name in {c1name,c2name}: continue
# 				if c1name!=self.name: continue  # one-directional matching
# 				othername = c1name if c1name!=self.name else c2name
# 				idself = '1' if c1name==self.name else '2'
# 				idother = '2' if c1name==self.name else '1'
# 				#print self.name, c1name, c2name, othername

# 				othercorpus = name2corpus(othername)()
# 				othercorpus.load_metadata(maximal=True)
# 				self.matched_corpora[othercorpus.name]=othercorpus
# 				from lltk import tools
# 				match_ld=tools.read_ld(os.path.join(self.path_matches,match_fn))
# 				for matchd in match_ld:
# 					if matchd.get('match_ismatch','') and not matchd['match_ismatch'] in ['n','N']:
# 						idx_self=matchd['id'+idself]
# 						idx_other=matchd['id'+idother]

# 						if not self.name in match_corp2id2ids: match_corp2id2ids[self.name]={}
# 						if not othercorpus.name in match_corp2id2ids: match_corp2id2ids[othercorpus.name]={}

# 						if not idx_self in match_corp2id2ids[self.name]: match_corp2id2ids[self.name][idx_self]=[]
# 						if not idx_other in match_corp2id2ids[othercorpus.name]: match_corp2id2ids[othercorpus.name][idx_other]=[]

# 						#match_corp2id2ids[self.name][idx_self]+=[name2text(othername)(idx_other, othercorpus)]
# 						match_corp2id2ids[self.name][idx_self]+=[othercorpus.textd[idx_other]]
# 						match_corp2id2ids[othercorpus.name][idx_other]+=[self.textd[idx_self]]

# 						#if not idx_self in match_id2ids: match_id2ids[idx_self]=[]
# 						#match_id2ids[idx_self]+=[name2text(othername)(idx_other, othercorpus)]

# 						#match_id2ids[idx_self]+=[othercorpus.metad[idx_other]]

# 			self._matchd=match_corp2id2ids
# 		return self._matchd

# 	def match_records(self,corpus,match_by_id=False,match_by_title=True, title_match_cutoff=70, filter1={}, filter2={}, id_field_1='id',id_field_2='id',year_window=10):
# 		c1=self
# 		c2=corpus

# 		texts1=[t for t in c1.texts() if not False in [t.meta[k]==v for k,v in list(filter1.items())]]
# 		texts2=[t for t in c2.texts() if not False in [t.meta[k]==v for k,v in list(filter2.items())]]

# 		print('Matching from '+c1.name+' ('+str(len(texts1))+' texts)')
# 		print('to '+c2.name+' ('+str(len(texts2))+' texts)')
# 		print()

# 		matches={}

# 		old=[]

# 		if match_by_title:
# 			from fuzzywuzzy import fuzz
# 			for i,t1 in enumerate(tqdm(texts1)):
# 				if t1.id in matches: continue
# 				title1=''.join(x for x in t1.title if x.isalpha() or x==' ')
# 				author1=''.join(x for x in t1.author if x.isalpha() or x==' ')
# 				for t2 in texts2:
# 					if not t1.meta or not t2.meta: continue
# 					if not t1.title.strip() or not t2.title.strip(): continue
# 					if c1.name==c2.name and t1.id>=t2.id: continue
# 					if year_window is not None:
# 						try:
# 							if abs(float(t2.meta['year']) - float(t1.meta['year']))>year_window:
# 								continue
# 						except (ValueError,TypeError) as e:
# 							print("!!",e)
# 							continue
# 					title2=''.join(x for x in t2.title if x.isalpha() or x==' ')
# 					author2=''.join(x for x in t2.author if x.isalpha() or x==' ')
# 					mratio=fuzz.partial_ratio(title1.lower(), title2.lower())
# 					mratio_author=fuzz.partial_ratio(author1.lower(), author2.lower())
# 					if mratio>=title_match_cutoff:
# 						if not t1.id in matches: matches[t1.id]=[]
# 						matches[t1.id]+=[(t2.id,mratio,'')]
# 						dx={
# 							'id':t1.id,
# 							'id2':t2.id,
# 							'author':t1.author,
# 							'author2':t2.author,
# 							'title':t1.title,
# 							'title2':t2.title,
# 							'match_ratio':mratio,
# 							'match_ratio_author':mratio_author,
# 							'match_ismatch':''
# 						}
# 						old.append(dx)
		
# 		mdf=pd.DataFrame(old)
# 		ofn=os.path.join(self.path_data,'matches.'+c1.name+'--'+c2.name+'.csv')
# 		mdf.to_csv(ofn,index=False)


# 	def copy_match_files(self,corpus,match_fn=None,copy_xml=True,copy_txt=True):
# 		import shutil
# 		from lltk import tools
# 		match_fn = os.path.join(self.path,match_fn) if match_fn else os.path.join(self.path,'matches.'+corpus.name+'.xls')
# 		matches = [d for d in tools.read_ld(match_fn) if d['match_ismatch'].lower().strip()=='y']

# 		c1=self
# 		c2=corpus

# 		for d in matches:
# 			t1=c1.text(d['id1'])
# 			t2=c2.text(d['id2'])

# 			if copy_txt:
# 				if os.path.exists(t1.fnfn_txt): continue
# 				print(t2.fnfn_txt)
# 				print('-->')
# 				print(t1.fnfn_txt)
# 				print()
# 				shutil.copyfile(t2.fnfn_txt, t1.fnfn_txt)

# 			if copy_xml:
# 				if os.path.exists(t1.fnfn_xml): continue
# 				print(t2.fnfn_xml)
# 				print('-->')
# 				print(t1.fnfn_xml)
# 				print()
# 				shutil.copyfile(t2.fnfn_xml, t1.fnfn_xml)


# 	### DE-DUPING

# 	def rank_duplicates_bytitle(self,within_author=False,func='token_sort_ratio',min_ratio=90, allow_anonymous=True, func_anonymous='ratio', anonymous_must_be_equal=False,split_on_punc=[':',';','.','!','?'],modernize_spellings=False):
# 		from fuzzywuzzy import fuzz
# 		from lltk import tools
# 		func=getattr(fuzz,func)
# 		func_anonymous = func if not func_anonymous else getattr(fuzz,func_anonymous)
# 		if modernize_spellings:
# 			from lltk.tools import get_spelling_modernizer
# 			spelling_d=get_spelling_modernizer()

# 		## HACK
# 		#id1_done_before = set([d['id1'] for d in tools.readgen('data.duplicates.ESTC.bytitle.authorless-part1.txt')])
# 		#id1_done_before.remove(d['id1']) # remove last one
# 		#print len(id1_done_before)
# 		##

# 		def filter_title(title):
# 			for p in split_on_punc:
# 				title=title.split(p)[0]
# 			if modernize_spellings:
# 				return ' '.join([spelling_d.get(w,w) for w in title.split()])
# 			return title.lower()

# 		def writegen():
# 			texts = self.texts()
# 			for i1,t1 in enumerate(texts):
# 				print('>>',i1,len(texts),'...')
# 				for i2,t2 in enumerate(texts):
# 					if i1>=i2: continue
# 					title1=t1.title
# 					title2=t2.title
# 					dx={}
# 					dx['id1'],dx['id2']=t1.id,t2.id
# 					#dx['title1'],dx['title2']=title1,title2
# 					#dx['author1'],dx['author2']=t1.author,t2.author
# 					ratio=func(title1,title2)
# 					if min_ratio>ratio: continue
# 					#dx['match_ratio']=ratio
# 					yield dx

# 		def writegen_withinauthor():
# 			from collections import defaultdict
# 			author2texts=defaultdict(list)
# 			for t in self.texts(): author2texts[t.author]+=[t]
# 			NumTexts=len(self.texts())
# 			numauthors=len(author2texts)
# 			numtextsdone=0
# 			done=set()

# 			## HACK
# 			#done|=done_before
# 			##

# 			for ai1,author in enumerate(sorted(author2texts, key=lambda _k: len(author2texts[_k]), reverse=True)):
# 				texts=author2texts[author]
# 				#comparisons = set([(t1.id,t2.id) for t1 in texts for t2 in texts if t1.id<t2.id])
# 				#print len(comparisons)
# 				if not author and not allow_anonymous: continue

# 				title2texts=defaultdict(list)
# 				for t in texts: title2texts[filter_title(t.title)]+=[t]
# 				texts_unique_title=[x[0] for x in list(title2texts.values())]
# 				for title in title2texts:
# 					for i1,t1 in enumerate(title2texts[title]):
# 						print('>> a) finished {0} of {1} texts. currently on author #{2} of {3}, who has {4} texts. ...'.format(numtextsdone,NumTexts,ai1+1,numauthors,len(texts)))
# 						for i2,t2 in enumerate(title2texts[title]):
# 							if t1.id>=t2.id: continue
# 							dx={}
# 							dx['id1'],dx['id2']=t1.id,t2.id
# 							#dx['title1'],dx['title2']=t1.title,t2.title
# 							#dx['author1'],dx['author2']=t1.author,t2.author
# 							#dx['match_ratio']=100
# 							#yield dx
# 							#done|={tuple(sorted([t1.id,t2.id]))}
# 							done|={(t1.id,t2.id)}
# 						numtextsdone+=1

# 				if not author and anonymous_must_be_equal: continue

# 				for i1,t1 in enumerate(texts_unique_title):
# 					print('>> b) finished {0} of {1} texts. currently on author #{2} of {3} [{5}], who has {4} texts. ...'.format(numtextsdone,NumTexts,ai1+1,numauthors,len(texts),author.encode('utf-8',errors='ignore')))

# 					### HACK -->
# 					if numtextsdone<250000:
# 						numtextsdone+=1
# 						continue
# 					#if t1.id in id1_done_before: continue
# 					###


# 					title1=filter_title(t1.title)
# 					nuncmp=0
# 					for i2,t2 in enumerate(texts_unique_title):
# 						if t1.id>=t2.id: continue
# 						#print (t1.id,t2.id), (t1.id,t2.id) in done, (t2.id,t1.id) in done
# 						if (t1.id,t2.id) in done:
# 							print('>> skipping')
# 							continue
# 						nuncmp+=1
# 						#print i1,i2,nuncmp,'..'
# 						title2=filter_title(t2.title)
# 						ratio=func(title1,title2) if author else func_anonymous(title1,title2)

# 						if min_ratio>ratio: continue

# 						for t1_x in title2texts[title1]:
# 							for t2_x in title2texts[title2]:
# 								if t1_x.id>=t2_x.id: continue
# 								if (t1_x.id,t2_x.id) in done:
# 									print('>> skipping')
# 									continue
# 								dx={}
# 								dx['id1'],dx['id2']=t1_x.id,t2_x.id
# 								dx['title1'],dx['title2']=t1_x.title,t2_x.title
# 								dx['author1'],dx['author2']=t1_x.author,t2_x.author
# 								dx['match_ratio']=ratio
# 								yield dx
# 					numtextsdone+=1
# 				break

# 		from lltk import tools
# 		if not within_author:
# 			tools.writegen('data.duplicates.%s.bytitle.txt' % self.name, writegen)
# 		else:
# 			tools.writegen('data.duplicates.%s.bytitle.txt' % self.name, writegen_withinauthor)



# 	def rank_duplicates(self,threshold_range=None,path_hashes=None, suffix_hashes=None,sample=False,hashd=None,ofn=None):
# 		if not threshold_range:
# 			import numpy as np
# 			threshold_range=np.arange(0.25,1,0.05)

# 		import networkx as nx,random
# 		G=nx.Graph()

# 		threshold_range=[float(x) for x in threshold_range]
# 		print('>> getting duplicates for thresholds:',threshold_range)

# 		for threshold in reversed(threshold_range):
# 			threshold_count=0
# 			print('>> computing LSH at threshold =',threshold,'...')
# 			lsh,hashd=self.lsh(threshold=threshold,path_hashes=path_hashes,suffix_hashes=suffix_hashes,hashd=hashd)
# 			hash_keys = list(hashd.keys())
# 			random.shuffle(hash_keys)
# 			for idx1 in hash_keys:
# 				if sample and threshold_count>=sample:
# 					break
# 				r=lsh.query(hashd[idx1])
# 				r.remove(idx1)
# 				for idx2 in r:
# 					if not G.has_edge(idx1,idx2):
# 						G.add_edge(idx1,idx2,weight=float(threshold))
# 						threshold_count+=1
# 			print(G.order(), G.size())
# 			print()

# 		def writegen1():
# 			for a,b,d in sorted(G.edges(data=True),key=lambda tupl: -tupl[-1]['weight']):
# 				meta1=self.textd[a].meta
# 				meta2=self.textd[b].meta
# 				weight=d['weight']
# 				dx={'0_jacc_estimate':weight}
# 				dx['1_is_match']=''
# 				for k,v in list(meta1.items()): dx[k+'_1']=v
# 				for k,v in list(meta2.items()): dx[k+'_2']=v
# 				yield dx

# 		def writegen2():
# 			g=G
# 			gg = sorted(nx.connected_components(g), key = len, reverse=True)
# 			for i,x in enumerate(gg):
# 				print('>> cluster #',i,'has',len(x),'texts...')
# 				#if len(x)>50000: continue
# 				xset=x
# 				x=list(x)
# 				idx2maxjacc = {}
# 				for idx in x:
# 					neighbors = g.neighbors(idx)
# 					weights = [g.edge[idx][idx2]['weight'] for idx2 in neighbors]
# 					idx2maxjacc[idx]=max(weights)

# 				x.sort(key=lambda idx: (-idx2maxjacc[idx],self.metad[idx]['year']))
# 				for idx in x:
# 					dx={'0_cluster_id':i}
# 					dx['1_highest_jacc']=idx2maxjacc[idx]
# 					dx['2_is_correct']=''
# 					metad=self.textd[idx].meta
# 					dx['3_year']=metad.get('year','')
# 					dx['4_title']=metad.get('fullTitle','')
# 					dx['5_author']=metad.get('author','')
# 					dx['6_id']=metad.get('id','')
# 					for k,v in list(metad.items()): dx[k]=v
# 					yield dx

# 		ofn='data.duplicates.%s.txt' % self.name if not ofn else ofn
# 		from lltk import tools
# 		tools.writegen(ofn, writegen1)
# 		return G

# 	def hashd(self,path_hashes=None,suffix_hashes=None,text_ids=None):
# 		from datasketch import MinHash
# 		import six.moves.cPickle

# 		if not suffix_hashes: suffix_hashes='.hash.pickle'
# 		if not path_hashes: path_hashes=self.path_txt.replace('_txt_','_hash_')

# 		#print '>> loading hashes from files...'
# 		text_ids=self.text_ids if not text_ids else text_ids

# 		hashd={}
# 		for i,idx in enumerate(text_ids):
# 			#if i and not i%1000:
# 				#print '>>',i,'..'
# 				#break
# 			fnfn=os.path.join(path_hashes, idx+suffix_hashes)
# 			try:
# 				hashval=six.moves.cPickle.load(open(fnfn))
# 			except IOError as e:
# 				hashobj=self.textd[idx].minhash
# 				hashval=hashobj.digest()
# 			mh=MinHash(seed=1111, hashvalues=hashval)
# 			hashd[idx]=mh

# 		return hashd

# 	def lsh(self,threshold=0.5,path_hashes=None,suffix_hashes=None,hashd=None):
# 		from datasketch import MinHashLSH

# 		hashd=self.hashd(path_hashes=path_hashes,suffix_hashes=suffix_hashes) if not hashd else hashd
# 		lsh = MinHashLSH(threshold=threshold, num_perm=128)
# 		for idx in hashd: lsh.insert(idx,hashd[idx])
# 		return lsh,hashd


