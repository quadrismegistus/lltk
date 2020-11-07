from __future__ import absolute_import
from __future__ import print_function
import os,codecs,gzip,random,time
from pprint import pprint
from lltk.tools import tools
import six
from six.moves import range
from six.moves import zip
from collections import defaultdict
from tqdm import tqdm
from lltk.text import Text
from argparse import Namespace
from pprint import pprint
import inspect
from tqdm import tqdm
import random
import json
import pandas as pd

from os.path import expanduser
HOME=expanduser("~")

config = tools.config

ZIP_PART_DEFAULTS={'txt','freqs','metadata','xml','data'}
INSTALL_CMDS=['metadata','txt','freqs','mfw','dtm']
DEST_LLTK_CORPORA=config.get('CLOUD_DEST','/Share/lltk_corpora')

MANIFEST_REQUIRED_DATA=['name','id']

MANIFEST_DEFAULTS=dict(
	path_txt='txt',
	path_xml='xml',
	path_index='',
	ext_xml='.xml',
	ext_txt='.txt',

	path_model='',
	path_header=None,
	#path_metadata='metadata.txt',
	path_metadata='metadata.csv',
	paths_text_data=[],
	paths_rel_data=[],
	class_name='',

	path_freq_table={},
	col_id='id',
	col_fn='fn',
	path_root='',
	path_raw='raw',
	path_spacy='spacy',
	#path_freqs=os.path.join('freqs',name),
	path_freqs='freqs',
	manifest={},
	path_python='',
	manifest_override=True,
	path_data='data',
	is_meta = '')

PATH_HERE=os.path.abspath(os.path.dirname(__file__))
PATH_LLTK_HOME = os.path.join(HOME,'lltk_data')
PATH_CORPUS = config.get('PATH_TO_CORPORA', os.path.join(PATH_LLTK_HOME,'corpora') )
PATH_CORPUS_ZIP = os.path.join(PATH_CORPUS, 'lltk_corpora')
PATH_TO_CORPUS_CODE = config.get('PATH_TO_CORPUS_CODE', PATH_HERE )

PATH_MANIFEST=os.path.join(PATH_TO_CORPUS_CODE,'manifest.txt')
PATH_MANIFEST_USER = config.get('PATH_TO_MANIFEST','')
PATH_MANIFEST_USER_LAB = PATH_MANIFEST_USER.replace('.txt','_lab.txt')
PATH_MANIFEST_USER_SHARE = PATH_MANIFEST_USER.replace('.txt','_share.txt')

PATH_MANIFESTS = tools.remove_duplicates([
	PATH_MANIFEST,
	os.path.join(PATH_TO_CORPUS_CODE,'manifest_local.txt'),
	os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..','lltk_manifest.txt')),
	os.path.abspath(os.path.join(PATH_CORPUS,'manifest.txt')),
	os.path.abspath(os.path.join(PATH_CORPUS,'manifest_local.txt')),
	os.path.abspath(os.path.join(PATH_HERE,'..','..','config','lltk_manifest.txt')),
	os.path.join(PATH_TO_CORPUS_CODE,'manifest_lab.txt'),
	os.path.join(PATH_LLTK_HOME,'manifest_share.txt'),
	os.path.join(PATH_LLTK_HOME,'manifest_lab.txt'),
	os.path.join(PATH_LLTK_HOME,'manifest.txt'),
	os.path.join(HOME,'lltk_manifest.txt'),
	PATH_MANIFEST_USER,
	PATH_MANIFEST_USER_LAB,
	PATH_MANIFEST_USER_SHARE
], remove_empty=True)
#print(PATH_MANIFESTS)

from pprint import pprint

nlp=None
ENGLISH=None
stopwords=set()
MANIFEST={}


#### LOAD CORPUS FROM MANIFEST

def load_manifest(force=True,corpus_name=None):
	if MANIFEST and not force: return MANIFEST

	# read config
	#print('>> reading config files...')
	import configparser
	config = configparser.ConfigParser()
	config_d={}
	for path in PATH_MANIFESTS:
		if not os.path.exists(path): continue
		config.read(path)

	# convert config
	for corpus in list(config.keys()):
		if not corpus_name or corpus==corpus_name:
			if corpus=='DEFAULT': continue
			cd={}
			for k,v in MANIFEST_DEFAULTS.items(): cd[k]=v
			for k,v in list(config[corpus].items()): cd[k]=v

			## LAST MINUTE DEFAULTS!?
			try:
				if not cd.get('path_python'): cd['path_python']=cd['id']+'.py'
			except KeyError:
				continue

			MANIFEST[corpus]=cd

	return MANIFEST if not corpus_name else MANIFEST.get(corpus_name,{})




####


def download(name,**attrs):
	print(name,'?')
	corpus=load_corpus(name)
	corpus.download()


def corpora(load=True,incl_meta_corpora=True):
	manifest=load_manifest()
	for corpus_name in sorted(manifest):
		if not incl_meta_corpora and manifest[corpus_name]['is_meta']: continue
		corpus_obj=load_corpus(corpus_name, manifest=manifest) if load else manifest[corpus_name]
		#print(corpus_name, corpus_obj)
		if corpus_obj is None: continue
		yield (corpus_name, corpus_obj)

def check_corpora(paths=['path_raw','path_xml','path_txt','path_freqs','path_metadata'],incl_meta_corpora=False):
	old=[]
	#clist=tools.cloud_list()
	print('{:25s} {:32s} {:12s} {:12s} {:12s} {:12s} {:12s}'.format('[CORPUS]','[DESCRIPTION]',' [RAW?]',' [XML?]',' [TXT?]',' [FREQS?]',' [METADATA?]'))
	for cname,corpus in corpora(load=True,incl_meta_corpora=incl_meta_corpora):
		if corpus is None: continue
		print('{:25s} {:30s}'.format(cname, corpus.desc[:25]),end=" ")
		for path in paths:
			pathtype=path.replace('path_','')
			pathval = getattr(corpus,path)
			#pathval = corpus.get(path,'')
			exists = '↓' if os.path.exists(pathval) and (not os.path.isdir(pathval) or bool(os.listdir(pathval))) else ' '

			#exists_cloud = '↑' if f'{corpus.id}_{pathtype}.zip' in clist else ' '
			exists_link = '↑' if hasattr(corpus,f'url_{pathtype}') else ' '
			zip_fn=f'{corpus.id}_{pathtype}.zip'
			#exists_zip = '←' if os.path.exists(os.path.join(PATH_CORPUS_ZIP,zip_fn)) else ' '
			cell=' '.join([x for x in [exists,exists_link,pathtype] if x])
			print('{:12s}'.format(cell),end=' ')

		print()
			#odx={'name':cname,'id':corpus.id,'path_type':path, 'path_value':pathval, 'exists':exists}
			#old+=[odx]
	#import pandas as pd
	#df=pd.DataFrame(old)
	#print(df)
	#return df



def string_import(name):
	m = __import__(name)
	for n in name.split(".")[1:]:
		m = getattr(m, n)
	return m

def name2corpus(name):
	modulename = name.lower().split('-')[0]
	classname = name.replace('-','_')
	module = string_import('lltk.corpus.'+modulename)
	classx = getattr(module,classname)
	return classx

def name2text(name):
	modulename = name.lower().split('-')[0]
	classname = 'Text'+name.replace('-','_')
	module = string_import('lltk.text.'+modulename)
	classx = getattr(module,classname)
	return classx



class Corpus(object):
	EXT_TXT='.txt'
	EXT_XML='.xml'
	TYPE='Corpus'
	TEXT_CLASS=Text

	def __init__(self,name=None,id=None,sources=[PATH_CORPUS,''],**input_kwargs):
		#print('>> building corpus:',name)
		from lltk.tools import get_path_abs,camel2snake_case

		# Set defaults
		default_kwargs=MANIFEST_DEFAULTS
		if not name: name = input_kwargs.get('name')
		if not name: name = self.__class__.__name__
		if not id: id = input_kwargs.get('id')
		if not id: id = camel2snake_case(name)

		### Configs
		manifest_kwargs=load_manifest().get(name,{})
		self.opts = opts = dict(list(default_kwargs.items()) + list(manifest_kwargs.items()) + list(input_kwargs.items()))
		self.name=name
		self.id = id

		path_root = get_path_abs(self.opts.get('path_root',''),sources=sources)
		if not path_root: path_root=os.path.join(PATH_CORPUS,self.id)
		self.path_root=path_root

		# Set as attributes
		for opt_name,opt_val in sorted(self.opts.items()):
			# Set abs path
			if opt_name!='path_root' and opt_name.startswith('path_') and opt_val and type(opt_val)==str:
				#opt_val = os.path.join(path_root,opt_val) if not os.path.isabs(opt_val) else opt_val
				opt_val_abs = get_path_abs(opt_val,sources=[path_root,'.'])
				if not opt_val_abs: opt_val=os.path.join(path_root,opt_val)
				opt_val = opt_val_abs if opt_val_abs else opt_val
			#if opt_val and opt_val!=default_kwargs.get(opt_name): print(f'   {opt_name} = {opt_val}')
			setattr(self,opt_name,opt_val)


		path_python=get_python_path(self.opts.get('path_python'),path_root)
		# print('>> root:',path_root)
		# print('>> path_python:',path_python)
		# print('>> PATH_CORPUS:',PATH_CORPUS)
		# print('>> PATH_TO_CORPUS_CODE:',PATH_TO_CORPUS_CODE)
		# print()

	@property
	def num_texts(self):
		return len(self.texts())

	def path2text(self,path_key='path_txt'):
		return dict((getattr(t,path_key),t) for t in self.texts())

	## Sections
	@property
	def path_metadata_sections(self): return os.path.join(self.path_root,'metadata_section.csv')
	@property
	def metadata_sections(self):
		if not hasattr(self,'_metadata_sections'):
			import pandas as pd
			self._metadata_sections = pd.read_csv(self.path_metadata_sections,error_bad_lines=False).set_index('_id')
		return self._metadata_sections


	### TEXT RELATED

	@property
	def path_ext_texts(self):
		if self.path_xml:
			return (self.path_xml, self.ext_xml)
		elif self.path_txt:
			return (self.path_txt, self.ext_txt)
		else:
			return ('', '')

	def text(self,idx):
		try:
			return self.textd_addr[idx]
		except KeyError:
			return self.textd[idx]

	@property
	def idx(self):
		if hasattr(self,'name_module'): return self.name_module
		return os.path.splitext(os.path.split(__file__)[-1])[0]


	@property
	def Texts(self): return self.texts()

	@property
	def textl(self): return self.texts()

	def texts(self,text_ids=None,combine_matches=True,limit=False,from_files=False):
		if text_ids is not None:
			self._texts=[self.TEXT_CLASS(idx,self) for idx in text_ids]
		if not hasattr(self,'_texts'):
			self._texts=[self.TEXT_CLASS(idx,self) for idx in sorted(self.get_text_ids(limit=limit,from_files=from_files))]
			for i,t in enumerate(self._texts): t._i = i
		return self._texts

	@property
	def textd(self):
		if not hasattr(self,'_textd'):
			self._textd=td={}
			for t in self.texts():
				td[t.id]=t
		return self._textd

	@property
	def textd_addr(self):
		if not hasattr(self,'_textd_addr'):
			self._textd_addr=td={}
			for t in self.texts():
				td[t.addr]=t
		return self._textd_addr

	def text_random(self):
		import random
		return random.choice(self.texts())

	@property
	def exists_metadata(self):
		return os.path.exists(self.path_metadata)

	def get_id_from_metad(self,metad,addr_key='_addr'):
		if addr_key and addr_key in metad:
			return metad[addr_key]
		if self.col_id and self.col_id in metad:
			return metad[self.col_id]
		if self.col_fn and self.col_fn in metad:
			return os.path.splitext(metad[self.col_fn])[0]
		return None

	def get_text_ids(self,from_metadata=True,from_files=False,limit=False):
		if not from_files and self.exists_metadata:
			self._text_ids=[self.get_id_from_metad(d) for d in self.meta]
		else:
			## Otherwise get the filenames from the main file directory
			path,ext=self.path_txt,self.ext_txt
			#print '>> looking for text IDs using files...'
			if not path or not ext:
				self._text_ids=[]
			else:
				# let's go recursive
				fns=[]
				for root, sub_folders, files in os.walk(path):
					for fn in files:
						if fn.endswith(ext):
							idx=os.path.join(root,fn).replace(path,'').replace(ext,'')
							if idx.startswith('/'): idx=idx[1:]
							fns+=[idx]
						if limit and len(fns)>=limit: break
					if limit and len(fns)>=limit: break

				self._text_ids=fns




		return self._text_ids

	@property
	def text_ids(self):
		if not hasattr(self, '_text_ids'):
			#return self.get_text_ids(from_files=True)
			return self.get_text_ids()
		return self._text_ids

	def limit_texts(self,year_range=[]):
		if year_range:
			year_range=list(range(year_range[0], year_range[1]))
			self._text_ids = [tid for tid in self.text_ids if self.metad[tid]['year'] in year_range]

	def split_texts(self):
		for text in self.texts():
			text.split()

	def new_grouping(self):
		return CorpusGroups(self)






	###########################################################################
	## SLINGSHOT OR SOLO!!
	def slingshot_or_solo(self,method_name,force=False,slingshot=False,slingshot_n=None,slingshot_opts='',cmd_args=[],cmd_kwargs={},force_slingshot=False,shuffle_texts=True):
		print('>> %s.%s() ...' % (self.name, method_name))
		## loop
		if slingshot or force_slingshot:
			if not slingshot: slingshot_n=1
			Scmd=tools.slingshot_cmd_starter(self.name,method_name,slingshot_n,slingshot_opts)
			#Scmd+=' -nosave'

			if cmd_args: Scmd+=" -args '%s'" % json.dumps(cmd_args)
			if cmd_kwargs: Scmd+=" -kwargs '%s'" % json.dumps(cmd_kwargs)
			os.system(Scmd)
		else:
			texts = self.texts()
			if shuffle_texts: random.shuffle(texts)
			for i,text in enumerate(tqdm(texts)):
				f=getattr(text,method_name)
				f(*cmd_args,**cmd_kwargs)

	def save_freqs(self,force=False,slingshot=False,slingshot_n=None,slingshot_opts=''):
		#print(slingshot,slingshot_n,slingshot_opts)
		#slingshot_opts+=' -savedir _tmp_save_freqs -overwrite -savecsv %s' % self.get_path_freq_table()
		#slingshot_opts+=' -nosave'
		return self.slingshot_or_solo('save_freqs',force=force,slingshot=slingshot,slingshot_n=slingshot_n,slingshot_opts=slingshot_opts,force_slingshot=False)

	def save_plain_text(self,force=False,slingshot=False,slingshot_n=None,slingshot_opts=''):
		slingshot_opts+=' -nosave'
		return self.slingshot_or_solo('save_plain_text',force=force,slingshot=slingshot,slingshot_n=slingshot_n,slingshot_opts=slingshot_opts)

	def save_metadata_from_meta_jsons(self):
		
		metadatas=defaultdict(list)
		#from tqdm import tqdm
		from p_tqdm import p_map
		to_walk = sorted(list(os.walk(self.path_texts))) #[:1000]

		def do_walk(args):
			(root,dirs,fns)=args
			meta=None
			meta_type=None
			if 'meta.json' in fns:
				with open(os.path.join(root,'meta.json')) as f:
					meta=json.load(f)
					meta_type=meta.get('_type')
				#metadatas[meta['_type']]+=[meta]
			return (meta_type,meta)
		
		all_meta = p_map(do_walk, to_walk)
		print(len(all_meta))
		for mt,m in all_meta:
			if mt and m:
				metadatas[mt]+=[m]
			
		for metatype,meta_ld in metadatas.items():
			path_meta=os.path.join(self.path_home,'metadata_'+metatype+'.csv')
			#print(path_meta,'...')
			#tools.write(path_meta, meta_ld, sep='\t')
			pd.DataFrame(meta_ld).set_index('_id').to_csv(path_meta) #,index=False)

	



	def save_metadata(self,ofn=None,slingshot=False,slingshot_n=None,slingshot_opts='',force=False):
		from lltk import tools

		if not ofn: ofn=tools.iter_filename(self.path_metadata) if not force else self.path_metadata
		print('>> [%s] saving metadata to: %s...' % (self.name, ofn))

		# get texts
		texts = self.texts()
		num_texts = len(texts)

		## loop

		if slingshot:
			#Scmd='slingshot -lltk_corpus {corpus} -lltk_method get_meta_from_file -savedir _tmp_save_metadata -overwrite -savecsv {savecsv}'.format(corpus=self.name,slingshot=slingshot,savecsv=ofn)
			Scmd='slingshot -lltk_corpus {corpus} -lltk_method get_metadata -savedir _tmp_save_metadata -overwrite -savecsv {savecsv}'.format(corpus=self.name,slingshot=slingshot,savecsv=ofn)
			if slingshot_n: Scmd+=' -parallel {slingshot_n}'.format(slingshot_n=slingshot_n)
			if slingshot_opts: Scmd+=' '+slingshot_opts.strip()
			os.system(Scmd)
		else:
			from tqdm import tqdm

			def _writegen():
				for i,text in enumerate(tqdm(texts)):
					#md=text.meta_by_file if hasattr(text,'meta_by_file') else text.meta
					yield text.get_metadata()

			tools.writegen(ofn, _writegen)
	###########################################################################











	## METADATA

	def pathlist(self,path_attr='path'):
		for t in self.texts():
			x=getattr(t,path_attr)
			#print(path_attr,x)
			if x: yield x

	def save_pathlist(self,fn=None,path_attr='path'):
		if not fn: fn='paths.%s.txt' % self.name
		with open(fn,'w') as f:
			f.write('\n'.join(self.pathlist(path_attr=path_attr)))

	def load_metadata(self,combine_matches=False,load_text_data=True,load_rel_data=False,minimal=False,maximal=False,fix_ids=True):
		from lltk import tools
		import pandas as pd

		#meta_ld=tools.read_ld(self.path_metadata,keymap={'*':str})
		self._metadf=meta_df=tools.read_csv_with_pandas(self.path_metadata,dtype=None,low_memory=False,error_bad_lines=False).fillna('')
		self._metadf['corpus']=self.name
		self._meta=meta_ld=meta_df.to_dict('records')
		self._text_ids=[self.get_id_from_metad(d) for d in meta_ld]
		self._texts=[self.TEXT_CLASS(idx,self) for idx in self._text_ids]
		self._metad=dict(list(zip(self._text_ids,self._meta)))

		for i,dx in enumerate(self._meta):
			t = self._texts[i]
			t._meta = dx
			#dx['id']=t.id
			#dx['corpus']=t.corpus
			#dx['_lltk_']=t.addr

		if maximal or (not minimal and load_text_data): self.load_text_data()
		if maximal or (not minimal and combine_matches): self.combine_matches()
		if maximal or (not minimal and load_rel_data): self.load_rel_data()

	def load_text_data(self):
		from lltk import tools
		all_keys=set()
		for data_fn in self.paths_text_data:
			data_ld=tools.read_ld(data_fn,keymap={'*':six.text_type})
			for _d in data_ld:
				if not 'id' in _d: continue
				idx=_d['id']
				if not idx in self._metad: continue
				all_keys|=set(_d.keys())
				for k,v in list(_d.items()):
					if k in self._metad[idx]: continue
					self._metad[idx][k]=v
		for idx in self._metad:
			missing_keys = all_keys - set(self._metad[idx].keys())
			for k in missing_keys:
				self._metad[idx][k]=''

	def load_rel_data(self):
		# load graph
		for data_fn in self.paths_rel_data:
			now=time.time()
			print('>> reading as tsv:',data_fn)
			try:
				rel=data_fn.split('.')[data_fn.split('.').index('rel')+1]
			except IndexError:
				print("!! please format relation data files as:")
				print("data.rel.RELATION_TYPE.txt")
				continue
			if not rel in self.graph_text_rel:
				import networkx as nx
				self.graph_text_rel[rel]=nx.DiGraph()
			with codecs.open(data_fn,encoding='utf-8') as f:
				for ln in f:
					lndat=ln.strip().split('\t')
					if len(lndat)!=2: continue
					a,b=lndat
					#if not a in self._metad or not b in self._metad: continue
					self.graph_text_rel[rel].add_edge(str(a),str(b),rel=rel)
			nownow=time.time()
			print('>> done ['+str(round(nownow-now,1))+' seconds]')

		# merge matched corporas' relational data
		for corpname,corp in list(self.matched_corpora.items()):
			#corp.load_metadata(load_text_data=False, combine_matches=False, load_rel_data=True)
			for rel in corp.graph_text_rel:
				#if not rel in self.graph_text_rel: continue
				g_other=corp.graph_text_rel[rel]
				g_self=self.graph_text_rel[rel]
				for id_other1,id_other2 in g_other.edges():
					#print id_other1,id_other2
					for text_self1 in self.matchd[corpname].get(id_other1,[]):
						for text_self2 in self.matchd[corpname].get(id_other2,[]):
							#print text_self1.id,text_self2.id
							g_self.add_edge(text_self1.id,text_self2.id)
					#print

		## postprocess
		# reprint-graph
		if 'reprintOf' in self.graph_text_rel:
			g=self.graph_text_rel['reprintOf']
			for ci,component in enumerate(sorted(nx.weakly_connected_components(g), key = len, reverse = True)):
				component=list(component)
				component.sort(key=lambda idx: (self.metad[idx]['year'],self.metad[idx].get('releaseDate','')))
				first_year=int(self.metad[component[0]]['year'].replace('u','0'))
				from collections import Counter
				titles = Counter([self.metad[idx].get('title','') for idx in component])
				authors = Counter([self.metad[idx].get('author','') for idx in component if self.metad[idx].get('author','')])
				mediums = Counter([self.metad[idx].get('medium','') for idx in component if self.metad[idx].get('medium','')])
				predictions = Counter([self.metad[idx].get('prediction','') for idx in component if self.metad[idx].get('prediction','')])
				most_common_title = titles.most_common(1)[0][0]
				most_common_author = authors.most_common(1)[0][0] if authors.most_common(1) else ''
				most_common_medium = mediums.most_common(1)[0][0] if mediums.most_common(1) else ''
				most_common_prediction = predictions.most_common(1)[0][0] if predictions.most_common(1) else ''
				for ti,idx in enumerate(component):
					self.metad[idx]['reprint_cluster_id']=ci+1
					self.metad[idx]['reprint_cluster_name']=most_common_title
					self.metad[idx]['reprint_cluster_author']=most_common_author
					self.metad[idx]['reprint_cluster_medium']=most_common_medium
					self.metad[idx]['reprint_cluster_prediction']=most_common_prediction
					self.metad[idx]['reprint_cluster_medium_or_prediction']=most_common_medium if most_common_medium else most_common_prediction
					self.metad[idx]['reprint_cluster_size']=len(component)
					self.metad[idx]['reprint_cluster_rank']=ti+1
					self.metad[idx]['reprint_cluster_years_since_first_pub']=int(self.metad[component[0]]['year'].replace('u','0'))-first_year
					self.metad[idx]['reprint_cluster_first_pub']=first_year

			for dx in self.meta: dx['reprinted']='reprint_cluster_rank' in dx and int(dx['reprint_cluster_rank'])>1

		if 'reprintOf' in self.graph_text_rel and 'nextVolOf' in self.graph_text_rel:
			g_vol = self.graph_text_rel['nextVolOf']
			g_vol_components = [set(comp) for comp in nx.weakly_connected_components(g_vol)]
			g=self.graph_text_rel['reprintOf+nextVolOf']=nx.compose(self.graph_text_rel['reprintOf'],self.graph_text_rel['nextVolOf'])
			for ci,component in enumerate(sorted(nx.weakly_connected_components(g), key = lambda _comp: len({self.metad[idx]['ESTCID'] for idx in _comp}), reverse = True)):
				component=list(component)
				component.sort(key=lambda idx: (self.metad[idx]['year'],self.metad[idx]['releaseDate'],idx))
				estc_ids = {self.metad[idx]['ESTCID'] for idx in component}
				from collections import Counter
				titles = Counter([self.metad[idx].get('title','') for idx in component if self.metad[idx].get('title','')])
				authors = Counter([self.metad[idx].get('author','') for idx in component if self.metad[idx].get('author','')])
				mediums = Counter([self.metad[idx].get('medium','') for idx in component if self.metad[idx].get('medium','')])
				predictions = Counter([self.metad[idx].get('prediction','') for idx in component if self.metad[idx].get('prediction','')])
				most_common_title = titles.most_common(1)[0][0] if titles.most_common(1) else ''
				most_common_author = authors.most_common(1)[0][0] if authors.most_common(1) else ''
				most_common_medium = mediums.most_common(1)[0][0] if mediums.most_common(1) else ''
				most_common_prediction = predictions.most_common(1)[0][0] if predictions.most_common(1) else ''
				first_year=int(self.metad[component[0]]['year'])
				rank_num=0
				vol_components_done=set()
				for ti,idx in enumerate(component):
					vol_comp = [i for i,x in enumerate(g_vol_components) if idx in x]
					vol_comp = vol_comp[0] if vol_comp else None
					if vol_comp==None or not vol_comp in vol_components_done:
						vol_components_done|={vol_comp}
						rank_num+=1

					self.metad[idx]['title_reprint_cluster_id']=ci+1
					self.metad[idx]['title_reprint_cluster_name']=most_common_title
					self.metad[idx]['title_reprint_cluster_author']=most_common_author
					self.metad[idx]['title_reprint_cluster_medium']=most_common_medium
					self.metad[idx]['title_reprint_cluster_prediction']=most_common_prediction
					self.metad[idx]['title_reprint_cluster_medium_or_prediction']=most_common_medium if most_common_medium else most_common_prediction
					self.metad[idx]['title_reprint_cluster_first_pub']=first_year
					self.metad[idx]['title_reprint_cluster_size']=len(estc_ids)
					self.metad[idx]['title_reprint_cluster_size_in_volumes']=len(component)
					self.metad[idx]['title_reprint_cluster_rank']=rank_num #ti+1
					self.metad[idx]['title_reprint_cluster_years_since_first_pub']=int(self.metad[idx]['year'])-first_year

			for dx in self.meta: dx['reprinted']='title_reprint_cluster_rank' in dx and int(dx['title_reprint_cluster_rank'])>1

	def save_additional_metadata(self,fn=None,calculate={'ocr_accuracy','length'}):
		from lltk import tools
		if not fn: fn=os.path.join(self.path_data,'additional_metadata.txt')
		return
		print('?',fn)
		stop
		if os.path.exits(fn): return

		def writegen():
			for i,text in enumerate(tqdm(self.textl)):
				#dx=dict([('id',text.id)] + text.meta.items())
				meta=dict(text.meta.items())
				meta['id']=text.id
				yield d2
		tools.writegen(fn,writegen)

	def get_metadata(self):
		return self.meta

	@property
	def meta(self):
		if not hasattr(self,'_meta'):
			if os.path.exists(self.path_metadata):
				self.load_metadata()
			#else:
			#	return [t.meta_by_file for t in self.texts()]
		return [t.meta for t in self.texts()]


	def zip(self,savedir=None, ask=True, sbatch=False, sbatch_opts='', parts=ZIP_PART_DEFAULTS):
		if not savedir: savedir=os.path.join(PATH_CORPUS,'lltk_corpora')
		if not os.path.exists(savedir): os.makedirs(savedir)

		## ask which parts
		part2ok=defaultdict(None)
		for part in sorted(parts):
			path_part=getattr(self,f'path_{part}')
			if not path_part or not os.path.exists(path_part): continue
			part2ok[part]=input('>> [%s] Zip %s file(s)?: ' % (self.name, part)).strip().lower().startswith('y') if ask else True

		def _paths(path):
			for root, dirs, files in os.walk(path):
				for file in files:
					ofnfn=os.path.join(root, file)
					yield ofnfn

		def zipdir(path, ziph, paths=None):
			# ziph is zipfile handle
			if not paths: paths=_paths(paths)
			for ofnfn in paths:
				ziph.write(ofnfn)
				yield ofnfn

		def do_zip(path,fname,msg='Zipping files',default=False):
			if not os.path.exists(path): return
			#if ask and input('>> {msg}? [{path}]\n'.format(msg=msg,path=path)).strip()!='y': return
			if not default: return

			if not fname.endswith('.zip'): fname+='.zip'
			opath=os.path.join(savedir,fname)

			path1,path2=os.path.split(path)

			if not sbatch:
				import zipfile
				from tqdm import tqdm

				with zipfile.ZipFile(opath,'w',zipfile.ZIP_DEFLATED) as zipf:
					print('>> compressing: {a} --> {b}'.format(a=path,b=opath))
					#print('\tto:',opath)
					#print('\tfrom:',path2)
					#print('\tin:',path1)

					os.chdir(path1)
					paths=list(_paths(path2)) if os.path.isdir(path2) else [path2]
					#print(type(paths),paths[:3])
					zipper = zipdir(path2, zipf, paths=paths)
					for ofnfn in tqdm(zipper, total=len(paths)):
						pass

			else:
				zipcmd='zip -r9 {opath} {path}'.format(path=path2,opath=opath)
				if sbatch: zipcmd = 'sbatch {sbatch_opts} --wrap="{zipcmd}"'.format(sbatch_opts=sbatch_opts,zipcmd=zipcmd)
				cmd='cd "{cdto}" && {zipcmd}'.format(cdto=path1,zipcmd=zipcmd)
				print(cmd)
				os.system(cmd)

		for part in part2ok:
			if not part2ok[part]: continue
			do_zip(getattr(self,f'path_{part}'), f'{self.id}_{part}.zip', f'Zip {part} files',part in parts)
		# do_zip(self.path_txt, self.id+'_txt.zip','Zip txt files','txt' in parts)
		# do_zip(self.path_freqs, self.id+'_freqs.zip','Zip freqs files','freqs' in parts)
		# do_zip(self.path_metadata, self.id+'_metadata.zip','Zip metadata file','metadata' in parts)
		# do_zip(self.path_xml, self.id+'_xml.zip','Zip xml files','xml' in parts)
		# do_zip(self.path_data, self.id+'_data.zip','Zip data files (mfw/dtm)','xml' in parts)


	def upload(self,ask=True,uploader='dbu upload',dest=DEST_LLTK_CORPORA,zipdir=None,overwrite=False):
		#if not overwrite: uploader+=' -s'
		if not zipdir: zipdir=os.path.join(PATH_CORPUS,'lltk_corpora')
		os.chdir(zipdir)
		#print('?',zipdir,os.listdir('.'))

		cmds=[]
		for fn in os.listdir('.'):
			if not fn.endswith('.zip'): continue
			if not fn.startswith(self.id): continue
			if ask and not input(f'>> [{self.name}] Upload {fn}? ').strip().lower().startswith('y'): continue
			cmd='{upload} {file} {dest}'.format(upload=uploader,file=fn,dest=dest)
			cmds.append(cmd)

		for cmd in cmds:
			print('>>',cmd)
			os.system(cmd)

	def share(self,cmd_share='dbu share',dest=DEST_LLTK_CORPORA):
		ol=[]
		import subprocess
		ln='['+self.name+']'
		print(ln)
		ol+=[ln]
		for part in ZIP_PART_DEFAULTS:
			fnzip = self.id+'_'+part+'.zip'
			cmd=cmd_share+' '+os.path.join(dest,fnzip)
			try:
				out=str(subprocess.check_output(cmd.split()))
			except (subprocess.CalledProcessError,ValueError,TypeError) as e:
				#print('!!',e)
				continue
			link=out.strip().replace('\n','').split('http')[-1].split('?')[0]
			if link: link='http'+link+'?dl=1'

			url='url_'+part+' = '+link
			print(url)
			ol+=[url]
		print()
		return '\n'.join(ol)


	def mkdir_root(self):
		if not os.path.exists(self.path_root): os.makedirs(self.path_root)

	def chdir_root(self):
		os.chdir(self.path_root)

	def urls(self):
		urls=[(x[4:], getattr(self,x)) for x in dir(self) if x.startswith('url_') and getattr(self,x)]
		return urls

	def compile(self, **attrs):
		## THIS NEEDS TO BE OVERWRITTEN BY CHILD CLASS
		return

	def download(self, ask=True, urls=None, part=None, **attrs):
		self.mkdir_root()
		self.chdir_root()

		if not urls: urls=URLS=self.urls()

		url2ok=defaultdict(None)
		if part:
			for urltype,url in URLS: url2ok[urltype]=False
			url2ok[part]=True
		elif ask:
			for urltype,url in URLS:
				url2ok[urltype]=input('>> [%s] Download %s file(s)?: ' % (self.name, urltype)).strip().lower().startswith('y')
		else:
			url2ok=defaultdict(True)

		for (urltype,url) in URLS:
			if not url2ok[urltype]: continue
			tmpfn='_tmp_%s_%s.zip' % (self.id, urltype)
			print('\n>> downloading:',tmpfn)
			tools.download(url,tmpfn)

			print('\n>> unzipping:',tmpfn)
			tools.unzip(tmpfn)
			os.unlink(tmpfn)

		"""cmd='cd {p1} && wget -O {p3} {p2} && unzip -n {p3} -x "*.py*" -x "*.DS_Store" -x "*__pycache__*" && rm {p3}'.format(
		p1=PATH_CORPUS,
		p2=self.url_download,
		p3='_download.zip')

		import os

		ofnfn=os.path.join(PATH_CORPUS,'_download.zip')
		print('>> saving temporarily to:',ofnfn)

		print(cmd)
		import os
		os.system(cmd)"""

	def compile_get_raw(self):
		now=os.getcwd()

		# move to raw folder
		if not os.path.exists(self.path_raw):
			self.download(part='raw')


	def command_prep(self,method,attrs):
		valid_args=set(tools.valid_args_for(method))
		return dict((k,v) for k,v in attrs.items() if k in valid_args)


	def install_metadata(self,**attrs):
		force=attrs.get('force')
		if force or not os.path.exists(self.path_metadata):
			self.save_metadata(**self.command_prep(self.save_metadata,attrs))
			self.save_additional_metadata(**self.command_prep(self.save_additional_metadata,attrs))
		else:
			print('>> metadata already exists:',self.path_metadata)


	def install_txt(self,**attrs):
		attrs=self.command_prep(self.save_plain_text,attrs)
		self.save_plain_text(**attrs)


	def install_freqs(self,**attrs):
		attrs=self.command_prep(self.save_freqs,attrs)
		self.save_freqs(**attrs)


	def install_mfw(self,**attrs):
		attrs=self.command_prep(self.save_mfw,attrs)
		self.save_mfw(**attrs)


	def install_dtm(self,**attrs):
		attrs=self.command_prep(self.save_dtm,attrs)
		self.save_dtm(**attrs)

	def install(self,include=[],exclude=[],**attrs):
		"""
		Consider overwriting this
		"""
		self.mkdir_root()
		self.chdir_root()

		parts=include if include else INSTALL_CMDS
		parts=[x for x in parts if x not in set(exclude)]

		if 'part' in attrs and attrs['part']:
			parts=[attrs['part']]

		# metadata
		for part in parts:
			#print('>>',part)
			fname='install_'+part
			if not hasattr(self,fname): continue
			func=getattr(self,fname)
			func(**attrs)




	@property
	def metadf(self,add_paths=True):
		if not hasattr(self,'_metadf'):
			import pandas as pd
			df=self._metadf=pd.DataFrame(self.meta)
			#if add_paths:
				#df['_path_txt']=[os.path.join(self.path_txt, idx + self.ext_txt) for idx in df['id']]
				#df['_path_xml']=[os.path.join(self.path_xml, idx + self.ext_txt) for idx in df['id']]
				#df['_path_txt']=[t.path_txt for t in self.texts()]
				#df['_path_xml']=[t.path_xml for t in self.texts()]
		return self._metadf

	@property
	def metadata(self):
		return self.metadf

	@property
	def df(self): return self.metadf

	@property
	def addr2meta(self):
		if not hasattr(self,'_addr2meta'):
			self._addr2meta=a2m={}
			for t in self.texts():
				meta=t.meta
				a2m[t.addr]=meta
		return self._addr2meta

	@property
	def metad(self):
		if not hasattr(self,'_metad'):
			#if not hasattr(self,'_meta'): self.meta
			self._metad=dict(list(zip(self.text_ids,self.meta)))
		return self._metad

	def export(self,folder,meta_fn=None,txt_folder=None,compress=False):
		"""
		Exports everything to a folder.
		"""
		if not os.path.exists(folder): os.makedirs(folder)
		if not meta_fn: meta_fn='corpus-metadata.%s.txt' % self.name
		if not txt_folder: txt_folder='texts'

		# save metadata
		meta_fnfn=os.path.join(folder,meta_fn)
		self.save_metadata(meta_fnfn)

		# save texts
		txt_path=os.path.join(folder,txt_folder)
		if not os.path.exists(txt_path): os.makedirs(txt_path)
		import shutil
		for t in self.texts():
			ifnfn=t.path_txt
			if not os.path.exists(ifnfn): continue
			ofnfn=os.path.join(txt_path, t.id+t.ext_txt)
			ofnfn_path=os.path.split(ofnfn)[0]
			if not os.path.exists(ofnfn_path): os.makedirs(ofnfn_path)
			shutil.copyfile(ifnfn,ofnfn)
			#break






	### FREQS

	def get_path_freq_table(self,n=None,force=False,discover=True):
		if not os.path.exists(self.path_data): os.makedirs(self.path_data)
		return os.path.join(self.path_data,'dtm.txt.gz')



	# def get_path_freq_table(self,n=None,force=False,discover=True):
	# 	if force:
	# 		if not n:
	# 			return os.path.join(self.path,'freqs','data.freqs.%s.worddb.table.txt.gz' % self.name)
	# 		else:
	# 			return os.path.join(self.path,'freqs','data.freqs.%s.worddb.table.%s.txt.gz' % (self.name,str(n)+'MFW'))
	#
	# 	pathd=self.path_freq_table if hasattr(self,'path_freq_table') else {}
	# 	if not pathd and not discover: return ''
	# 	if not pathd and discover:
	# 		freqdir=os.path.join(self.path,'freqs')
	# 		candidates = [fn for fn in os.listdir(freqdir) if fn.endswith('.txt.gz') and self.name in fn]
	# 		for cfn in candidates:
	# 			size=cfn.split('.table.')[1].split('.txt.gz')[0]
	# 			if 'MFW' in size:
	# 				size=int(size.replace('MFW',''))
	# 				pathd[size]=os.path.join(freqdir,cfn)
	# 			else:
	# 				pathd[1000000]=os.path.join(freqdir,cfn)
	# 		if not candidates: return ''
	#
	# 	sizes = sorted(pathd.keys())
	# 	if not n: return pathd[max(sizes)]
	# 	for size in sizes:
	# 		if size>=n:
	# 			return pathd[size]
	# 	return pathd[max(sizes)]

	@property
	def dtm(self):
		return self.freqs(fpm=False)

	def freqs(self,n=5000,toks=[],text_ids=None,sep='\t',encoding='utf-8',tf=False,fpm=False,z=False):
		import time
		"""
		Load a pandas dataframe for specified number of words (n) or for specied words (toks),
		for all texts or for specified ones (text_ids).

		Assumes self.path_freq_table points to a gzipped tab-separated file of the form:
		[blank]   [tab] [most-freq-word1] [tab] [most-freq-word-2] ...
		[textid1] [tab] [count1]          [tab] [count2]           ...
		[textid2] [tab] [count1]          [tab] [count2]           ...
		"""
		from lltk.tools import find_nth_character
		from lltk import tools

		if toks: toks=set(toks)
		if text_ids is not None: text_ids=set(text_ids)
		import pandas as pd, numpy as np

		fnfn=self.get_path_freq_table(n=n,discover=True)

		header=tools.header(fnfn,tsep=sep,encoding=encoding)
		if toks:
			toks=toks&set(header[1:])
			index_tok=[(header.index(tok),tok) for tok in toks]
			index_tok.sort()
			#dtypes = [('index','S50')]+[(tok,'i4') for i,tok in index_tok]
			columns=[tok for i,tok in index_tok]
		elif n:
			#dtypes = [('index','S50')]+[(tok,'i4') for tok in header[1:n+1]]
			columns=[tok for tok in header[1:n+1]]
		else:
			#dtypes = [('index','S50')]+[(tok,'i4') for tok in header[1:]]
			columns=[tok for tok in header[1:]]

		data=[]
		indices=[]
		#for row in tools.readgen(fnfn,as_list=True):

		now=time.time()
		print('>> streaming freqs:',fnfn)
		f=gzip.open(fnfn) if fnfn.endswith('.gz') else open(fnfn)
		for i,ln in enumerate(f):
			ln=ln[:-1] # remove \n
			ln=ln.decode(encoding=encoding)
			if i==0: continue  # = header
			str_index = ln.index(sep)
			index=ln[:str_index]
			if text_ids is not None and not index in text_ids: continue
			indices+=[index]


			row=ln.split(sep)
			#if toks and n:
			#	allowed=set(row[1:n])
			#	row = [row[i] for i,tok in index_tok if row[i] in allowed]
			if toks:
				#row=ln.split(sep)
				row = [row[i] for i,tok in index_tok]
			elif n:
				row = row[1:n+1]
				#ln = ln[str_index+1:find_nth_character(ln,sep,n+1)]
				#row=ln.split(sep)
			else:
				#row=ln[str_index+1:].split(sep)
				row=row[1:]


			#row=np.array(row,dtype=int)
			#row=[float(x) for x in row]
			data+=[row]
		f.close()
		nownow=time.time()
		print('   done ['+str(round(nownow-now,1))+' seconds]')


		now=time.time()
		print('>> generating numpy array...')
		#X = np.zeros((len(data),len(data[0])), dtype=int)
		#X[:] = data
		#data=X
		data = np.array(data, dtype=float)

		#data = np.array(data)
		nownow=time.time()
		print('   done [{0} seconds]'.format(round(nownow-now,1)))

		now=time.time()
		print('>> generating pandas dataframe...')
		#data = [pd.to_numeric(row) for row in data]
		df=pd.DataFrame(data,index=indices,columns=columns)
		df.fillna(0,inplace=True)

		if tf or fpm:
			lengths = [self.textd[idx].length for idx in indices]
			df = df.divide(lengths,axis=0)
		if fpm:
			df = df * 1000000

		if z:
			for colname in df.columns:
				mean,std=df[colname].mean(),df[colname].std()
				df[colname] = (df[colname] - mean) / std

		nownow=time.time()
		print('   done [{0} seconds]'.format(round(nownow-now,1)))
		return df


	def save_dtm(self,n=10000,words=None,only_english=False,path=None,index_attr='id'):
		words = list(self.mfw(n=n,only_english=only_english)) if not words else words
		path = self.get_path_freq_table(n=n,force=True) if not path else path
		path_path = os.path.split(path)[0]
		if not os.path.exists(path_path): os.makedirs(path_path)

		texts=list(self.texts())
		texts.sort(key=lambda t: t.id)

		def writegen():
			for t in tqdm(texts):
				tfreqs=t.freqs()
				odx=dict((w,tfreqs.get(w,0)) for w in words)
				odx['']=getattr(t,index_attr)
				yield odx

		print(f'>> [{self.name}] building DTM (#words={n}): {path}')
		tools.writegen(path, writegen, header=['']+words)

	def freqs_async(self,words={},texts=[]):
		import multiprocessing as mp
		pool=mp.Pool()
		if not texts: texts=self.texts()
		for i,dx in enumerate(pool.imap(get_freq_async, texts)):
			yield dict((k,v) for k,v in dx.items() if not words or k in words)

	### MFW

	@property
	def path_mfw(self):
		return os.path.join(self.path_data, 'mfw.txt')
	@property
	def path_home(self):
		return os.path.join(PATH_CORPUS,self.id)
	@property
	def path_texts(self):
		return os.path.join(self.path_home,'texts')


	def mfw_df(self,**attrs):
		from lltk import tools
		import numpy as np,pandas as pd

		n=attrs.get('n',None)
		pos=attrs.get('only_pos',None)
		alpha=attrs.get('only_alpha',None)
		eng=attrs.get('only_english',None)
		modspell=attrs.get('modernize_spelling',None)
		min_count=attrs.get('min_count',None)
		with_rank=attrs.get('with_count',False)
		mfw_prefix=attrs.get('mfw_prefix','data.mfw.'+self.name+'.')
		if eng:
			from lltk.tools import get_english_wordlist
			ENGLISH=get_english_wordlist()
		if modspell:
			from lltk.tools import get_spelling_modernizer
			SPELLREGD=get_spelling_modernizer()

		mfw_fn=self.path_mfw
		mfw_ld=[]

		for mfw_fn in os.listdir(self.path):
			if not mfw_fn.startswith(mfw_prefix): continue
			mfw_fnfn=os.path.join(self.path, mfw_fn)
			mfw_fn_code = os.path.splitext(mfw_fn)[0].replace(mfw_prefix,'')
			if mfw_prefix.startswith(mfw_fn_code):
				mfw_fn_code='total'
				continue  # skip totalized mfw's

			with codecs.open(mfw_fnfn,encoding='utf-8') as f:
				num=0
				for i,ln in enumerate(f):
					if n and num>=n: break
					lndat=ln.strip().split('\t',1)
					w=lndat[0]
					if alpha and not str(w).replace('-','').isalpha(): continue
					c=float(lndat[1]) if len(lndat)>1 else np.nan
					if min_count and c<min_count: continue
					if eng and not w in ENGLISH: continue
					if modspell: w=SPELLREGD.get(w,w)
					num+=1
					dx={'word':w, 'count':c, 'rank':i, 'group':mfw_fn_code}
					mfw_ld+=[dx]
		mfw_df=pd.DataFrame(mfw_ld) #.set_index('word')
		mfw_df=mfw_df.groupby(['word','group']).agg({'rank':'mean','count':'sum'}).reset_index()

		from scipy.stats import zscore
		mfw_df['z']=zscore(mfw_df['count'])

		total_count=sum(mfw_df['count'])
		mfw_df['fpm']=[c/total_count*1000000 for c in mfw_df['count']]
		return mfw_df


	def mfw_simple(self,**attrs):
		from lltk import tools
		import numpy as np

		n=attrs.get('n',None)
		pos=attrs.get('only_pos',None)
		eng=attrs.get('only_english',None)
		min_count=attrs.get('min_count',None)
		with_count=attrs.get('with_count',False)
		if eng:
			from lltk.tools import get_english_wordlist
			ENGLISH=get_english_wordlist()

		mfw_fn=self.path_mfw
		if os.path.exists(mfw_fn):
			with codecs.open(mfw_fn,encoding='utf-8') as f:
				num=0
				for i,ln in enumerate(f):
					if n and num>=n: break
					#w=ln.split('\t')[0].strip()
					lndat=ln.strip().split('\t',1)
					#print(lndat)
					w=lndat[0]
					c=float(lndat[1]) if len(lndat)>1 else np.nan
					#print([min_count,c])
					if min_count and c<min_count: continue
					if eng and not w in ENGLISH: continue
					num+=1
					yield w if not with_count else (w,c)
		else:

			# recursively try again
			self.gen_mfw()
			for x in self.mfw_simple(**attrs): yield x

	def mfw(self,**attrs):
		func=self.mfw_simple(**attrs)
		#func=self.mfw_modeled(**attrs)
		#return r1 if r1 else self.mfw_text(**attrs)
		for x in func:
			yield x

	def divide_texts_historically(self,yearbin=10,yearmin=None,yearmax=None,texts=None,min_len=None):
		Grouping = CorpusGroups(self,texts=texts)
		if yearmin or yearmax or min_len:
			Grouping.prune_groups(min_len=None,min_group=yearmin,max_group=yearmax)
		Grouping.group_by_year(yearbin=yearbin)
		return Grouping.groups

	def save_mfw(self,**attrs): return self.gen_mfw(**attrs)

	def gen_mfw(self,yearbin=None,year_min=None,year_max=None,include_freq=True,gen_total=False):
		"""
		From tokenize_texts()
		"""
		from bounter import bounter
		from collections import Counter

		import numpy as np
		texts=[t for t in self.texts() if (not year_min or t.year>=year_min) and (not year_max or t.year<year_max)]
		period2texts = {'all':texts} if not yearbin else self.divide_texts_historically(yearbin=yearbin,texts=texts)
		period2bounter = {}
		for period,texts in sorted(period2texts.items()):
			if period.startswith('0-'): continue
			print('>> generating mfw for text grouping \'%s\' (%s texts)' % (period,len(texts)))
			if gen_total:
				countd = bounter(1024)
				#countd = Counter()
			else:
				countd = Counter()

			from tqdm import tqdm
			for i,text in enumerate(tqdm(texts)):
				#if not i%100: print '>>',i,len(texts),'...'
				freqs = text.freqs(use_text_if_nec=False)
				if freqs: countd.update(freqs)

			mfwfn1=os.path.splitext(self.path_mfw)[0]
			path_mfw_period=mfwfn1+'.'+period+'.txt' if period!='all' else self.path_mfw
			path_mfw_period_dir=os.path.dirname(path_mfw_period)
			if not os.path.exists(path_mfw_period_dir): os.makedirs(path_mfw_period_dir)
			with open(path_mfw_period,'w') as f:
				for i,(word,val) in enumerate(sorted(six.iteritems(countd), key=lambda _t: -_t[1])):
					line=word if not include_freq else word+'\t'+str(val)
					f.write(line+'\n')
				print('>> saved:',path_mfw_period)

			if gen_total:
				period2bounter[period] = countd



		if gen_total:
			print('>> generating total...')
			all_words = set()
			totals={}
			for period,countd in list(period2bounter.items()):
				all_words|=set(countd.keys())
				totals[period]=float(countd.total())

			num_words=len(all_words)
			COUNTD={}

			for i,word in enumerate(tqdm(all_words)):
				#if not i%1000: print '>>',i,num_words,'...'
				vals = [float(countd[word])/totals[period]*1000000 for period,countd in list(period2bounter.items())]
				median_val = np.mean(vals)
				#COUNTD.update({word:median_val})
				COUNTD[word]=median_val

			del period2bounter

			with open(self.path_mfw,'w',encoding='utf-8') as f:
				for i,(word,val) in enumerate(sorted(six.iteritems(COUNTD), key=lambda _t: -_t[1])):
					#if not i%1000: print '>>',i,num_words,'...'
					line=word if not include_freq else word+'\t'+str(val)
					f.write(line+'\n')
			print('>> saved:',self.path_mfw)




	### WORD2VEC
	@property
	def model(self):
		if not hasattr(self,'_model'):
			self._model=gensim.models.Word2Vec.load(self.fnfn_model)
		return self._model

	@property
	def fnfn_skipgrams(self):
		return os.path.join(self.path_skipgrams,'skipgrams.'+self.name+'.txt.gz')

	def word2vec(self,skipgram_n=10,name=None,skipgram_fn=None):
		if not name: name=self.name
		from lltk.model.word2vec import Word2Vec
		if skipgram_fn and not type(skipgram_fn) in [six.text_type,str]:
			skipgram_fn=self.fnfn_skipgrams

		return Word2Vec(corpus=self, skipgram_n=skipgram_n, name=name, skipgram_fn=skipgram_fn)

	def doc2vec(self,skipgram_n=5,name=None,skipgram_fn=None):
		if not name: name=self.name
		from lltk.model.word2vec import Doc2Vec
		if not skipgram_fn or not type(skipgram_fn) in [six.text_type,str]:
			skipgram_fn=os.path.join(self.path_skipgrams,'sentences.'+self.name+'.txt.gz')

		return Doc2Vec(corpus=self, skipgram_n=skipgram_n, name=name, skipgram_fn=skipgram_fn)

	def word2vec_by_period(self,bin_years_by=None,word_size=None,skipgram_n=10, year_min=None, year_max=None):
		"""NEW word2vec_by_period using skipgram txt files
		DOES NOT YET IMPLEMENT word_size!!!
		"""
		from lltk.model.word2vec import Word2Vec
		from lltk.model.word2vecs import Word2Vecs

		if not year_min: year_min=self.year_start
		if not year_max: year_max=self.year_end

		path_model = self.path_model
		model_fns = os.listdir(path_model)
		model_fns2=[]
		periods=[]

		for mfn in model_fns:
			if not (mfn.endswith('.txt') or mfn.endswith('.txt.gz')) or '.vocab.' in mfn: continue
			mfn_l = mfn.split('.')
			period_l = [mfn_x for mfn_x in mfn_l if mfn_x.split('-')[0].isdigit()]
			if not period_l: continue

			period = period_l[0]
			period_start,period_end=period.split('-') if '-' in period else (period_l[0],period_l[0])
			period_start=int(period_start)
			period_end=int(period_end)+1
			if period_start<year_min: continue
			if period_end>year_max: continue
			if bin_years_by and period_end-period_start!=bin_years_by: continue
			model_fns2+=[mfn]
			periods+=[period]

		#print '>> loading:', sorted(model_fns2)
		#return

		name_l=[self.name, 'by_period', str(bin_years_by)+'years']
		if word_size: name_l+=[str(word_size / 1000000)+'Mwords']
		w2vs_name = '.'.join(name_l)
		W2Vs=Word2Vecs(corpus=self, fns=model_fns2, periods=periods, skipgram_n=skipgram_n, name=w2vs_name)
		return W2Vs









	## MATCHING TITLES

	def combine_matches(self):
		for idx in self._metad:
			"""
			for match_meta in self.matchd.get(idx,[]):
				if not match_meta: continue
				for k,v in match_meta.items():
					if k in self._metad[idx]: k+='_'+match_meta['corpus']
					self._metad[idx][k]=v
			"""
			for other_text in self.matchd.get(self.name,{}).get(idx,[]):
				match_meta=other_text.meta
				if not match_meta: continue
				for k,v in list(match_meta.items()):
					#print [k,v,self._metad[idx].get(k,'')]

					if 'reprint' in k: # HACK HACK HACK HACK
						k+='_'+other_text.corpus.name
					elif k in self._metad[idx]:
						if self._metad[idx][k]==v: continue
						k+='_'+other_text.corpus.name

					self._metad[idx][k]=v
				#print

	@property
	def matchd(self):
		if not hasattr(self,'_matchd'):
			match_corp2id2ids={}
			match_id2ids={}
			match_fns=[]
			for fn in os.listdir(self.path_matches):
				if (fn.endswith('.xls') or fn.endswith('.xlsx') or fn.endswith('.txt') or fn.endswith('.csv')) and fn.startswith('matches.'):
					match_fns+=[fn]

			for match_fn in match_fns:
				c1name,c2name=match_fn.split('.')[1].split('--')
				#if not self.name in {c1name,c2name}: continue
				if c1name!=self.name: continue  # one-directional matching
				othername = c1name if c1name!=self.name else c2name
				idself = '1' if c1name==self.name else '2'
				idother = '2' if c1name==self.name else '1'
				#print self.name, c1name, c2name, othername

				othercorpus = name2corpus(othername)()
				othercorpus.load_metadata(maximal=True)
				self.matched_corpora[othercorpus.name]=othercorpus
				from lltk import tools
				match_ld=tools.read_ld(os.path.join(self.path_matches,match_fn))
				for matchd in match_ld:
					if matchd.get('match_ismatch','') and not matchd['match_ismatch'] in ['n','N']:
						idx_self=matchd['id'+idself]
						idx_other=matchd['id'+idother]

						if not self.name in match_corp2id2ids: match_corp2id2ids[self.name]={}
						if not othercorpus.name in match_corp2id2ids: match_corp2id2ids[othercorpus.name]={}

						if not idx_self in match_corp2id2ids[self.name]: match_corp2id2ids[self.name][idx_self]=[]
						if not idx_other in match_corp2id2ids[othercorpus.name]: match_corp2id2ids[othercorpus.name][idx_other]=[]

						#match_corp2id2ids[self.name][idx_self]+=[name2text(othername)(idx_other, othercorpus)]
						match_corp2id2ids[self.name][idx_self]+=[othercorpus.textd[idx_other]]
						match_corp2id2ids[othercorpus.name][idx_other]+=[self.textd[idx_self]]

						#if not idx_self in match_id2ids: match_id2ids[idx_self]=[]
						#match_id2ids[idx_self]+=[name2text(othername)(idx_other, othercorpus)]

						#match_id2ids[idx_self]+=[othercorpus.metad[idx_other]]

			self._matchd=match_corp2id2ids
		return self._matchd

	def match_records(self,corpus,match_by_id=False,match_by_title=True, title_match_cutoff=90, filter1={}, filter2={}, id_field_1='id',id_field_2='id',year_window=0):
		c1=self
		c2=corpus

		texts1=[t for t in c1.texts() if not False in [t.meta[k]==v for k,v in list(filter1.items())]]
		texts2=[t for t in c2.texts() if not False in [t.meta[k]==v for k,v in list(filter2.items())]]

		print('Matching from '+c1.name+' ('+str(len(texts1))+' texts)')
		print('\t to '+c2.name+' ('+str(len(texts2))+' texts)')
		print()

		matches={}


		if match_by_id:
			from collections import defaultdict
			#id2t1=dict([(t.meta[id_field_1], t) for t in texts1])
			#id2t2=dict([(t.meta[id_field_2], t) for t in texts2])
			id2t1=defaultdict(list)
			id2t2=defaultdict(list)
			for t in texts1: id2t1[t.meta[id_field_1]].append(t)
			for t in texts2: id2t2[t.meta[id_field_2]].append(t)

			for i,(idx,t1) in enumerate(sorted(id2t1.items())):
				#print i,idx #,t1.id,'...'
				if idx in id2t2:
					#print 'match!' #,t1.id,id2t2[idx].id
					for t1 in id2t1[idx]:
						for t2 in id2t2[idx]:
							matches[t1.id]=[(t2.id,100,'Y')]

		if match_by_title:
			from fuzzywuzzy import fuzz
			for i,t1 in enumerate(texts1):
				if t1.id in matches: continue
				#print i,t1.id,t1.title,'...'
				for t2 in texts2:
					if not t1.meta or not t2.meta: continue
					if not t1.title.strip() or not t2.title.strip(): continue
					#print '\t',i,t2.id,t2.corpus.name,t1.meta['year'],t2.meta['year'],t2.title,'...'
					try:
						if abs(float(t2.meta['year']) - float(t1.meta['year']))>year_window:
							#print '>> skipping...'
							continue
					except (ValueError,TypeError) as e:
						# print "!!",e,
						# print "!!",t1.meta
						# print "!!",t2.meta
						continue
					mratio=fuzz.partial_ratio(t1.title, t2.title)
					if mratio>title_match_cutoff:
						if not t1.id in matches: matches[t1.id]=[]
						matches[t1.id]+=[(t2.id,mratio,'')]

		old=[]
		for t1_id in matches:
			matches[t1_id].sort(key=lambda _t: -_t[1])
			for t2_id,mratio,is_match in matches[t1_id]:
				dx={}
				for k,v in list(c1.metad[t1_id].items()):
					if not k in ['author','title','year','id']: continue
					dx[k+'1']=v
				for k,v in list(c2.metad[t2_id].items()):
					if not k in ['author','title','year','id']: continue
					dx[k+'2']=v
				dx['match_ratio']=mratio
				dx['match_ismatch']=is_match
				old+=[dx]
		from lltk import tools
		tools.write2(os.path.join(self.path,'matches.'+c1.name+'--'+c2.name+'.txt'), old)

	def copy_match_files(self,corpus,match_fn=None,copy_xml=True,copy_txt=True):
		import shutil
		from lltk import tools
		match_fn = os.path.join(self.path,match_fn) if match_fn else os.path.join(self.path,'matches.'+corpus.name+'.xls')
		matches = [d for d in tools.read_ld(match_fn) if d['match_ismatch'].lower().strip()=='y']

		c1=self
		c2=corpus

		for d in matches:
			t1=c1.text(d['id1'])
			t2=c2.text(d['id2'])

			if copy_txt:
				if os.path.exists(t1.fnfn_txt): continue
				print(t2.fnfn_txt)
				print('-->')
				print(t1.fnfn_txt)
				print()
				shutil.copyfile(t2.fnfn_txt, t1.fnfn_txt)

			if copy_xml:
				if os.path.exists(t1.fnfn_xml): continue
				print(t2.fnfn_xml)
				print('-->')
				print(t1.fnfn_xml)
				print()
				shutil.copyfile(t2.fnfn_xml, t1.fnfn_xml)


	### DE-DUPING

	def rank_duplicates_bytitle(self,within_author=False,func='token_sort_ratio',min_ratio=90, allow_anonymous=True, func_anonymous='ratio', anonymous_must_be_equal=False,split_on_punc=[':',';','.','!','?'],modernize_spellings=False):
		from fuzzywuzzy import fuzz
		from lltk import tools
		func=getattr(fuzz,func)
		func_anonymous = func if not func_anonymous else getattr(fuzz,func_anonymous)
		if modernize_spellings:
			from lltk.tools import get_spelling_modernizer
			spelling_d=get_spelling_modernizer()

		## HACK
		#id1_done_before = set([d['id1'] for d in tools.readgen('data.duplicates.ESTC.bytitle.authorless-part1.txt')])
		#id1_done_before.remove(d['id1']) # remove last one
		#print len(id1_done_before)
		##

		def filter_title(title):
			for p in split_on_punc:
				title=title.split(p)[0]
			if modernize_spellings:
				return ' '.join([spelling_d.get(w,w) for w in title.split()])
			return title.lower()

		def writegen():
			texts = self.texts()
			for i1,t1 in enumerate(texts):
				print('>>',i1,len(texts),'...')
				for i2,t2 in enumerate(texts):
					if i1>=i2: continue
					title1=t1.title
					title2=t2.title
					dx={}
					dx['id1'],dx['id2']=t1.id,t2.id
					#dx['title1'],dx['title2']=title1,title2
					#dx['author1'],dx['author2']=t1.author,t2.author
					ratio=func(title1,title2)
					if min_ratio>ratio: continue
					#dx['match_ratio']=ratio
					yield dx

		def writegen_withinauthor():
			from collections import defaultdict
			author2texts=defaultdict(list)
			for t in self.texts(): author2texts[t.author]+=[t]
			NumTexts=len(self.texts())
			numauthors=len(author2texts)
			numtextsdone=0
			done=set()

			## HACK
			#done|=done_before
			##

			for ai1,author in enumerate(sorted(author2texts, key=lambda _k: len(author2texts[_k]), reverse=True)):
				texts=author2texts[author]
				#comparisons = set([(t1.id,t2.id) for t1 in texts for t2 in texts if t1.id<t2.id])
				#print len(comparisons)
				if not author and not allow_anonymous: continue

				title2texts=defaultdict(list)
				for t in texts: title2texts[filter_title(t.title)]+=[t]
				texts_unique_title=[x[0] for x in list(title2texts.values())]
				for title in title2texts:
					for i1,t1 in enumerate(title2texts[title]):
						print('>> a) finished {0} of {1} texts. currently on author #{2} of {3}, who has {4} texts. ...'.format(numtextsdone,NumTexts,ai1+1,numauthors,len(texts)))
						for i2,t2 in enumerate(title2texts[title]):
							if t1.id>=t2.id: continue
							dx={}
							dx['id1'],dx['id2']=t1.id,t2.id
							#dx['title1'],dx['title2']=t1.title,t2.title
							#dx['author1'],dx['author2']=t1.author,t2.author
							#dx['match_ratio']=100
							#yield dx
							#done|={tuple(sorted([t1.id,t2.id]))}
							done|={(t1.id,t2.id)}
						numtextsdone+=1

				if not author and anonymous_must_be_equal: continue

				for i1,t1 in enumerate(texts_unique_title):
					print('>> b) finished {0} of {1} texts. currently on author #{2} of {3} [{5}], who has {4} texts. ...'.format(numtextsdone,NumTexts,ai1+1,numauthors,len(texts),author.encode('utf-8',errors='ignore')))

					### HACK -->
					if numtextsdone<250000:
						numtextsdone+=1
						continue
					#if t1.id in id1_done_before: continue
					###


					title1=filter_title(t1.title)
					nuncmp=0
					for i2,t2 in enumerate(texts_unique_title):
						if t1.id>=t2.id: continue
						#print (t1.id,t2.id), (t1.id,t2.id) in done, (t2.id,t1.id) in done
						if (t1.id,t2.id) in done:
							print('>> skipping')
							continue
						nuncmp+=1
						#print i1,i2,nuncmp,'..'
						title2=filter_title(t2.title)
						ratio=func(title1,title2) if author else func_anonymous(title1,title2)

						if min_ratio>ratio: continue

						for t1_x in title2texts[title1]:
							for t2_x in title2texts[title2]:
								if t1_x.id>=t2_x.id: continue
								if (t1_x.id,t2_x.id) in done:
									print('>> skipping')
									continue
								dx={}
								dx['id1'],dx['id2']=t1_x.id,t2_x.id
								dx['title1'],dx['title2']=t1_x.title,t2_x.title
								dx['author1'],dx['author2']=t1_x.author,t2_x.author
								dx['match_ratio']=ratio
								yield dx
					numtextsdone+=1
				break

		from lltk import tools
		if not within_author:
			tools.writegen('data.duplicates.%s.bytitle.txt' % self.name, writegen)
		else:
			tools.writegen('data.duplicates.%s.bytitle.txt' % self.name, writegen_withinauthor)



	def rank_duplicates(self,threshold_range=None,path_hashes=None, suffix_hashes=None,sample=False,hashd=None,ofn=None):
		if not threshold_range:
			import numpy as np
			threshold_range=np.arange(0.25,1,0.05)

		import networkx as nx,random
		G=nx.Graph()

		threshold_range=[float(x) for x in threshold_range]
		print('>> getting duplicates for thresholds:',threshold_range)

		for threshold in reversed(threshold_range):
			threshold_count=0
			print('>> computing LSH at threshold =',threshold,'...')
			lsh,hashd=self.lsh(threshold=threshold,path_hashes=path_hashes,suffix_hashes=suffix_hashes,hashd=hashd)
			hash_keys = list(hashd.keys())
			random.shuffle(hash_keys)
			for idx1 in hash_keys:
				if sample and threshold_count>=sample:
					break
				r=lsh.query(hashd[idx1])
				r.remove(idx1)
				for idx2 in r:
					if not G.has_edge(idx1,idx2):
						G.add_edge(idx1,idx2,weight=float(threshold))
						threshold_count+=1
			print(G.order(), G.size())
			print()

		def writegen1():
			for a,b,d in sorted(G.edges(data=True),key=lambda tupl: -tupl[-1]['weight']):
				meta1=self.textd[a].meta
				meta2=self.textd[b].meta
				weight=d['weight']
				dx={'0_jacc_estimate':weight}
				dx['1_is_match']=''
				for k,v in list(meta1.items()): dx[k+'_1']=v
				for k,v in list(meta2.items()): dx[k+'_2']=v
				yield dx

		def writegen2():
			g=G
			gg = sorted(nx.connected_components(g), key = len, reverse=True)
			for i,x in enumerate(gg):
				print('>> cluster #',i,'has',len(x),'texts...')
				#if len(x)>50000: continue
				xset=x
				x=list(x)
				idx2maxjacc = {}
				for idx in x:
					neighbors = g.neighbors(idx)
					weights = [g.edge[idx][idx2]['weight'] for idx2 in neighbors]
					idx2maxjacc[idx]=max(weights)

				x.sort(key=lambda idx: (-idx2maxjacc[idx],self.metad[idx]['year']))
				for idx in x:
					dx={'0_cluster_id':i}
					dx['1_highest_jacc']=idx2maxjacc[idx]
					dx['2_is_correct']=''
					metad=self.textd[idx].meta
					dx['3_year']=metad.get('year','')
					dx['4_title']=metad.get('fullTitle','')
					dx['5_author']=metad.get('author','')
					dx['6_id']=metad.get('id','')
					for k,v in list(metad.items()): dx[k]=v
					yield dx

		ofn='data.duplicates.%s.txt' % self.name if not ofn else ofn
		from lltk import tools
		tools.writegen(ofn, writegen1)
		return G

	def hashd(self,path_hashes=None,suffix_hashes=None,text_ids=None):
		from datasketch import MinHash
		import six.moves.cPickle

		if not suffix_hashes: suffix_hashes='.hash.pickle'
		if not path_hashes: path_hashes=self.path_txt.replace('_txt_','_hash_')

		#print '>> loading hashes from files...'
		text_ids=self.text_ids if not text_ids else text_ids

		hashd={}
		for i,idx in enumerate(text_ids):
			#if i and not i%1000:
				#print '>>',i,'..'
				#break
			fnfn=os.path.join(path_hashes, idx+suffix_hashes)
			try:
				hashval=six.moves.cPickle.load(open(fnfn))
			except IOError as e:
				hashobj=self.textd[idx].minhash
				hashval=hashobj.digest()
			mh=MinHash(seed=1111, hashvalues=hashval)
			hashd[idx]=mh

		return hashd

	def lsh(self,threshold=0.5,path_hashes=None,suffix_hashes=None,hashd=None):
		from datasketch import MinHashLSH

		hashd=self.hashd(path_hashes=path_hashes,suffix_hashes=suffix_hashes) if not hashd else hashd
		lsh = MinHashLSH(threshold=threshold, num_perm=128)
		for idx in hashd: lsh.insert(idx,hashd[idx])
		return lsh,hashd





	### SUBCORPORA FILES

	def save_subcorpus_files(self,odir,by_fields=['genre','quartercentury'],dry_run=False,min_num_words=1000000,genres=['non','fic']):
		subcorpus2texts={}

		for text in self.texts():
			if not os.path.exists(text.fnfn_txt): continue
			subcorpus = '_'.join([getattr(text,x) for x in by_fields])
			if not subcorpus in subcorpus2texts: subcorpus2texts[subcorpus]=[]
			subcorpus2texts[subcorpus]+=[text]

		for sc in sorted(subcorpus2texts.keys()):
			nw=sum(t.num_words for t in subcorpus2texts[sc] if t.num_words)
			if nw<min_num_words:
				del subcorpus2texts[sc]
				continue
			print(sc, nw)

		if dry_run: return

		if not os.path.exists(odir): os.makedirs(odir)
		for sc in sorted(subcorpus2texts):
			fnfn=os.path.join(odir,sc+'.txt')
			print('>> saving:',fnfn,'...')
			of=codecs.open(fnfn,'w',encoding='utf-8')
			for text in subcorpus2texts[sc]:
				of.write(text.text_plain + u'\n\n\n\n\n\n\n\n\n\n')

	def save_subcorpus_files_controlling_author(self,odir,by_fields=['medium','quartercentury'],dry_run=False,min_num_words_per_author=50000, max_num_words_per_author=100000, min_num_words_per_subcorpus=1000000):
		subcorpus2author2texts={}

		for text in self.texts():
			if not os.path.exists(text.fnfn_txt): continue
			subcorpus = tuple([getattr(text,x) for x in by_fields])
			names=[x.strip() for x in text.author.split(',') if x.strip()]
			author=', '.join(names[:2])
			if not author: continue
			if not subcorpus: continue

			if not subcorpus in subcorpus2author2texts: subcorpus2author2texts[subcorpus]={}
			if not author in subcorpus2author2texts[subcorpus]: subcorpus2author2texts[subcorpus][author]=[]
			subcorpus2author2texts[subcorpus][author]+=[text]

		for sc in sorted(subcorpus2author2texts.keys()):
			nw_sc=0
			na_sc=0
			for author in sorted(subcorpus2author2texts[sc].keys()):

				#print sc,author,type(subcorpus2author2texts), type(subcorpus2author2texts[sc])
				author_texts=subcorpus2author2texts[sc][author]
				nw=sum([t.num_words for t in author_texts])
				if nw<min_num_words_per_author:
					del subcorpus2author2texts[sc][author]
					continue
				na_sc+=1
				nw_sc+=max_num_words_per_author if nw>max_num_words_per_author else nw
				#print '\t',sc,author,nw

			#if nw_sc<min_num_words_per_subcorpus:
			#	del subcorpus2author2texts[sc]
			#	continue

			print(sc,'\t',na_sc,'\t',nw_sc)

		if dry_run: return

		#"""
		def writegen():
			import multiprocessing as mp
			pool=mp.Pool(processes=4)
			results = [pool.apply_async(do_save_subcorpus,args=(os.path.join(odir,'_'.join(subcorpus)+'.txt'), subcorpus2author2texts[subcorpus], subcorpus, 1000, max_num_words_per_author)) for subcorpus in sorted(subcorpus2author2texts)]
			for gen in results:
				for x in gen.get():
					yield x

		from lltk import tools
		tools.writegen('subcorpus-metadata.txt', writegen)
		"""
		for sc in subcorpus2author2texts:
			do_save_subcorpus(os.path.join(odir,'_'.join(subcorpus)+'.txt'), subcorpus2author2texts[sc], subcorpus)
		"""


	## ETC

	def modernize_spelling(self,word):
		if not hasattr(self,'spelling_modernizer'):
			from lltk.tools import get_spelling_modernizer
			self.spelling_modernizer = get_spelling_modernizer()
		return self.spelling_modernizer.get(word,word)





## CORPUS META

class CorpusMeta(Corpus):
	TYPE='CorpusMeta'

	def __init__(self,name,corpora,**kwargs):
		self.name=name
		self.path = os.path.dirname(__file__)
		self.corpora=corpora
		self.corpus2rank=dict([(c,i+1) for i,c in enumerate(corpora)])
		self.corpus2metad={}
		for k,v in list(kwargs.items()):
			setattr(self,k,v)
		self.path_freq_table={}
		#self._metad={}
		#self._meta=[]
		looper = tqdm(self.corpora,desc='>> loading subcorpora')
		for c in looper:
			cname=c.name if hasattr(c,'name') else c.__class__.__name__
			looper.set_description(f'>> loading {cname}')
			self.corpus2metad[cname]=c.metad
			#for idx,dx in c.metad.items():
			#	dx['corpus']=c.name
			#	self._metad[idx]=dx
			#	self._meta+=[dx]

		self.merge()


	@property
	def meta(self):
		if not hasattr(self,'_meta'):
			self._meta=[t.meta for t in self.texts()]
		return self._meta

	def merge(self):
		self._texts=texts=[]
		self._text_ids=text_ids=[]
		ids=set()
		for c in self.corpora:
			for t in c.texts():
				if t.is_metatext:
					worth_adding=True
					t.texts.sort(key=lambda _t: self.corpus2rank[_t.corpus.name])
					for t2 in t.texts:
						idx=(c.name,t2.id)
						if idx in ids:
							worth_adding=False
							continue
						ids|={idx}
					if worth_adding:
						texts+=[t]
						text_ids+=[t.id]
				else:
					idx=(c.name,t.id)
					if idx in ids: continue
					ids|={idx}
					texts+=[t]
					text_ids+=[t.id]

	"""
	def save_metadata(self,ofn=None):
		#return super(CorpusMeta,self).save_metadata(ofn=ofn,num_words=False,ocr_accuracy=False)
		from lltk import tools
		if not ofn:
			timestamp=tools.now().split('.')[0]
			ofn=os.path.join(self.path,'corpus-metadata.%s.%s.txt' % (self.name,timestamp))

		tools.write2(ofn,self.meta)
	"""



## Corpus in Sections
class Corpus_in_Sections(Corpus):
	TYPE='Corpus_in_Sections'

	def __init__(self,name,parent,col_id='id',**kwargs):
		self.name=name
		self.parent=parent
		self.col_id=col_id
		for k,v in list(kwargs.items()):
			setattr(self,k,v)
		#super(Corpus_in_Sections,self).__init__(name,self.PATH_XML,self.PATH_INDEX,self.EXT_XML,path_header=self.PATH_HEADER,path_metadata=self.PATH_METADATA,path_freq_table=self.PATH_FREQ_TABLE,paths_text_data=self.PATHS_TEXT_DATA)
		#self.path = os.path.dirname(__file__)

	def texts(self):
		if not hasattr(self,'_texts'):
			#print(self.path_metadata, os.path.exists(self.path_metadata))
			if os.path.exists(self.path_metadata):
				self.meta
				for t in self._texts:
					yield t
			else:
				for t in self.parent.texts():
					for ts in t.sections:
						yield ts
						ts.unload()
		else:
			for t in self._texts:
				yield t

	@property
	def path(self): return self.parent.path

	@property
	def path_xml(self): return self.parent.path_xml

	@property
	def path_txt(self): return self.parent.path_txt

	@property
	def ext_xml(self): return self.parent.ext_xml

	@property
	def ext_txt(self): return self.parent.ext_txt



	#@property
	#def path_freq_table(self): return self.parent.path_freq_table


	def get_section(self,idx):
		sidx=idx.split('/')[-1]
		text_id='/'.join(idx.split('/')[:-1])
		return self.parent.textd[text_id].get_section(idx)

	def create_texts_from_meta(self):
		if not hasattr(self,'TEXT_SECTION_CLASS') or not self.TEXT_SECTION_CLASS:
			from lltk.text import TextSection
			self.TEXT_SECTION_CLASS=TextSection

		self._texts=[]
		for dx in self._meta:
			#print dx
			section_num=int(dx['id'].split('/section')[1])
			ts=self.TEXT_SECTION_CLASS(text=self.parent.textd[dx['text_id']],section_num=section_num,section_type=dx['section_type'])
			ts._meta=dx
			self._texts+=[ts]

	@property
	def meta1(self):
		if not hasattr(self,'_meta'):
			if os.path.exists(self.path_metadata):
				self.load_metadata(minimal=True)
			else:
				return [t.get_meta_from_file for t in self.texts()]
		return self._meta

	@property
	def meta(self):
		return [t.get_metadata() for t in self.texts]

	@property
	def path_metadata(self): return os.path.join(self.path,'corpus-metadata.%s.txt' % self.name)

	@property
	def path_mfw(self): return self.parent.path_mfw




## CorpusGroups
class CorpusGroups(Corpus):
	TYPE='CorpusGroups'

	def __init__(self,corpus,texts=None,name=None):
		self.corpus=corpus
		self._groups={}
		self._desc=[]
		self._texts=texts
		self.name=corpus.name if not name else name

	@property
	def log(self):
		header='## Groups for corpus %s ##\n# texts in corpus: %s\n' % (self.corpus.name,self.corpus.num_texts)
		body='\n'.join(self._desc)
		return header+body

	@property
	def textid2group(self):
		if hasattr(self,'_textid2group'): return self._textid2group
		self._textid2group=t2g={}
		for groupname,grouptexts in sorted(self.groups.items()):
			for gtext in grouptexts:
				t2g[gtext.id]=groupname
		return t2g

	@property
	def group2textid(self):
		g2id={}
		for group_name,group_texts in  sorted(self.groups.items()):
			for t in group_texts:
				g2id[group_name]=t.id
		return g2id

	def group_by_author_dob(self,yearbin=10,yearplus=0,texts=None):
		return self.group_by_year(year_key='author_dob',yearbin=yearbin,yearplus=yearplus,texts=texts)

	def group_by_author_at_30(self,yearbin=10,texts=None):
		return self.group_by_author_dob(yearbin=yearbin,yearplus=30,texts=texts)

	def prune_groups(self,min_len=None,min_group=None,max_group=None):
		for g,gl in sorted(self.groups.items()):
			if min_len and len(gl)<min_len:
				self._desc+=['>> deleted group %s because its length (%s) < min_len (%s)' % (g,len(gl),min_len)]
				if g in self.groups: del self.groups[g]

			if min_group and str(g)<str(min_group):
				self._desc+=['>> deleted group %s because its name is < minimum of %s' % (g,min_group)]
				if g in self.groups: del self.groups[g]

			if max_group and str(g)>str(max_group):
				self._desc+=['>> deleted group %s because its name is > maximum of %s' % (g,max_group)]
				if g in self.groups: del self.groups[g]

		if min_group:
			self.groups

	def group_by_year(self,year_key='year',yearbin=10,yearplus=0,texts=None,toreturn=False):
		# function to get 'decade' from year
		def get_dec(yr,res=25,plus=30):
			if type(yr)==int:
				pass
			else:
				yr = ''.join([x for x in yr if x.isdigit()])
				if len(yr)!=4: return 0
			year=int(yr)
			year+=plus
			dec=year//int(res)*int(res)
			return dec

		# group texts
		periodd={}
		if not texts:
			if self._texts:
				texts=self._texts
			else:
				texts=self.corpus.texts()

		for text in texts:
			period = get_dec(text.meta.get(year_key,''), res=yearbin, plus=yearplus)
			period_str = '%s-%s' % (period, period+yearbin-1)
			if not period_str in periodd: periodd[period_str]=[]
			periodd[period_str]+=[text]
		self._groups=periodd

		self._desc=['>> grouped by {year_key}{yearplus} into {yearbin}-year long bins'.format(year_key=year_key,yearbin=yearbin,yearplus=' (+%s)' % yearplus if yearplus else '')]
		self._desc+=['\t'+self.table_of_counts.replace('\n','\n\t')]
		if toreturn: return self._groups

	@property
	def table_of_counts(self):
		o=[]
		for group in sorted(self.groups):
			o+=['%s\t%s' % (group, len(self.groups[group]))]
		return '\n'.join(o)

	def save_id2group(self,ofn=None):
		if not ofn: ofn='data.groups.%s.id2group.txt' % self.corpus.name
		ld=[{'id':idx, 'group':group} for idx,group in list(self.textid2group.items())]
		tools.write(ofn,ld)


	def save_as_ids(self,ofolder=None):
		return self.save_as_paths(ofolder=ofolder,path_key='id')

	def save_as_paths_xml(self,ofolder=None):
		return self.save_as_paths(ofolder=ofolder,path_key='path_xml')

	def save_as_paths(self,ofolder=None,path_key='path_txt'):
		#if not ofolder: ofolder='groups_%s_%s' % (self.corpus.name, tools.now())
		from lltk import tools
		if not ofolder: ofolder='groups_%s' % self.corpus.name
		ofolder2=os.path.join(ofolder,'groups')
		if not os.path.exists(ofolder): os.makedirs(ofolder)
		if not os.path.exists(ofolder2):
			os.makedirs(ofolder2)
		else:
			for fn in os.listdir(ofolder2):
				if fn.endswith('.txt'):
					os.remove(os.path.join(ofolder2,fn))

		self._desc+=['>> saving groups to %s' % ofolder]
		self._desc+=['\t'+self.table_of_counts.replace('\n','\n\t')]
		with codecs.open(os.path.join(ofolder,'log.txt'),'w') as lf: lf.write(self.log)      # write log
		for groupname,grouptexts in sorted(self.groups.items()):
			#print '>> saving group "%s" with %s texts...' % (groupname,len(grouptexts))
			ofnfn=os.path.join(ofolder2,str(groupname)+'.txt')
			with codecs.open(ofnfn,'w',encoding='utf-8') as of:
				for t in grouptexts:
					x=getattr(t,path_key)
					of.write(x+'\n')

	@property
	def texts(self):
		texts=[]
		for group,group_texts in sorted(self.groups.items()):
			texts+=group_texts
		return texts

	@property
	def groups(self):
		return self._groups


### FUNCTIONS

def do_metadata_text(i,text,num_words=False,ocr_accuracy=False):
	global ENGLISH
	md=text.meta
	print('>> starting:',i, text.id, len(md),'...')
	if num_words or ocr_accuracy:
		print('>> getting freqs:',i,text.id,'...')
		freqs=text.freqs()
		print('>> computing values:',i,text.id,'...')
		if num_words:
			md['num_words']=sum(freqs.values())
		if ocr_accuracy:
			num_words_recognized = sum([v for k,v in list(freqs.items()) if k[0] in ENGLISH])
			print(md['num_words'], num_words_recognized)
			md['ocr_accuracy'] = num_words_recognized / float(md['num_words']) if float(md['num_words']) else 0.0
	print('>> done:',i, text.id, len(md))
	return [md]


def do_save_subcorpus(ofnfn,author2texts,subcorpus,slice_len=1000,num_words=50000):
	f=codecs.open(ofnfn,'w',encoding='utf-8')
	numlines=0
	numwords=0
	old=[]
	from lltk import tools
	for author in sorted(author2texts):
		texts=author2texts[author]
		author_words=[]
		for text in texts:
			author_words+=text.words

		num_words_author = len(author_words)
		num_words_author_included=0

		author_slices = tools.slice(author_words,slice_length=slice_len,runts=False)
		print(author,'has',len(author_words),'and',len(author_slices),'slices', end=' ')
		random.shuffle(author_slices)
		author_slices = author_slices[:(num_words/slice_len)]
		print('normalized to',len(author_slices),'slices')
		for slicex in author_slices:
			f.write(' '.join(slicex)+'\n')
			numlines+=1
			numwords+=len(slicex)
			num_words_author_included+=len(slicex)
		f.write('\n')

		md={'author':author, 'subcorpus':subcorpus, 'num_words':num_words_author, 'num_words_included':num_words_author_included}
		old+=[md]
	f.close()
	print('DONE!')
	print(subcorpus)
	print(numlines)
	print(numwords)
	print()
	return old

def skipgram_do_text(text,i=0,n=10):
	from lltk import tools
	print(i, text.id, '...')
	from nltk import word_tokenize
	words=word_tokenize(text.text_plain)
	words=[w for w in words if True in [x.isalpha() for x in w]]
	word_slices = tools.slice(words,slice_length=n,runts=False)
	return word_slices

def skipgram_do_text2(text_i,n=10,lowercase=True):
	text,i=text_i
	import random
	print(i, text.id, '...')
	from lltk import tools
	words=text.text_plain.strip().split()
	words=[tools.noPunc(w.lower()) if lowercase else tools.noPunc(w) for w in words if True in [x.isalpha() for x in w]]
	#sld=[]
	for slice_i,slice in enumerate(tools.slice(words,slice_length=n,runts=False)):
		sdx={'id':text.id, 'random':random.random(), 'skipgram':slice, 'i':slice_i}
		yield sdx
		#sld+=[sdx]
	#return sld

def skipgram_save_text(text_i_mongotup,n=10,lowercase=True,batch_size=1000):
	text,i,mongotuple = text_i_mongotup
	from pymongo import MongoClient
	c=MongoClient()
	db1name,db2name=mongotuple
	db0=getattr(c,db1name)
	db=getattr(db0,db2name)

	sld=[]
	for sdx in skipgram_do_text2((text,i),n=n,lowercase=lowercase):
		sld+=[sdx]
		if len(sld)>=batch_size:
			db.insert(sld)
			sld=[]
	if len(sld): db.insert(sld)
	c.close()
	return True


def dict_factory(cursor, row):
	d = {}
	for idx, col in enumerate(cursor.description):
		d[col[0]] = row[idx]
	return d




def tokenize_text(text):
	from nltk import word_tokenize
	txt=text.text_plain().lower().strip()
	toks=word_tokenize(txt)
	toks=[tok for tok in toks if tok and tok[0].isalpha()]
	return toks

def save_tokenize_text(text,ofolder=None,force=False):
	import os
	if not ofolder: ofolder=os.path.join(text.corpus.path, 'freqs', text.corpus.name)
	ofnfn=os.path.join(ofolder,text.id+'.json')
	opath = os.path.split(ofnfn)[0]
	if not os.path.exists(opath): os.makedirs(opath)
	if not force and os.path.exists(ofnfn) and os.stat(ofnfn).st_size:
		print('>> already tokenized:',text.id)
		return
	else:
		print('>> tokenizing:',text.id,ofnfn)

	from collections import Counter
	import json,codecs
	toks=tokenize_text(text)
	tokd=dict(Counter(toks))
	with codecs.open(ofnfn,'w',encoding='utf-8') as of:
		json.dump(tokd,of)
	#assert 1 == 2

def get_freq_async(text):
	return text.freqs()




class Bunch(object):
	DEFAULT=''
	def __init__(self, **adict):
		self.__dict__.update(adict)
	def __getattr__(self,attr):
		return self.__dict__.get(attr,self.DEFAULT)


def start_new_corpus(attrs):
	from argparse import Namespace
	#ns = Bunch(**attrs)
	ns=Namespace(**attrs)

	manifeststr="""[{name}]
name = {name}
id = {id}
desc = {desc}
link = {link}
path_root = {path_root}
path_txt = {path_txt}
path_xml = {path_xml}
path_metadata = {path_metadata}
path_python = {path_python}
class_name = {class_name}
""".format(**attrs)

	print('-'*40)


	### WRITE MANIFEST
	path_manifest = PATH_MANIFEST_USER if os.path.exists(PATH_MANIFEST_USER) else PATH_MANIFEST
	with open(path_manifest) as f:
		global_manifest_txt = f.read()

	if not '[%s]' % ns.name in global_manifest_txt:
		print('>> Saving to corpus manifest [%s]' % path_manifest)
		with open(path_manifest,'a+') as f:
			f.write('\n\n'+manifeststr+'\n\n')
		#print(manifeststr)

	## create new data folders
	ns.path_root = os.path.join(PATH_CORPUS,ns.path_root) if not os.path.isabs(ns.path_root) else ns.path_root
	ns.path_txt = os.path.join(ns.path_root,ns.path_txt) if not os.path.isabs(ns.path_txt) else ns.path_txt
	ns.path_xml = os.path.join(ns.path_root,ns.path_xml) if not os.path.isabs(ns.path_xml) else ns.path_xml
	ns.path_metadata = os.path.join(ns.path_root,ns.path_metadata) if not os.path.isabs(ns.path_metadata) else ns.path_metadata
	ns.path_metadata_dir,ns.path_metadata_fn=os.path.split(ns.path_metadata)




	#check_make_dirs([ns.path_root,ns.path_txt,ns.path_xml,ns.path_metadata_dir],consent=True)


	### Create new code folder
	ns.path_python=os.path.join(ns.path_root,ns.path_python) if not os.path.isabs(ns.path_python) else ns.path_python
	path_python_dir,path_python_fn=os.path.split(ns.path_python)
	python_module=os.path.splitext(path_python_fn)[0]
	#if not path_python_dir: path_python_dir=os.path.join(PATH_TO_CORPUS_CODE,python_module)
	#if not path_python_dir: path_python_dir=os.path.abspath(os.path.join(ns.path_root,python_module))

	if not os.path.exists(path_python_dir):
		print('>> creating:',path_python_dir)
		os.makedirs(path_python_dir)

	python_fnfn=os.path.join(path_python_dir,path_python_fn)
	#python_fnfn2=os.path.join(path_python_dir,'__init__.py')
	python_ifnfn=os.path.join(PATH_TO_CORPUS_CODE,'default','new_corpus.py')

	#if not os.path.exists(python_fnfn) and not os.path.exists(python_fnfn2) and os.path.exists(python_ifnfn):
	#if not os.path.exists(python_fnfn) and os.path.exists(python_ifnfn):
	ofn=tools.iter_filename(python_fnfn)
	if os.path.exists(python_ifnfn):
		#with open(python_fnfn,'w') as of, open(python_fnfn2,'w') as of2, open(python_ifnfn) as f:
		with open(ofn,'w') as of, open(python_ifnfn) as f:
			itxt=f.read()
			itxt=itxt.replace('NewCorpus',ns.class_name)

			of.write(itxt)
			#of2.write('from .%s import *\n' % python_module)
			print('>> creating:',python_fnfn)



		#print('>> creating:',ns.path_metadata)
		#from pathlib import Path
		#Path(ns.path_metadata).touch()

	print('\n>> Corpus finalized with the following manifest configuration:')
	print(manifeststr)




def get_python_path(path_python,path_root):
	if not os.path.isabs(path_python):
		#cd['path_python'] = path_python=os.path.join(PATH_TO_CORPUS_CODE,path_python)  # if not path_python.startswith(os.path.sep) else path_python
		path_python1=os.path.join(path_root,path_python)  # if not path_python.startswith(os.path.sep) else path_python
		module_name=os.path.splitext(os.path.basename(path_python))[0]
		path_python2=os.path.join(path_root,module_name,path_python)  # if not path_python.startswith(os.path.sep) else path_python
		path_python3=os.path.join(PATH_TO_CORPUS_CODE,path_python)
		path_python4=os.path.join(PATH_TO_CORPUS_CODE,module_name,path_python)

		for ppath in [path_python1,path_python2,path_python3,path_python4]:
			#print('?',ppath,os.path.exists(ppath))
			if os.path.exists(ppath):
				return os.path.abspath(ppath)
	else:
		return path_python




def load_corpus(name_or_id,sources=[PATH_CORPUS,''],**input_kwargs):
	from lltk.tools import get_path_abs,camel2snake_case

	# Set defaults
	default_kwargs=MANIFEST_DEFAULTS
	#print('>> loading corpus:',name_or_id)

	### Configs
	manifest_kwargs={}
	manifest=load_manifest()
	manifest_byid = dict((cd['id'],cd) for cd in manifest.values())
	manifest_kwargs=manifest_byid.get(name_or_id)
	if not manifest_kwargs: manifest_kwargs=manifest.get(name_or_id)
	#print(manifest_kwargs)
	opts = Bunch(**dict(list(default_kwargs.items()) + list(manifest_kwargs.items()) + list(input_kwargs.items())))
	opts.path_root = get_path_abs(opts.path_root,sources=sources)
	if not opts.path_root: opts.path_root=os.path.join(PATH_CORPUS,opts.id)


	#pprint(opts.__dict__)

	opts.path_python = get_python_path(opts.path_python,opts.path_root)
	if not opts.path_python:
		#print('!! No path_python set.')
		return

	opts.module_name = os.path.splitext(os.path.basename(opts.path_python))[0]
	#print(module_name,path_python,os.path.exists(path_python))
	try:
		import imp
		#print(f'>> loading {opts.module_name} from {opts.path_python}')
		module = imp.load_source(opts.module_name, opts.path_python)
	except ValueError as e:#FileNotFoundError:
		return None

	opts.class_name = opts.class_name if opts.class_name else opts.name.replace(' ','')
	class_class = getattr(module,opts.class_name)
	class_obj = class_class()


	# Set as attributes
	for opt_name,opt_val in sorted(opts.__dict__.items()):
		# Set abs path
		if opt_name!='path_root' and opt_name.startswith('path_') and opt_val and type(opt_val)==str:
			#opt_val = os.path.join(path_root,opt_val) if not os.path.isabs(opt_val) else opt_val
			opt_val_abs = get_path_abs(opt_val,sources=[opts.path_root,'.'])
			if not opt_val_abs: opt_val=os.path.join(opts.path_root,opt_val)
			opt_val = opt_val_abs if opt_val_abs else opt_val
		#if opt_val and opt_val!=default_kwargs.get(opt_name): print(f'   {opt_name} = {opt_val}')
		setattr(class_obj,opt_name,opt_val)
		setattr(opts,opt_name,opt_val)

	class_obj.opts = opts
	return class_obj
































# Add new name for function
load = load_corpus
#################################################################
