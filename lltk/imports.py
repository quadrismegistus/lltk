KEYSERVER_URL = 'http://128.232.229.63:7799/key/'
JSONPREF='>>>> JSON:'
from collections import defaultdict
DATABOX=defaultdict(dict)

import sys,os,warnings,shutil
from tracemalloc import stop
warnings.filterwarnings('ignore')
import multiprocessing as mp
try:
	mp.set_start_method('fork')
except RuntimeError:
	pass
# default num proc is?
mp_cpu_count=mp.cpu_count()
DEFAULT_NUM_PROC = mp_cpu_count - 2
if mp_cpu_count==1: DEFAULT_NUM_PROC=1
if mp_cpu_count==2: DEFAULT_NUM_PROC=2
if mp_cpu_count==3: DEFAULT_NUM_PROC=2

# Paths
HOME = os.path.expanduser('~')
LLTK_ROOT = PATH_HERE = ROOT = os.path.dirname(os.path.realpath(__file__))
PATH_BASE_CONF=os.path.join(HOME,'.lltk_config')
PATH_DEFAULT_LLTK_HOME = os.path.join(HOME,'lltk_data')
PATH_DEFAULT_CONF=os.path.abspath(os.path.join(PATH_DEFAULT_LLTK_HOME,'config_default.txt'))
PATH_LLTK_CONFIG_DIR = os.path.abspath(os.path.join(PATH_DEFAULT_LLTK_HOME,'config'))
PATH_LLTK_CONFIG_USR = os.path.join(PATH_DEFAULT_LLTK_HOME,'.user.json')
PATH_SECURE_BUNDLE=os.path.join(PATH_LLTK_CONFIG_DIR,'secure-connect-lltk.zip')
PATH_ASTRA_ENV=os.path.abspath(os.path.join(PATH_DEFAULT_LLTK_HOME,'config','.env'))
META_KEY_SEP='__'
CACHE_DB=True
CACHE_JSON=False
MATCHRELNAME='rdf:type'
REMOTE_SOURCES=['hathi','wikidata','goodreads','isbn']
REMOTE_DEFAULT=False
REMOTE_REMOTE_DEFAULT = True


MINIMETAD={
    'author':['author'],
    'title':['title'],
    'year':['year','date']
}

#'worldcat','goodreads']
# REMOTE_SOURCES=['worldcat']
# REMOTE_SOURCES=['hathi']

# Get tools
LOG_TO_SCREEN = False
LOG_TO_FILE = True

BAD_PKL_KEYS=set()
from lltk.tools.logs import *
from lltk.tools.tools import *


### IMPORTANT: SET WHERE LLTK ROOT IS BASED:
PATH_LLTK_HOME = PATH_DEFAULT_LLTK_HOME
### CURRENTLY IN ~/lltk_data
PATH_CORPUS = config.get('PATH_TO_CORPORA', os.path.join(PATH_LLTK_HOME,'corpora') )
PATH_CORPUS_ZIP = os.path.join(PATH_CORPUS, 'lltk_corpora')
PATH_TO_CORPUS_CODE = config.get('PATH_TO_CORPUS_CODE', os.path.join(PATH_HERE,'corpus') )
PATH_TO_DATA_CODE = os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..','data'))
PATH_LLTK_CODE_HOME = os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..'))
# PATH_LLTK_REPO = os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..','..'))
PATH_LLTK_HOME_DATA = PATH_LLTK_DATA = os.path.join(PATH_LLTK_HOME,'data')
PATH_LLTK_KEYS = os.path.join(PATH_LLTK_DATA,'keys')

PATH_LLTK_DB = os.path.join(PATH_LLTK_DATA,'db')
PATH_LLTK_DB_FN = os.path.join(PATH_LLTK_DB,'lltk')
PATH_LLTK_MATCHES = os.path.join(PATH_LLTK_DATA,'rels')
PATH_LLTK_DB_MATCHES = os.path.join(PATH_LLTK_MATCHES, 'db')
PATH_LLTK_DB_ENGINE = 'rdict'
PATH_LLTK_ZODB = os.path.join(PATH_LLTK_DB,'zodb.fs')

DEFAULT_PATH_TO_MANIFEST = os.path.join(PATH_LLTK_HOME,'manifest.txt')
PATH_MANIFEST=os.path.join(PATH_TO_CORPUS_CODE,'manifest.txt')
PATH_MANIFEST_USER = config.get('PATH_TO_MANIFEST','')
PATH_MANIFEST_USER_LAB = PATH_MANIFEST_USER.replace('.txt','_lab.txt')
PATH_MANIFEST_USER_SHARE = PATH_MANIFEST_USER.replace('.txt','_share.txt')


PATH_MANIFESTS = remove_duplicates([
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
PATH_DEFAULT_DATA = os.path.join(PATH_LLTK_HOME_DATA,'default.zip')
URL_DEFAULT_DATA='https://www.dropbox.com/s/cq1xb85yaysezx4/lltk_default_data.zip?dl=1'
PATH_MANIFEST_GLOBAL = os.path.join(ROOT,'corpus','manifest.txt')
PATH_LLTK_REPO=os.path.abspath(os.path.join(LLTK_ROOT,'..'))
URL_DEFAULT_DATA='https://www.dropbox.com/s/cq1xb85yaysezx4/lltk_default_data.zip?dl=1'
PATH_LLTK_LOG_FN = os.path.join(PATH_LLTK_HOME, 'logs','debug.log')
DIR_SECTION_NAME='sections'
TEXT_META_DEFAULT = {'id':'', 'author':'', 'title':'', 'year':''}

BAD_COLS={'Unnamed: 0','_llp_'}

CHECKMARK='✓'#✅'
CROSSMARK='✗'#❌'
DIR_TEXTS_NAME='texts'


### OPTIONS
COL_ID='id'
COL_ADDR='_id'
COL_CORPUS='_corpus'
IDSEP_START='_'
IDSEP='/'

USE_DB = True


NULL_QID='Q0'
ANNO_EXTS=['.anno.xlsx','.anno.xls','.anno.csv','.xlsx','.xls']

EMPTY_GROUP='(all)'

TMP_CORPUS_ID='tmp'
# PATH_LLTK_LOG_FN = os.path.join(PATH_LLTK_HOME, 'logs','debug_{time}.log')
LOG_VERBOSE_TERMINAL=1  # 0-3
LOG_VERBOSE_JUPYTER=1


MODERNIZE_SPELLING=False
ZIP_PART_DEFAULTS={'txt','freqs','metadata','xml','data'} # raw
DOWNLOAD_PART_DEFAULTS=['metadata']
PREPROC_CMDS=['txt','freqs','mfw','dtm']
DEST_LLTK_CORPORA=config.get('CLOUD_DEST','/Share/llp_corpora')
DEFAULT_MFW_N =25000
DEFAULT_DTM_N = 25000
DEFAULT_MFW_YEARBIN = 100
MANIFEST_REQUIRED_DATA=['name','id']
TEXT_PATH_KEYS=[
	'path_freqs',
	'path_txt',
	'path_xml'
]
BROKENSTATE='__Broken_state__'
# corenlp
PATH_CORENLP = '~/lltk_data/tools/corenlp'

# Files for text processing
PATH_TO_ENGLISH_WORDLIST = 'data/default/wordlist.aspell.net.with_caps.txt.gz'
PATH_TO_ENGLISH_STOPWORDS = 'data/default/stopwords.onix.txt.gz'
PATH_TO_ENGLISH_SPELLING_MODERNIZER = 'data/default/spelling_variants_from_morphadorner.txt.gz'
PATH_TO_ENGLISH_OCR_CORRECTION_RULES = 'data/default/CorrectionRules.txt.gz'
PATH_TO_ENGLISH_WORD2POS = 'data/default/word2pos.json.gz'


# BookNLP?
PATH_TO_BOOKNLP_BINARY= 'bin/book-nlp/runjava'


# uploaders
PATH_CLOUD_SHARE_CMD='bin/dropbox_uploader.sh share'
PATH_CLOUD_UPLOAD_CMD='bin/dropbox_uploader.sh upload'
PATH_CLOUD_LIST_CMD='bin/dropbox_uploader.sh list'
PATH_CLOUD_DEST = '/Share/llp_corpora'



# etc
CLARISSA_ID=CLAR_ID=f'_epistolary/_chadwyck/Eighteenth-Century_Fiction/richards.01'
MASQ_ID='_epistolary/_chadwyck/Eighteenth-Century_Fiction/haywood.13'
EVELINA_ID = '_epistolary/_chadwyck/Eighteenth-Century_Fiction/burney.01'





MANIFEST_DEFAULTS=dict(
	# id='corpus',
	# name='Corpus',
	path_txt='txt',
	path_xml='xml',
	path_nlp='nlp',
	path_pos='pos',
	path_index='',
	ext_xml='.xml',
	ext_txt='.txt',
	ext_nlp='.jsonl',
	path_cadence_scan='cadence_scan',
	path_cadence_parse='cadence_parse',

	path_model='',
	path_header=None,
	#path_metadata='metadata.txt',
	path_metadata='metadata.csv',
	path_metadata_init='metadata_init.csv',
	path_metadata_letters='metadata_letters.pkl',
	path_notebook='notebook.ipynb',
	paths_text_data=[],
	paths_rel_data=[],
	class_name='',
	path_freq_table={},
	col_id='id',
	col_addr='_id',
	col_id_corpus='id_corpus',
	col_id_text='id_text',
	idsep='|',col_t='_text',
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
	path_chars='chars',
	path_letters='letters',
	is_meta = '',
	public='',
	private='',
	mfw_yearbin=False,
	mfw_n=25000,

	license='',
	license_type='',
	year_start=None,
	year_end=None,
	lang='en'
)
BAD_TAGS={'figdesc','head','edit','note','header','footer','dochead','front'}
colorconc='#f9b466'
colorabs='#83b9d8'



# models: booknlp
BOOKNLP_NARRATOR_ID='NARRATOR'
BOOKNLP_DEFAULT_LANGUAGE="en"
BOOKNLP_DEFAULT_MODEL='small'
BOOKNLP_DEFAULT_PIPELINE="entity,quote,supersense,event,coref"
BOOKNLP_RENAME_COLS={'paragraph_ID': 'para_i',
'sentence_ID': 'sent_i',
'token_ID_within_sentence': 'sent_token_i',
'token_ID_within_document': 'token_i',
'word': 'token',
'lemma': 'lemma',
'byte_onset': 'onset',
'byte_offset': 'offset',
'POS_tag': 'pos',
'fine_POS_tag': 'pos2',
'dependency_relation': 'deprel',
'syntactic_head_ID': 'head',
'event': 'event'
}
BAD_CHAR_IDS={'?','?!','x','nan','None'}
chardata_metakeys_initial = dict(
    char_race='',
    char_gender='',
    char_class='',
    char_geo_birth='',
    char_geo_marriage='',
    char_geo_death='',
    char_geo_begin='',
    char_geo_middle='',
    char_geo_end='',
)


CORPUS_SOURCE_RANKS={
	'chadwyck':1,
	'hathi':4,
	'wikidata':7,
	'chicago':3,
	'markmark':2,
}

YEARKEYS=['year','date']

INIT_DB_WITH_CORPORA = {
	# 'bpo',
	'chadwyck',
	'chicago',
	'markmark',
	'txtlab',
	'tedjdh',
	'gildedage',
	# 'canon_fiction',
	'clmet',
	'dta',
	'dialnarr',
	'estc',
	'eebo_tcp',
	'ecco_tcp',
	'ecco',
	'evans_tcp',
	'litlab',
	# 'ravengarside',
	'semantic_cohort',
	'spectator'
}









### BUILTIN MODULES
import imp,os,sys,json,random,gzip,time,inspect,pickle,re,configparser,urllib,tempfile,six,shutil,tarfile,gzip,time,logging,math
from pprint import pprint,pformat
from collections import defaultdict,Counter,OrderedDict,UserList,UserDict,UserString
from functools import partial
from datetime import datetime
from os.path import expanduser
from pkg_resources import ensure_directory
from argparse import Namespace
from urllib.error import HTTPError
from zipfile import ZipFile
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
    cast,
    Tuple,
	# Literal
)

from pathlib import Path
from urllib.parse import quote_plus
import requests






# objects
ZODB_CONN=None
ZODB_OBJ=None
nlp=None
ENGLISH=None
stopwords=set()
MANIFEST={}
spellingd={}
CORPUS_FUNCS={}





### EXTERNAL MODULES
from yapmap import *
import numpy as np,pandas as pd
import plotnine as p9
import networkx as nx
from yapmap import *
from xopen import xopen
from base64 import b64decode, b64encode
from dotenv import load_dotenv
import ujson,orjson

if os.path.exists(PATH_ASTRA_ENV): load_dotenv(PATH_ASTRA_ENV)


## Setup logger
### MY MODULES

from lltk.tools import *
log = Log(
	to_screen=LOG_TO_SCREEN,
	to_file=LOG_TO_FILE,
	fn=PATH_LLTK_LOG_FN,
	force=True,
	verbose=LOG_VERBOSE_JUPYTER if in_jupyter() else LOG_VERBOSE_TERMINAL
)
# log.info('booting')

from lltk.text import *
from lltk.corpus import *
from lltk.model import *
from lltk.model.networks import draw_nx

log_on() if REMOTE_DEFAULT else log_off()
LLTK = BaseLLTK(log=log)
online=LLTK.online
offline=LLTK.offline

def search(*x,**y): return LLTK.search(*x,**y)
def get(*x,**y): return LLTK.get(*x,**y)
def sync(*x,**y): return LLTK.sync(*x,**y)
def walk(*x,**y): yield from LLTK.search_iter(*x,**y)
where = get
find = search
G = LLTK
look = hunt = walk
cdb=db=CDB
try:
	CDB()
except:
	pass
# cdb = db = CDB()
# cdb = lambda: CDB()
T=Text
C=Corpus
# log.info('--' * 25)
# log.info('ready')
