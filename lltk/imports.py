import sys,os,warnings,shutil
# warnings.filterwarnings('ignore')
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

# Get tools
LOG_TO_SCREEN = True
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
PATH_LLTK_DB = os.path.join(PATH_LLTK_DATA,'db')
PATH_LLTK_DB_FN = os.path.join(PATH_LLTK_DB,'database')
PATH_LLTK_DB_MATCHES = os.path.join(PATH_LLTK_DB,'matches')
PATH_LLTK_MATCHES = os.path.join(PATH_LLTK_DATA,'matches')
PATH_LLTK_DB_ENGINE = 'sqlite'
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
TEXT_META_DEFAULT = {}

BAD_COLS={'Unnamed: 0','_llp_'}

CHECKMARK='✓'#✅'
CROSSMARK='✗'#❌'
DIR_TEXTS_NAME='texts'


### OPTIONS
COL_ID='id'
COL_ADDR='_addr'
COL_CORPUS='_corpus'
IDSEP_START='_'
IDSEP='/'

USE_DB = True


NULL_QID='Q0'
ANNO_EXTS=['.anno.xlsx','.anno.xls','.anno.csv','.xlsx','.xls']

EMPTY_GROUP='(all)'

TMP_CORPUS_ID='tmp'
PATH_LLTK_LOG_FN = os.path.join(PATH_LLTK_HOME, 'logs','debug.log')
LOG_VERBOSE_TERMINAL=1  # 0-3
LOG_VERBOSE_JUPYTER=0


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
	col_addr='_addr',
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
	'wikidata':1,
	'chicago':3,
	'chadwyck':5,
}









# objects
ZODB_CONN=None
ZODB_OBJ=None
nlp=None
ENGLISH=None
stopwords=set()
MANIFEST={}
spellingd={}
CORPUS_FUNCS={}




### BUILTIN MODULES
import imp,os,sys,json,random,gzip,time,inspect,pickle,re,configparser,urllib,tempfile,six,shutil,tarfile,gzip,time,logging,math
from pprint import pprint,pformat
from collections import defaultdict,Counter
from functools import partial
from datetime import datetime
from os.path import expanduser
from pkg_resources import ensure_directory
from argparse import Namespace
from urllib.error import HTTPError
from zipfile import ZipFile
from typing import Union,Literal,Optional



### EXTERNAL MODULES
from yapmap import *
import numpy as np,pandas as pd
import plotnine as p9
import networkx as nx
from yapmap import *
from xopen import xopen


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
def log_start(*x,**y): log.show()
def log_stop(*x,**y): log.hide()

from lltk.text.utils import *
from lltk.corpus.utils import *
from lltk.text.text import *

from lltk.corpus.corpus import *
from lltk.model import *
from lltk.text.text import BaseText,Text
from lltk.corpus.corpus import BaseCorpus,Corpus

# models
from lltk.model.matcher import Matcher,MatcherModel
from lltk.model.preprocess import *
with log.hidden(): M = Matcher()