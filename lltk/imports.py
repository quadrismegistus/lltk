from lltk.tools.constants import *

import sys,os,warnings,shutil
warnings.filterwarnings('ignore')

from lltk.tools.logs import log, log_on, log_off, logger
from lltk.tools.tools import config, remove_duplicates

### IMPORTANT: SET WHERE LLTK ROOT IS BASED:
PATH_LLTK_HOME = PATH_DEFAULT_LLTK_HOME
PATH_CORPUS = os.path.expanduser(config.get('PATH_TO_CORPORA', os.path.join(PATH_LLTK_HOME,'corpora') ))
PATH_CORPUS_ZIP = os.path.join(PATH_CORPUS, 'lltk_corpora')
PATH_TO_CORPUS_CODE = config.get('PATH_TO_CORPUS_CODE', os.path.join(PATH_HERE,'corpus') )
PATH_TO_DATA_CODE = os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..','data'))
PATH_LLTK_CODE_HOME = os.path.abspath(os.path.join(PATH_TO_CORPUS_CODE,'..','..'))
PATH_LLTK_HOME_DATA = PATH_LLTK_DATA = os.path.join(PATH_LLTK_HOME,'data')


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
PATH_DEFAULT_DATA = os.path.join(PATH_LLTK_HOME_DATA,'default.zip')
PATH_MANIFEST_GLOBAL = os.path.join(ROOT,'corpus','manifest.txt')
PATH_LLTK_REPO=os.path.abspath(os.path.join(LLTK_ROOT,'..'))
DEST_LLTK_CORPORA=config.get('CLOUD_DEST','/Share/llp_corpora')

# objects
nlp=None
ENGLISH=None
stopwords=set()
MANIFEST={}
spellingd={}
CORPUS_FUNCS={}

### BUILTIN MODULES
import os,sys,json,random,gzip,time,inspect,pickle,re,configparser,urllib,tempfile,shutil,tarfile,logging,math
from pprint import pprint
from collections import defaultdict,Counter,OrderedDict,UserList
from functools import partial
from datetime import datetime
from os.path import expanduser
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
)

from pathlib import Path
from urllib.parse import quote_plus
import requests

### EXTERNAL MODULES
import numpy as np,pandas as pd
import networkx as nx
from xopen import xopen
from base64 import b64decode, b64encode
import orjson

## Setup tools (re-exports pmap, get_tqdm, etc.)
from lltk.tools import *

from lltk.text import *
from lltk.corpus import *
from lltk.model import *

T=Text
C=Corpus
