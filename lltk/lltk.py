"""
v1.1 LLTK Lab Edition
"""


from __future__ import absolute_import
from __future__ import print_function
import os,codecs
#from . import tools
from .tools import *

ROOT = os.path.dirname(os.path.realpath(__file__))
SPELLING_VARIANT_PATH=os.path.join(ROOT,'data/spelling_variants_from_morphadorner.txt')






### FUNCTIONS

from .text import *
from .corpus import * #load_corpus, corpora




## Spelling
V2S = None
def variant2standard():
	global V2S
	if not V2S:
		V2S = dict((d['variant'],d['standard']) for d in tools.tsv2ld(SPELLING_VARIANT_PATH,header=['variant','standard','']))
	return V2S

def standard2variant():
	v2s=variant2standard()
	d={}
	for v,s in list(v2s.items()):
		if not s in d: d[s]=[]
		d[s]+=[v]
	return d



def phrase2variants(phrase):
	s2v=standard2variant()
	words = phrase.split()
	word_opts = [[s]+s2v[s] for s in words]
	word_combos = list(tools.product(*word_opts))
	phrase_combos = [' '.join(x) for x in word_combos]
	return phrase_combos
###




def load_english():
	return get_english_wordlist()
