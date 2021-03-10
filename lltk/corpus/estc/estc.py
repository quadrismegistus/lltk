from lltk.imports import *

class TextESTC(Text):
	META_SEP=' | '

	@property
	def json(self):
		with open(self.fnfn) as f:
			return json.loads(f.read())

	@property
	def estcid(self):
		return self.id.split('/')[-1]

	@property
	def id_estc(self):
		return self.estcid

	@property
	def codes(self):
		codes=[]
		jsonx=self.json
		for fieldd in jsonx['fields']:
			for field_code,field_value in list(fieldd.items()):
				if type(field_value)==dict and 'subfields' in field_value:
					for subfieldd in field_value['subfields']:
						for subfield_code,subfield_value in list(subfieldd.items()):
							codes+=[(field_code+'_'+subfield_code, subfield_value)]
				else:
					codes+=[(field_code,field_value)]
		return codes

	@property
	def coded(self):
		coded=defaultdict(list)
		for code,val in self.codes:
			val=tools.noPunc(val).strip()
			if not val in coded[code]:
				coded[code]+=[val]
		return coded


	@property
	def meta_by_file(self):
		md={}
		md['id']=self.id
		md['id_estc']=self.id.split('/')[-1]

		coded = self.coded

		## marc data in 008
		marc=''.join(coded['008'])
		md['year']=marc[7:11]
		md['year_end']=marc[11:15].strip()
		md['year_type_marc']=marc[6]
		md['form_marc']=marc[33]
		md['lang']=marc[35:38]

		# author
		md['author']=coded['100_a']
		md['author_dates']=coded['100_c']

		# title
		md['title']=coded['245_a']
		md['title_sub']=coded['245_b']

		# pubinfo
		#md['pubplace']=tools.noPunc(coded.get('260_a','')).strip()
		md['pub_statement']=coded['260_b']
		md['pub_nation']=coded['752_a']
		md['pub_region']=coded['752_b']
		md['pub_city']=coded['752_d']

		# book
		ansi_escape = re.compile(r'\x1b.')
		md['book_extent']=coded['300_a']
		md['book_dimensions']=[ansi_escape.sub('',x).replace('0',u'⁰') for x in coded['300_c']]

		# book v2
		md['book_extent_num_pages']=''
		if md['book_extent']:
			extent=md['book_extent'][0]
			nums_in_extent = [int(x) for x in re.findall(r'\d+', extent)]
			md['book_extent_num_pages']=str(max(nums_in_extent)) if nums_in_extent else ''

		md['book_dimensions_standard']=''
		if md['book_dimensions']:
			dim=md['book_dimensions'][0]
			if u'⁰' in dim and dim[dim.index(u'⁰')-1].isdigit():
				md['book_dimensions_standard']=dim[dim.index(u'⁰')-1]



		# notes
		#"""
		md['notes']=[]
		for key in coded:
			if int(key[:3])>=500 and int(key[:3])<510 and key.endswith('a'):
				md['notes']+=coded[key]
		#"""

		# subject tags
		md['subject_topic']=coded['650_a']
		md['subject_place']=coded['651_a']
		md['subject_person']=coded['700_a']
		md['form']=coded['655_a']

		# stringify
		for k,v in list(md.items()):
			if type(v)==list:
				md[k]=self.META_SEP.join(v)

		return md


	@property
	def features(self):
		pass


	#@property
	#def title(self):
	#	return self.meta['title']+' '+self.meta['title_sub']



## CORPUS ##

from lltk.corpus.corpus import Corpus
from collections import defaultdict,Counter
import os
from lltk import tools


"""
Marc form codes

0: 9380,    --> nonfiction (not further specified)
1: 52,      --> fiction (not futher specified)
'': 1086,   --> ?
u'a': 3276, --> journalism
u'b': 22,   --> ?
u'd': 13,   --> Drama
u'e': 3,    --> Essays
u'f': 4,    --> Novels
u'h': 12,   --> Humor
u'i': 1,    --> Letters
u'm': 3,    --> Mixed forms
u'p': 112,  --> Poetry
u's': 7,    --> Speeches
u'|': 467740  --> Not tagged

"""



class ESTC(Corpus):
	TEXT_CLASS=TextESTC
	PATH_TXT = 'estc/_json_estc'
	EXT_TXT='.json.txt'
	PATH_METADATA = 'estc/corpus-metadata.ESTC.txt'
	PATHS_TEXT_DATA = ['estc/_data_texts_estc/data.genre-predictions.ESTC.txt']
	PATHS_REL_DATA = ['estc/_data_rels_estc/data.rel.reprintOf.exact-matches.ESTC.txt','estc/_data_rels_estc/data.rel.reprintOf.fuzzy-matches.ESTC.txt'] # ,'estc/_data_rels_estc/data.rel.reprintOf.ecco-text-matches.ESTC.txt'


	def save_metadata(self):
		print('>> generating metadata...')
		texts = self.texts()
		num_texts = len(texts)
		estc_ids_in_ecco = set(open('/Users/ryan/DH/18C/titles/estc/estc_ids_in_ecco.txt').read().split())

		# def meta(text):
		# 	dx=text.meta_by_file
		# 	dx['in_ecco']=dx['id_estc'] in estc_ids_in_ecco
		# 	return dx

		def writegen():
			for i,t in enumerate(self.texts()):
				if not i%1000: print(i)
				yield meta(t)

		tools.writegen('corpus-metadata.'+self.name+'.txt', writegen)


	def save_code_freqs_by_year(self):
		estc_ids_in_ecco = set(open('/Users/ryan/DH/18C/titles/estc/estc_ids_in_ecco.txt').read().split())
		for i,t in enumerate(self.texts()):
			#if not t.estcid in estc_ids_in_ecco: continue
			if not i%1000: print('>>',i,'...')


			dx={'year':year}

	def save_code_freqs(self):
		code2freq=defaultdict(dict)
		estc_ids_in_ecco = set(open('/Users/ryan/DH/18C/titles/estc/estc_ids_in_ecco.txt').read().split())
		for i,t in enumerate(self.texts()):
			#if not t.estcid in estc_ids_in_ecco: continue
			if not i%1000: print('>>',i,'...')
			codes = t.codes

			code8=[v for c,v in codes if c=='008'][0]
			year=code8[7:11]
			if not year[:2].isdigit() or int(year[:2])<17 or int(year[:2])>17: continue
			if not year in code2freq['008']: code2freq['008'][year]=0
			code2freq['008'][year]+=1

			"""
			cd = Counter([c for c,v in codes])
			for code,value in codes:
				if code<'6': continue
				value=tools.noPunc(value)
				if not value in code2freq[code]: code2freq[code][value]=0.0
				code2freq[code][value]+=1.0 / cd[code]
			"""

			cdict=defaultdict(list)
			for c,v in codes:
				if c<'6': continue
				cdict[c]+=[tools.noPunc(v)]

			for code,values in list(cdict.items()):
				value = ' | '.join(values)
				if not value in code2freq[code]: code2freq[code][value]=0
				code2freq[code][value]+=1


			#break


		for code in code2freq:
			tools.write2('code_freqs.%s.txt' % code, code2freq[code])
