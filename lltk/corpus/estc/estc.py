from lltk.imports import *

class TextESTC(BaseText):
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

from lltk.corpus.corpus import BaseCorpus
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



class ESTC(BaseCorpus):
	TEXT_CLASS=TextESTC
	PATH_TXT = 'estc/_json_estc'
	EXT_TXT='.json.txt'
	PATH_METADATA = 'estc/corpus-metadata.ESTC.txt'
	PATHS_TEXT_DATA = ['estc/_data_texts_estc/data.genre-predictions.ESTC.txt']
	PATHS_REL_DATA = ['estc/_data_rels_estc/data.rel.reprintOf.exact-matches.ESTC.txt','estc/_data_rels_estc/data.rel.reprintOf.fuzzy-matches.ESTC.txt'] # ,'estc/_data_rels_estc/data.rel.reprintOf.ecco-text-matches.ESTC.txt'
	LINKS = {
		'ecco': ('id_estc', 'ESTCID'),
		'eebo_tcp': ('id_estc', 'id_stc'),
	}


	@property
	def path_metadata_enriched(self):
		return os.path.join(self.path, 'metadata_enriched.parquet')

	def load_metadata(self, force=False, **kwargs):
		# Fast path: read enriched parquet cache if newer than CSV
		enriched_path = self.path_metadata_enriched
		if not force and os.path.exists(enriched_path) and os.path.exists(self.path_metadata):
			if os.path.getmtime(enriched_path) >= os.path.getmtime(self.path_metadata):
				try:
					meta = pd.read_parquet(enriched_path)
					if self.col_id in meta.columns:
						meta = meta.set_index(self.col_id)
					return meta
				except Exception:
					pass

		from lltk.corpus.estc.book_history import standardize_format, parse_extent
		import numpy as np

		meta = super().load_metadata()
		if not len(meta):
			return meta

		# Standardize book format from book_dimensions
		if 'book_dimensions' in meta.columns:
			fmt_dicts = meta['book_dimensions'].apply(standardize_format)
			meta['format_std'] = fmt_dicts.apply(lambda x: x['format_std'])
			meta['format_modifier'] = fmt_dicts.apply(lambda x: x['format_modifier'])
			meta['format_note'] = fmt_dicts.apply(lambda x: x['format_note'])
			meta['format_secondary'] = fmt_dicts.apply(lambda x: x['format_secondary'])
			meta['format_cm'] = fmt_dicts.apply(lambda x: x['format_cm'])

		# Parse extent from book_extent
		if 'book_extent' in meta.columns:
			ext_dicts = meta['book_extent'].apply(parse_extent)
			meta['num_pages'] = ext_dicts.apply(lambda x: x['num_pages'])
			meta['num_volumes'] = ext_dicts.apply(lambda x: x['num_volumes'])
			meta['has_plates'] = ext_dicts.apply(lambda x: x['has_plates'])
			meta['extent_type'] = ext_dicts.apply(lambda x: x['extent_type'])

		# Classify genres
		genre_results = meta.apply(
			lambda r: classify_genres(
				r.get('form', ''),
				r.get('subject_topic', ''),
				r.get('title', ''),
				r.get('title_sub', ''),
			), axis=1
		)
		meta['genres'] = genre_results.apply(lambda x: x['genres'])
		meta['genre_source'] = genre_results.apply(lambda x: x['source'])
		# Pick first genre as genre_raw; map to harmonized genre
		meta['genre_raw'] = meta['genres'].apply(
			lambda gs: ' | '.join(sorted(gs)) if gs else None
		)
		meta['genre'] = meta['genres'].apply(
			lambda gs: _genres_to_harmonized(gs) if gs else None
		)
		# Keep is_fiction for backwards compat
		meta['is_fiction'] = meta['genres'].apply(is_fiction)

		# Detect translations
		meta['is_translated'] = meta.apply(_is_translated, axis=1)

		# Cache enriched result
		try:
			meta.to_parquet(self.path_metadata_enriched)
		except Exception:
			pass

		return meta


	def save_metadata(self):
		print('>> generating metadata...')
		texts = self.texts()
		num_texts = len(texts)
		estc_ids_in_ecco = set(open('/Users/ryan/DH/18C/titles/estc/estc_ids_in_ecco.txt').read().split())

		# def meta(BaseText):
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















"""
Classify ESTC records into genres using three tiers:
  1. form field (most reliable, cataloger-assigned)
  2. subject_topic field (cataloger-assigned but noisier)
  3. title + title_sub keywords (last resort fallback)

Usage:
    from classify_genre import classify_genres

    result = classify_genres(
        form='Sermons',
        subject_topic='Funeral sermons',
        title='A sermon preached before the King',
        title_sub='on the occasion of ...'
    )
    # result = {'genres': {'Sermon'}, 'source': 'form'}

    df['genres'] = df.apply(lambda r: classify_genres(
        r['form'], r['subject_topic'], r['title'], r.get('title_sub', '')
    )['genres'], axis=1)
"""

import re

GENRE_RULES = {

    # ── Fiction sub-genres (more specific first, then catch-all) ────
    # These are all "fiction" but broken out for analysis.
    # Aggregate to Fiction downstream if needed.

    'Novel': {
        'form_exact': {
            'novels', 'novellas', 'gothic novels', 'gothic fiction',
            'historical fiction', 'biographical fiction',
            'mystery and detective fiction', 'comic histories',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['novel'],
    },

    'Romance': {
        'form_exact': {'romances'},
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['romance'],
    },

    'Tale': {
        'form_exact': {
            'fairy tales', 'folk tales', 'nursery stories',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ["children's stories"],
        'title_keywords': ['tale', 'tales'],
    },

    'Fable': {
        'form_exact': {'fables', 'fabliaux'},
        'form_keywords': [],
        'topic_exact': {'fables'},
        'topic_keywords': ['fables,'],
        'title_keywords': ['fable', 'fables'],
    },

    'Picaresque': {
        'form_exact': {'picaresque fiction'},
        'form_keywords': ['picaresque'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Epistolary fiction': {
        'form_exact': {'epistolary novels', 'epistolary fiction'},
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['epistolary fiction'],
        'title_keywords': [],
    },

    'Imaginary voyage': {
        'form_exact': {
            'imaginary voyages', 'imaginary conversations',
            'robinsonades',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['voyages, imaginary'],
        'title_keywords': [],
    },

    'Fiction': {
        'form_exact': {
            'fiction', 'chapbooks', 'adventure stories',
            'utopian literature', 'utopian literature.',
            'fantasy literature', 'harlequinades',
            'short stories', 'sea stories', 'bible stories',
            'comic books',
        },
        'form_keywords': [],
        'topic_exact': {
            'english fiction', 'french fiction', 'utopias',
            'chapbooks', 'fiction',
        },
        'topic_keywords': [
            'chapbooks,',
        ],
        'title_keywords': [],
    },

    'Poetry': {
        'form_exact': {
            'poems', 'broadside poems', 'odes', 'elegies', 'verse',
            'lyric poems', 'occasional poems', 'pastoral poems',
            'poetical miscellanies', 'epics', 'acrostics',
            'epitaphs', 'epigrams', 'eclogues', 'sonnets',
            'english poetry', 'american poetry', 'neo-latin poems',
            'penny poems', 'begging poems', 'alphabet rhymes',
            'carol books', 'epistolary poetry', 'poetry of places',
        },
        'form_keywords': ['poem', 'poetry', 'elegiac'],
        'topic_exact': {
            'english poetry', 'latin poetry', 'french poetry',
            'greek poetry', 'italian poetry', 'poetry',
        },
        'topic_keywords': [
            'poetry,', 'elegiac poetry', 'verse,',
            'narrative poetry', 'love poetry', 'political poetry',
            'christian poetry', 'pastoral poetry', 'religious poetry',
            'occasional verse', 'epic poetry',
        ],
        'title_keywords': [
            'poem', 'poems', 'ode', 'odes', 'elegy', 'elegies',
            'verse', 'verses', 'sonnet', 'sonnets', 'eclogue',
            'epitaph', 'epigram', 'epigrams', 'poetry', 'poetical',
        ],
    },

    'Drama': {
        'form_exact': {
            'plays', 'plays.', 'comedies', 'tragedies',
            'tradegies', 'farces', 'tragicomedies', 'drama', 'dramas',
            'operas', 'operettas', 'libretti', 'masques',
            'morality plays', 'heroic dramas', 'ballad operas',
            'english drama (comedy', 'acting editions',
            'promptbooks', 'scenarios',
        },
        'form_keywords': ['play', 'drama', 'comedi', 'tragedi', 'librett', 'opera'],
        'topic_exact': {
            'english drama', 'opera', 'operas', 'drama',
            'french drama', 'italian drama', 'theater', 'theatres',
        },
        'topic_keywords': [
            'english drama', 'ballad opera', 'english farce',
        ],
        'title_keywords': [
            'comedy', 'tragedy', 'farce', 'opera',
            'acted', 'dramatick', 'dramatic', 'theatrical',
            'masque', 'interlude',
        ],
    },

    'Sermon': {
        'form_exact': {
            'sermons', 'funeral sermons', 'ordination sermons',
            'fast-day sermons', 'fast day sermons', 'thanksgiving sermons',
            'election sermons', 'execution sermons', 'occasional sermons',
            'artillery election sermons', 'dedication sermons',
            'installation sermons', 'farewell sermons', 'visitation sermons',
            "children's sermons", 'new year sermons', 'christmas sermons',
            'fourth of july sermons', 'anniversary sermons',
            'apocalyptic sermons', 'wedding sermons', 'century sermons',
            'half century sermons', 'thanksgiving day sermons',
        },
        'form_keywords': ['sermon'],
        'topic_exact': set(),
        'topic_keywords': ['sermon'],
        'title_keywords': ['sermon', 'sermons', 'preached', "preach'd"],
    },

    'Essay': {
        'form_exact': {'essays', 'essays.', 'essaysy', 'english essays'},
        'form_keywords': ['essay'],
        'topic_exact': {'english essays'},
        'topic_keywords': [],
        'title_keywords': ['essay', 'essays'],
    },

    'Letter': {
        'form_exact': {'letters'},
        'form_keywords': [],
        'topic_exact': {'english letters'},
        'topic_keywords': ['letter writing'],
        'title_keywords': ['letter', 'letters', 'epistle', 'epistles'],
    },

    'Almanac': {
        'form_exact': {'almanacs', 'alamancs', 'ephemerides'},
        'form_keywords': ['almanac'],
        'topic_exact': {'almanacs', 'ephemerides'},
        'topic_keywords': ['almanac'],
        'title_keywords': ['almanack', 'almanac', 'ephemeris', 'prognostication'],
    },

    'Ballad/Song': {
        'form_exact': {
            'ballads', 'songs', 'songsters', 'national songs',
            'song sheets', 'broadside ballads', 'carol books',
        },
        'form_keywords': ['ballad', 'songster'],
        'topic_exact': set(),
        'topic_keywords': ['ballads,', 'songs,', "children's songs"],
        'title_keywords': ['ballad', 'ballads', 'song', 'songs', 'garland'],
    },

    'Satire': {
        'form_exact': {
            'satires', 'satire', 'burlesques', 'lampoons',
            'lampoons test', 'parodies', 'facetiae', 'jestbooks',
            'caricatures',
        },
        'form_keywords': ['satir', 'lampoon', 'burlesque'],
        'topic_exact': set(),
        'topic_keywords': [
            'satire,', 'political satire', 'verse satire',
            'wit and humor', 'english wit',
        ],
        'title_keywords': ['satire', 'satyr', 'burlesque'],
    },

    'Catechism': {
        'form_exact': {'catechisms'},
        'form_keywords': ['catechism'],
        'topic_exact': {'catechisms'},
        'topic_keywords': ['catechism'],
        'title_keywords': ['catechism', 'catechisme', 'catechismus'],
    },

    'Devotional': {
        'form_exact': {
            'devotional literature', 'devoltional literature',
            'devotional works', 'prayer books', 'prayer books.',
            'prayers', 'hymnals', 'psalters', 'liturgical books',
            'books of hours', 'book of hours', 'breviaries', 'brevaries',
            'service books', 'missals', 'devotional calendars',
            'gospel books', 'evangeliaries',
            'primers (devotional books',
        },
        'form_keywords': ['devotional', 'prayer book', 'hymnal', 'psalter'],
        'topic_exact': {
            'devotional literature', 'devotional exercises',
            'meditations', 'prayers', 'prayer', 'psalmody',
            'psalters', 'worship', 'public worship',
        },
        'topic_keywords': [
            'devotional literature', 'devotional exercise',
            'hymns,', 'primers (prayer',
        ],
        'title_keywords': [
            'prayer', 'prayers', 'devotion', 'devotions',
            'meditation', 'meditations', 'liturgy', 'psalter',
        ],
    },

    'Hymn': {
        'form_exact': {'hymns', 'hymnals'},
        'form_keywords': ['hymn'],
        'topic_exact': set(),
        'topic_keywords': ['hymns,'],
        'title_keywords': ['hymn', 'hymns', 'psalm', 'psalms'],
    },

    'Periodical': {
        'form_exact': {
            'newspapers', 'periodicals', 'periocials', 'periiodicals',
            'newsbooks', 'newsletters',
        },
        'form_keywords': ['periodical', 'newspaper', 'newsbook', 'newsletter'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [
            'gazette', 'mercury', 'courant', 'intelligencer',
            'advertiser', 'magazine',
        ],
    },

    'Catalogue': {
        'form_exact': {
            'catalogs', 'catalogues', "booksellers' catalogs",
            "booksellers' catalogues", 'library catalogs',
            'auction catalogs', 'auction catalogues',
            'sales catalogs', 'sales catalogues', 'college catalogs',
            'trade catalogs', 'exhibition catalogs', "publishers' catalogs",
            'book auction catalogs', 'museum catalogs', 'university catalogs',
        },
        'form_keywords': ['catalog'],
        'topic_exact': set(),
        'topic_keywords': ['catalogs,', 'book auction', 'estate sales'],
        'title_keywords': ['catalogue', 'catalog'],
    },

    'Biography': {
        'form_exact': {
            'biographies', 'biogarphies', 'biography', 'memoirs',
            'autobiographies', 'diaries',
        },
        'form_keywords': ['biograph'],
        'topic_exact': {'biography', 'christian biography'},
        'topic_keywords': ['biograph'],
        'title_keywords': ['life', 'lives', 'memoir', 'memoirs'],
    },

    'History': {
        'form_exact': {
            'chronicles', 'historical works', 'military histories',
            'local histories', 'family histories', 'military unit histories',
            'annals',
        },
        'form_keywords': ['histor'],
        'topic_exact': {'history', 'world history', 'church history'},
        'topic_keywords': ['history,', 'constitutional history'],
        'title_keywords': ['chronicle', 'annals'],  # not 'history' — too many novels use it
    },

    'Travel': {
        'form_exact': {
            'travel literature', 'exploration literature',
            'discovery narratives', 'maritime journals', 'roadbooks',
        },
        'form_keywords': ['travel'],
        'topic_exact': {'voyages and travels'},
        'topic_keywords': ['voyages and travel', 'voyages around', 'shipwreck'],
        'title_keywords': ['voyage', 'voyages', 'travels', 'tour'],
    },

    'Legal': {
        'form_exact': {
            'laws', 'bills', 'biils', 'legal works', 'session laws',
            'legal formularies', 'legal instruments', 'legal petitions',
            'legal proceedings', 'charters', 'constitutions', 'treaties',
            'trial proceedings', 'trials', 'indentures', 'deeds',
            'bonds (legal records', 'oaths',
        },
        'form_keywords': ['legal'],
        'topic_exact': {'law'},
        'topic_keywords': [
            'law report', 'criminal justice', 'courts',
            'ecclesiastical law', 'maritime law', 'criminal law',
            'civil procedure', 'pleading', 'canon law',
            'equity pleading', 'conveyancing',
        ],
        'title_keywords': ['act', 'statute', 'ordinance', 'trial', 'tryal', 'indictment'],
    },

    'Proclamation': {
        'form_exact': {
            'proclamations', 'thanksgiving day proclamations',
            'fast day proclamations', 'orders in council',
            'royal ordinances', 'regulations',
            'administrative regulations',
            'military regulations', 'military orders',
        },
        'form_keywords': ['proclamation'],
        'topic_exact': {'proclamations'},
        'topic_keywords': [],
        'title_keywords': ['proclamation'],
    },

    'Petition/Address': {
        'form_exact': {
            'petitions', 'petition', 'petitoins',
            'addresses', 'addresses.', 'funeral addresses',
            'fourth of july addresses', 'legislative addresses',
            "carriers' addresses", 'new year addresses',
            'thanksgiving day addresses', 'congressional addresses',
            'parliamentary addresses', 'occasional addresses',
            'academic addresses', 'parliamentary petitions',
            'royal petitions', 'fast day addresses',
        },
        'form_keywords': ['petition'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['petition', 'address', 'remonstrance'],
    },

    'Speech': {
        'form_exact': {
            'speeches', 'gallows speeches', 'gallows speeches y 1723',
        },
        'form_keywords': [],
        'topic_exact': set(),
        'topic_keywords': ['speeches, addresses'],
        'title_keywords': ['speech', 'oration'],
    },

    'Dialogue': {
        'form_exact': {'dialogues', 'questions and answers',
                       'question and answers', 'ouestion and answers'},
        'form_keywords': ['dialogue'],
        'topic_exact': set(),
        'topic_keywords': ['dialogues,', 'questions and answers'],
        'title_keywords': ['dialogue', 'dialogues'],
    },

    'Treatise': {
        'form_exact': {'treatises', 'discursive works'},
        'form_keywords': ['treatise'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': ['treatise', 'discourse', 'dissertation'],
    },

    'Grammar/Textbook': {
        'form_exact': {
            'grammars', 'grammar', 'textbooks', 'text books',
            'spellers', 'readers', 'alphabet books',
            'primers (instructional books', 'writing books',
            'instruction books', 'drawing books', 'copy books (penmanship',
            'exercise books (penmanship', 'penmanship manuals',
            'penmanship specimen books', 'phrase books', 'hornbooks',
            'instructional works',
        },
        'form_keywords': ['grammar', 'textbook', 'speller'],
        'topic_exact': {
            'latin language', 'english language', 'french language',
            'greek language', 'hebrew language', 'italian language',
            'rhetoric', 'elocution', 'shorthand', 'penmanship',
            'bookkeeping',
        },
        'topic_keywords': ['language and language'],
        'title_keywords': ['grammar', 'arithmetic', 'spelling'],
    },

    'Dictionary': {
        'form_exact': {
            'dictionaries', 'etymological dictionaries',
            'glossaries', 'thesauri', 'concordances',
            'encyclopedias', 'bibliographies', 'indexes',
            'gazetteers', 'gazetters',
        },
        'form_keywords': ['dictionar', 'encycloped'],
        'topic_exact': {'encyclopedias and dictionaries'},
        'topic_keywords': [],
        'title_keywords': ['dictionary', 'lexicon', 'glossary', 'thesaurus'],
    },

    'Narrative': {
        'form_exact': {
            'narratives', 'captivity narratives',
            'conversion narratives', "survivors' narratives",
            'miracle narratives',
        },
        'form_keywords': ['narrative'],
        'topic_exact': set(),
        'topic_keywords': ['indian captivities'],
        'title_keywords': ['narrative', 'account', 'relation'],
    },

    'Cookery': {
        'form_exact': {'cookbooks', 'cookery books'},
        'form_keywords': ['cookbook', 'cookery'],
        'topic_exact': {'cookery'},
        'topic_keywords': ['cookery,'],
        'title_keywords': ['cookery'],
    },

    'Medical': {
        'form_exact': {'medical formularies', 'pharmacopoeias', 'herbals'},
        'form_keywords': ['pharmacopoeia'],
        'topic_exact': {'medicine', 'surgery', 'obstetrics', 'pharmacy'},
        'topic_keywords': [
            'medicine,', 'materia medica', 'dispensatories',
            'veterinary', 'pharmacopoeia',
        ],
        'title_keywords': ['physick', 'pharmacopoeia', 'dispensatory', 'surgery', 'anatomy'],
    },

    'Map': {
        'form_exact': {
            'maps', 'atlases', 'atlases (geographic',
            'atlases (scientific', 'celestial atlases', 'pilot guides',
        },
        'form_keywords': ['atlas'],
        'topic_exact': set(),
        'topic_keywords': ['pilot guide'],
        'title_keywords': ['atlas'],
    },

    'Lecture': {
        'form_exact': {'lectures', 'lecture notes'},
        'form_keywords': ['lecture'],
        'topic_exact': {'oratory'},
        'topic_keywords': [],
        'title_keywords': ['lecture', 'lectures'],
    },

    'Advertisement': {
        'form_exact': {
            'advertisements', 'advertisements.', 'advertisments',
            'adverisements', 'advertisement',
            "booksellers' advertisements", "booksellers advertisements",
            "booksellers' asvertisements", "bookseller's advertisements",
            "publishers' advertisements", "publishers' advertisement",
            "publisher's advertisements", "printers' advertisements",
            'prospectuses', 'prospectus', 'book prospectuses',
            'company prospectuses',
        },
        'form_keywords': ['advertisement', 'prospectus'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Juvenile': {
        'form_exact': {'juvenile literature', 'juvenilia'},
        'form_keywords': ['juvenile'],
        'topic_exact': set(),
        'topic_keywords': [],
        'title_keywords': [],
    },

    'Genealogy': {
        'form_exact': {'genealogies', 'genalogies', 'family trees', 'armorials'},
        'form_keywords': ['genealog'],
        'topic_exact': {'heraldry'},
        'topic_keywords': ['genealog'],
        'title_keywords': ['genealogy', 'pedigree'],
    },

    'Music': {
        'form_exact': {'musical works', 'scores', 'part books'},
        'form_keywords': ['musical'],
        'topic_exact': {'music', 'church music', 'concert programs'},
        'topic_keywords': ['oratorio'],
        'title_keywords': ['cantata', 'oratorio', 'sonata', 'concerto'],
    },

    'Allegory': {
        'form_exact': {
            'allegories', 'emblem books', 'parables',
            'doomsday literature',
        },
        'form_keywords': ['allegor'],
        'topic_exact': set(),
        'topic_keywords': ['allegory', 'emblem'],
        'title_keywords': ['allegory', 'parable'],
    },
}


def classify_genres(form=None, subject_topic=None, title=None, title_sub=None):
    """
    Classify an ESTC record into genre(s).

    Returns dict:
      'genres': set of genre labels (may be empty)
      'source': 'form' | 'topic' | 'title' | None
                which tier produced the first match
    
    Tiers 1 (form) and 2 (topic) both contribute.
    Tier 3 (title keywords) only fires if tiers 1+2 found nothing.
    """
    genres = set()
    source = None

    if form and not (isinstance(form, float) and form != form):
        form_genres = _match_field(str(form), 'form')
        if form_genres:
            genres.update(form_genres)
            source = 'form'

    if subject_topic and not (isinstance(subject_topic, float) and subject_topic != subject_topic):
        topic_genres = _match_field(str(subject_topic), 'topic')
        if topic_genres:
            genres.update(topic_genres)
            if source is None:
                source = 'topic'

    if not genres:
        title_text = ''
        if title and not (isinstance(title, float) and title != title):
            title_text = str(title)
        if title_sub and not (isinstance(title_sub, float) and title_sub != title_sub):
            title_text = title_text + ' ' + str(title_sub)
        if title_text.strip():
            title_genres = _match_title(title_text)
            if title_genres:
                genres.update(title_genres)
                source = 'title'

    return {'genres': genres, 'source': source}


def _match_field(value, field_type):
    genres = set()
    segments = [seg.strip().lower() for seg in value.split('|')]
    for genre_name, rules in GENRE_RULES.items():
        exact_set = rules.get(f'{field_type}_exact', set())
        keywords = rules.get(f'{field_type}_keywords', [])
        for seg in segments:
            if not seg:
                continue
            if seg in exact_set:
                genres.add(genre_name)
                break
            for kw in keywords:
                if kw in seg:
                    genres.add(genre_name)
                    break
    return genres


def _match_title(title_text):
    genres = set()
    words = set(re.findall(r"[a-z']+", title_text.lower()))
    for genre_name, rules in GENRE_RULES.items():
        for kw in rules.get('title_keywords', []):
            if kw in words:
                genres.add(genre_name)
                break
    return genres


def get_all_genre_names():
    return sorted(GENRE_RULES.keys())


# ── Translation detection ────────────────────────────────────────

_TRANSLATION_PHRASES = [
    'translated', 'translation', 'translator',
    'rendered into', 'rendred into', "render'd into", "rendr'd into",
    'done into english', 'turn\'d into english', 'put into english',
    'englished', 'made english',
]

_TRANSLATION_LANGUAGES = {
    'french', 'italian', 'german', 'spanish', 'dutch', 'portuguese',
    'latin', 'greek', 'hebrew', 'arabic', 'persian', 'turkish',
    'swedish', 'danish', 'polish', 'russian', 'chinese', 'japanese',
    'gaelic', 'irish', 'welsh', 'scottish gaelic',
}

def _is_translated(row):
    """Detect if an ESTC record is a translation based on metadata fields."""
    # Check title, title_sub, notes, form for translation phrases
    for col in ('title', 'title_sub', 'notes', 'form'):
        val = row.get(col, '')
        if not val or (isinstance(val, float) and val != val):
            continue
        s = str(val).lower()
        for phrase in _TRANSLATION_PHRASES:
            if phrase in s:
                return True

    # Check subject_topic for foreign language indicators
    subj = row.get('subject_topic', '')
    if subj and not (isinstance(subj, float) and subj != subj):
        s = str(subj).lower()
        for lang in _TRANSLATION_LANGUAGES:
            if lang in s:
                return True

    return False

# All fiction sub-genres for easy aggregation
FICTION_GENRES = {
    'Fiction', 'Novel', 'Romance', 'Tale', 'Fable',
    'Picaresque', 'Epistolary fiction', 'Imaginary voyage',
}

def is_fiction(genres):
    """Check if a set of genres contains any fiction sub-genre."""
    return bool(genres & FICTION_GENRES)

# Map fine-grained ESTC genres to GENRE_VOCAB broad categories
_GENRE_TO_HARMONIZED = {
    # Fiction family
    'Fiction': 'Fiction', 'Novel': 'Fiction', 'Romance': 'Fiction',
    'Tale': 'Fiction', 'Fable': 'Fiction', 'Picaresque': 'Fiction',
    'Epistolary fiction': 'Fiction', 'Imaginary voyage': 'Fiction',
    # Direct mappings
    'Poetry': 'Poetry', 'Drama': 'Drama', 'Sermon': 'Sermon',
    'Essay': 'Essay', 'Letter': 'Letters', 'Almanac': 'Almanac',
    'Biography': 'Biography', 'Legal': 'Legal', 'Speech': 'Speech',
    'Periodical': 'Periodical', 'Treatise': 'Treatise',
    # ESTC-specific → broad
    'Ballad/Song': 'Poetry', 'Hymn': 'Poetry',
    'Satire': None,  # cross-cutting mode; genre depends on co-occurring labels
    'Catechism': 'Nonfiction', 'Devotional': 'Nonfiction',
    'Catalogue': 'Reference', 'Dictionary': 'Reference',
    'Grammar/Textbook': 'Reference', 'Map': 'Reference',
    'History': 'History', 'Travel': 'Nonfiction',
    'Narrative': 'Nonfiction', 'Cookery': 'Nonfiction',
    'Medical': 'Nonfiction', 'Lecture': 'Nonfiction',
    'Advertisement': 'Nonfiction', 'Juvenile': None,
    'Genealogy': 'Reference', 'Music': 'Nonfiction',
    'Allegory': None, 'Dialogue': 'Nonfiction',
    'Proclamation': 'Legal',
    'Petition/Address': 'Nonfiction',
}

# Priority order for picking a single harmonized genre when multiple match
# Specific genres first; Fiction/Poetry/Drama last (they're catch-alls)
_HARMONIZED_PRIORITY = [
    'Sermon', 'Legal', 'Periodical', 'Almanac', 'Speech',
    'Essay', 'Letters', 'Treatise', 'Biography', 'History',
    'Reference', 'Nonfiction',
    'Drama', 'Poetry', 'Fiction',
]

def _genres_to_harmonized(genres):
    """Map a set of fine-grained genres to a single harmonized GENRE_VOCAB label."""
    harmonized = set()
    for g in genres:
        h = _GENRE_TO_HARMONIZED.get(g)
        if h:
            harmonized.add(h)
    if not harmonized:
        return None
    # Pick by priority
    for h in _HARMONIZED_PRIORITY:
        if h in harmonized:
            return h
    return harmonized.pop()


# if __name__ == '__main__':
#     tests = [
#         ('Fiction', '', '', '', {'Fiction'}, 'form'),
#         ('Novels', '', '', '', {'Novel'}, 'form'),
#         ('Sermons', '', '', '', {'Sermon'}, 'form'),
#         ('Plays', '', '', '', {'Drama'}, 'form'),
#         ('Poems', '', '', '', {'Poetry'}, 'form'),
#         ('Satires', '', '', '', {'Satire'}, 'form'),
#         ('Ballads', '', '', '', {'Ballad/Song'}, 'form'),
#         ('Essays', '', '', '', {'Essay'}, 'form'),
#         ('Almanacs', '', '', '', {'Almanac'}, 'form'),
#         ('Catechisms', '', '', '', {'Catechism'}, 'form'),
#         ('Grammars', '', '', '', {'Grammar/Textbook'}, 'form'),
#         ('Letters', '', '', '', {'Letter'}, 'form'),
#         ('Newspapers', '', '', '', {'Periodical'}, 'form'),
#         ('Catalogs', '', '', '', {'Catalogue'}, 'form'),
#         ('Biographies', '', '', '', {'Biography'}, 'form'),
#         ('Maps', '', '', '', {'Map'}, 'form'),
#         # Fiction sub-genres
#         ('Chapbooks', '', '', '', {'Fiction'}, 'form'),
#         ('Fiction', '', '', '', {'Fiction'}, 'form'),
#         ('Epistolary novels', '', '', '', {'Epistolary fiction'}, 'form'),
#         ('Epistolary Novels', '', '', '', {'Epistolary fiction'}, 'form'),
#         ('Gothic novels', '', '', '', {'Novel'}, 'form'),
#         ('Fables', '', '', '', {'Fable'}, 'form'),
#         ('Romances', '', '', '', {'Romance'}, 'form'),
#         ('Fairy tales', '', '', '', {'Tale'}, 'form'),
#         ('Picaresque fiction', '', '', '', {'Picaresque'}, 'form'),
#         ('Imaginary voyages', '', '', '', {'Imaginary voyage'}, 'form'),
#         ('Adventure stories', '', '', '', {'Fiction'}, 'form'),
#         # Topics
#         ('', 'English fiction', '', '', {'Fiction'}, 'topic'),
#         ('', 'Epistolary fiction, English', '', '', {'Epistolary fiction'}, 'topic'),
#         ('', 'Fables, English', '', '', {'Fable'}, 'topic'),
#         ('', 'Voyages, Imaginary', '', '', {'Imaginary voyage'}, 'topic'),
#         ('', 'English drama', '', '', {'Drama'}, 'topic'),
#         ('', 'English poetry', '', '', {'Poetry'}, 'topic'),
#         ('', 'Funeral sermons', '', '', {'Sermon'}, 'topic'),
#         # Both tiers
#         ('Sermons', 'English poetry', '', '', {'Sermon', 'Poetry'}, 'form'),
#         # Title fallback
#         ('', '', 'A sermon preached before the King', '', {'Sermon'}, 'title'),
#         ('', '', 'An essay concerning humane understanding', '', {'Essay'}, 'title'),
#         ('', '', 'A voyage to the South Seas', '', {'Travel'}, 'title'),
#         ('', '', 'A tragedy called Hamlet', '', {'Drama'}, 'title'),
#         ('', '', 'The novel adventures of Tom Jones', '', {'Novel'}, 'title'),
#         ('', '', 'A romance of the forest', '', {'Romance'}, 'title'),
#         ('', '', 'A pleasant tale of two lovers', '', {'Tale'}, 'title'),
#         ('', '', 'Fables of Aesop', '', {'Fable'}, 'title'),
#         # Title should NOT fire if form/topic matched
#         ('Sermons', '', 'poems and essays', '', {'Sermon'}, 'form'),
#         # Empty
#         ('', '', '', '', set(), None),
#         (None, None, None, None, set(), None),
#         # Pipe-separated
#         ('Poems | Fiction', '', '', '', {'Poetry', 'Fiction'}, 'form'),
#         ('Poems | Novels', '', '', '', {'Poetry', 'Novel'}, 'form'),
#     ]
#     failures = 0
#     for form, topic, title, title_sub, exp_genres, exp_source in tests:
#         result = classify_genres(form, topic, title, title_sub)
#         ok = result['genres'] == exp_genres and result['source'] == exp_source
#         if not ok:
#             failures += 1
#             print(f"FAIL: form={form!r} topic={topic!r} title={title!r}")
#             print(f"  expected genres={exp_genres} source={exp_source}")
#             print(f"  got      genres={result['genres']} source={result['source']}")
#     if failures:
#         print(f"\n{failures}/{len(tests)} FAILED")
#     else:
#         print(f"All {len(tests)} tests passed")