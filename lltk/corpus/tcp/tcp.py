#encoding=utf-8
from __future__ import absolute_import
from __future__ import print_function
import os
from lltk.corpus import Corpus,Corpus_in_Sections
from lltk import tools
STYPE_FN=os.path.abspath(os.path.join(__file__,'../data.section_types.xlsx'))


import os,codecs
from lltk import tools
from lltk.text import Text,TextSection
from lltk.text import clean_text
import six


class TextSectionTCP(TextSection):
	DIVIDER_TAG='DIV1'

	def load_metadata(self):
		md=super(TextSectionTCP,self).load_metadata()
		md['genre'],md['medium']=self.parent.return_genre()
		return md

	@property
	def meta(self):
		md=super(TextSectionTCP,self).meta
		genre=self.corpus.parent.sectiontype2genre(self.type)
		if genre: md['genre']=genre
		return md

class TextTCP(Text):

	@property
	def path_header(self):
		return os.path.join(self.corpus.path_header if hasattr(self.corpus,'path_header') else '', self.id+'.hdr')

	def sections_xml(self,divider_tag='DIV1'):
		return super(TextTCP,self).sections_xml(divider_tag=divider_tag,text_section_class=self.corpus.TEXT_SECTION_CLASS)

	def text_plain_from_xml(self, xml=None, OK=['p','l'], BAD=[], body_tag='text', force_xml=False, text_only_within_medium=True):
		#print('>> text_plain from stored XML file...')
		import bs4

		## get dom
		txt=[]
		if not xml:
			text_xml=xml=self.text_xml
			dom = bs4.BeautifulSoup(xml,'lxml')
		elif type(xml)==bs4.BeautifulSoup:
			dom = xml
			text_xml=six.text_type(xml)
		else:
			text_xml=xml
			dom = bs4.BeautifulSoup(xml,'lxml')

		## remove bad tags
		for tag in BAD:
			[x.extract() for x in dom.findAll(tag)]

		## get only correct text for medium
		if text_only_within_medium:
			medium=self.medium
			if not medium:
				genre,medium=self.return_genre(text_xml)
			if 'Verse' in medium or 'Poetry' in medium:
				OK = ['l']
			elif 'Prose' in medium or 'Fiction' in medium or 'Non-Fiction' in medium:
				OK = ['p']

		for doc in dom.find_all(body_tag):
			for tag in doc.find_all():
				if tag.name in OK:
					txt+=[clean_text(tag.text)]
		return '\n\n'.join(txt).replace(u'∣','')

	def text_plain_by_tag(self,OK=['p','l']):
		dom = self.dom
		body = dom.find('text')
		#print len(dom), len(body)
		last_tag=None
		tag2lists={}
		for tag in OK: tag2lists[tag]=[]
		for tag in body.find_all():
			if not tag.name in OK: continue
			tagtxt=clean_text(tag.text)
			if tag.previous_sibling == last_tag and len(tag2lists[tag.name]):
				tag2lists[tag.name][-1]+=[tagtxt]
			else:
				tag2lists[tag.name]+=[ [tagtxt] ]

			last_tag = tag

		return tag2lists

	def extract_metadata(self,mtxt):
		md={}
		## IDs
		for idnox in mtxt.split('</IDNO>')[1:-1]:
			idno_type=idnox.split('TYPE="')[1].split('">')[0]
			idno_id=idnox.split('">')[-1]

			md['id_'+idno_type.replace(' ','_')]=idno_id

		biblio = tools.yank(mtxt,'BIBLFULL')
		if biblio:
			md['title'] = biblio.split('</TITLE>')[0].split('">')[-1]
			md['author'] = tools.yank(biblio,'AUTHOR')
			md['extent'] = tools.yank(biblio,'EXTENT')
			md['pubplace'] = tools.yank(biblio,'PUBPLACE')
			md['publisher'] = tools.yank(biblio,'PUBLISHER')
			md['date'] = tools.yank(biblio,'DATE')
			md['notes']=tools.yank(biblio,'NOTESSTMT')

		try:
			md['year']=int(''.join([x for x in md.get('date','') if x.isdigit()][:4]))
		except (ValueError,TypeError) as e:
			md['year']=0

		return md

	def return_genre(self,text_xml=None):
		speaker_tag=False
		l_tag=False
		p_tag=False

		txt=text_xml.upper() if text_xml else self.text_xml.upper()

		tags = ['</SPEAKER>', '</L>', '</P>']
		tag_counts={}
		for tag in tags: tag_counts[tag]=txt.count(tag)

		drama = tag_counts['</SPEAKER>'] > 100
		verse = tag_counts['</L>'] > tag_counts['</P>']
		prose = tag_counts['</L>'] <= tag_counts['</P>']


		medium = 'Verse' if verse else 'Prose'
		genre = 'Drama' if drama else medium

		return genre,medium





class TCP(Corpus):
	STYPE_DD=None

	def sectiontype2genre(self,stype):
		if not self.STYPE_DD:
			print('>> loading stype2genre')
			self.STYPE_DD=tools.ld2dd(tools.read_ld(STYPE_FN),'section_type')
		return self.STYPE_DD.get(stype,{}).get('genre','')

	#def __init__(self,**attrs):
	#	return super(TCP,self).__init__(**attrs)


def gen_section_types():
	import tools
	from lltk.corpus.eebo import EEBO_TCP
	from lltk.corpus.ecco import ECCO_TCP
	corpora = [EEBO_TCP(), ECCO_TCP()]

	from collections import defaultdict,Counter
	section_types=defaultdict(Counter)
	for c in corpora:
		cs=c.sections
		for d in cs.meta:
			section_types[c.name][d['section_type']]+=1

	def writegen():
		all_stypes = set([key for cname in section_types for key in section_types[cname]])
		for stype in all_stypes:
			dx={'section_type':stype}
			dx['count']=0
			for cname in section_types:
				dx['count_'+cname]=section_types[cname].get(stype,0)
				dx['count']+=dx['count_'+cname]
			yield dx

	tools.writegen('data.section_types.txt', writegen)
