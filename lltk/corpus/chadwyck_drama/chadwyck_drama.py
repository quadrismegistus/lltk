from lltk.imports import *

### TEXT

class TextChadwyckDrama(Text):
	SPEECH_TAGS = ['speech']
	LINE_TAGS = ['l','lb','pl']
	PARA_TAGS = ['p']

	@property
	def genre(self): return 'Drama'

	@property
	def meta_by_file(self):
		md={}
		num_lines=0
		num_paras=0
		num_speeches=0

		for line in self.lines_xml():
			#if '<doc>' in line: break
			if '<T1>' in line: md['title']=line.split('<T1>')[1].split('</T1>')[0].strip()
			if '<t1>' in line: md['title']=line.split('<t1>')[1].split('</t1>')[0].strip()
			if '<A1>' in line: md['author']=line.split('<A1>')[1].split('</A1>')[0].strip()
			if '<a1>' in line: md['author']=line.split('<a1>')[1].split('</a1>')[0].strip().replace('&ndash;','-').replace('&mdash;','-').replace('&hyphen;','-')
			if '<Y1>' in line: md['date']=line.split('<Y1>')[1].split('</Y1>')[0].strip()
			if '<y1>' in line: md['date']=line.split('<y1>')[1].split('</y1>')[0].strip()
			if '<T2>' in line: md['title_volume']=line.split('<T2>')[1].split('</T2>')[0].strip()
			if '<t2>' in line: md['title_volume']=line.split('<t2>')[1].split('</t2>')[0].strip()
			if '<ID>' in line: md['idz']=line.split('<ID>')[1].split('</ID>')[0].strip()
			if '<id>' in line: md['idz']=line.split('<id>')[1].split('</id>')[0].strip()
			for lntag in self.LINE_TAGS:
				if '<'+lntag+'>' in line or '</'+lntag+'>' in line:
					num_lines+=1
			for lntag in self.PARA_TAGS:
				if '<'+lntag+'>' in line or '</'+lntag+'>' in line:
					num_paras+=1
			for lntag in self.SPEECH_TAGS:
				if '<'+lntag+'>' in line or '</'+lntag+'>' in line:
					num_speeches+=1


		if 'American' in self.id:
			md['nation']='American'
		else:
			md['nation']='British'
		md['medium']='Verse' if num_lines > num_paras else 'Prose'
		md['genre']='Drama'
		md['id']=self.id
		md['num_lines']=num_lines
		md['num_paras']=num_paras
		md['num_speeches']=num_speeches
		md['subcorpus']=self.id.split('/')[0]
		md['author_id']=self.id.split('/')[1]
		md['text_id']=self.id.split('/')[2]
		if 'date' in md and md['date']:
			try:
				md['year']=int(md['date'][:4])
			except ValueError:
				md['year']=''
		else:
			md['year']=''
		#md['author_gender']=self.get_author_gender(md['author'])
		md['posthumous']=self.get_is_posthumous(md['author'],md['year']) if md.get('author','') and md.get('year','') else None
		return md


	def text_plain(self, OK=['speech'], BAD=['note','speaker','stage'], body_tag='play'):
		REPLACEMENTS={
			'&indent;':'    ',
			'&hyphen;':'-',
			u'\u2014':' -- ',
			u'\u2013':' -- ',
		}

		if not self.exists: return ''
		if os.path.exists(self.fnfn_txt):
			#print('>> text_plain from stored text file:',self.fnfn_txt)
			return tools.read(self.fnfn_txt)

		#print('>> text_plain from stored XML file...')

		txt=[]
		dom = self.dom

		for tag in BAD:
			[x.extract() for x in dom.findAll(tag)]

		for speech in dom('speech'):
			txt+=[speech.text.strip()]

		txt='\n\n'.join(txt).replace(u'∣','').strip()
		for k,v in list(REPLACEMENTS.items()):
			txt=txt.replace(k,v)
		return txt


	#@property
	#def path_txt(self):
	#	# @HACK!!!
	#	return self.path_xml



### CORPUS
class ChadwyckDrama(Corpus):
	TEXT_CLASS=TextChadwyckDrama

	def gen_xml(self, split_start='<poem>', split_end='</poem>', id_split_start='<ID>', id_split_end='</ID>'):
		"""
		Mapping from raw to this corpus's data.
		Will generate XML snippets from raw data, one per poem, stored in author folders.
		"""
		import multiprocessing as mp

		objects = []

		for subcorpus in os.listdir(self.PATH_RAW):
			subpath = os.path.join(self.PATH_RAW, subcorpus)
			opath = os.path.join(self.path_xml, subcorpus)
			for author_fn in os.listdir(subpath):
				author_fnfn=os.path.join(subpath,author_fn)
				if not author_fnfn.endswith('.new'): continue

				objects+=[(author_fnfn,opath)]


		#pool=mp.Pool()
		#pool.map(save_plays_from_raw_author_folder, objects)
		for objectx in objects:
			save_plays_from_raw_author_folder(objectx)




## FUNCTIONS

def save_plays_from_raw_author_folder(xxx_todo_changeme, use_bs4=False):
	(author_fnfn, opath) = xxx_todo_changeme
	import bs4,codecs
	author_fn=os.path.split(author_fnfn)[-1]
	author_id = author_fn.split('.')[0]
	opath = os.path.join(opath, author_id)
	if not os.path.exists(opath): os.makedirs(opath)

	print('>>',opath,'...')
	with codecs.open(author_fnfn,encoding='latin1') as f:
		txt=f.read()
		txt=six.text_type(txt.replace('\r\n','\n').replace('\r','\n'))
		if use_bs4:
			dom = bs4.BeautifulSoup(txt,'lxml')
			for poem_i,poem in enumerate(dom('poem')):
				poem_num=poem_i+1
				ids=poem('id')
				if not ids: continue
				idx=ids[0].text
				if not idx: continue
				poem_xml_str = six.text_type(poem)

				ofnfn = os.path.join(opath, idx+'.xml')
				with codecs.open(ofnfn,'w',encoding='utf-8') as of:
					of.write(poem_xml_str)
		else:
			txt=txt.replace('<play ','<play>')
			for poem in txt.split('<play>')[1:]:
				idx=None
				#try:
				poem = '<play>'+ poem.split('</play>')[0]+'</play>'
				if '</id>' in poem:
					idx=poem.split('</id>')[0].split('<id>')[1]
				elif '</ID>' in poem:
					idx=poem.split('</ID>')[0].split('<ID>')[1]
				elif '</idref>' in poem:
					idx=poem.split('</idref>')[0].split('<idref>')[1]
				else:
					raise Exception("ID??"+poem)
				#except IndexError as e:
				#	pass
				if not idx or not poem: continue
				poem_xml_str = poem

				ofnfn = os.path.join(opath, idx+'.xml')
				with codecs.open(ofnfn,'w',encoding='utf-8') as of:
					of.write(poem_xml_str)
					print('>> saved:',ofnfn)
