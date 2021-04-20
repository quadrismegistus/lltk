from lltk.imports import *




class TextECCO(Text):
	@property
	def meta_by_file(self):
		if not hasattr(self,'_meta'):
			import gzip
			mtxt=''
			f = gzip.open(self.fnfn,'rb')
			for line in f:
				line=line.decode('iso-8859-1').encode('utf8')
				mtxt+=line
				if '</citation>' in line:
					break

			md=self.extract_metadata(mtxt)
			md['id']=self.id
			self._meta=md
		return self._meta

	def extract_metadata(self,mtxt,word_stats=True):
		from lltk import load_english
		ENGLISH=load_english()

		md={}
		## IDs
		import bs4
		dom=bs4.BeautifulSoup(mtxt,'html.parser')
		md={}

		simples = ['documentID','ESTCID','pubDate','releaseDate','sourceLibrary','language','model','documentType','marcName','birthDate','deathDate','marcDate','fullTitle','currentVolume','totalVolumes','imprintFull','imprintCity','imprintPublisher','imprintYear','collation','publicationPlace','totalPages']

		for x in simples:
			try:
				md[x]=dom.find(x.lower()).text
			except AttributeError:
				md[x]=''


		## Num Holdings
		md['holdings_num_libraries']=mtxt.count('</holdings>')
		#md['libraries'] = [tag.text for tag in dom('libraryname')]
		md['notes'] = ' | '.join([tag.text for tag in dom('notes')])

		for subjhead in dom('locsubjecthead'):
			subtype=subjhead.get('type','')
			for subj in subjhead('locsubject'):
				subfield=subj.get('subfield','')
				val=subj.text
				md[subtype+'_'+subfield]=val

		try:
			md['year']=int(''.join([x for x in md['pubDate'] if x.isdigit()][:4]))
		except (ValueError,TypeError) as e:
			md['year']=0

		## if word_stats

		words=[w for w,p in self.tokens]
		md['num_words']=len(words)
		md['ocr_accuracy']=len([w for w in words if w in ENGLISH]) / float(len(words)) if len(words) else 0.0

		return md

	@property
	def text(self):
		import gzip
		try:
			with gzip.open(self.fnfn, 'rb') as f:
				file_content = f.read()
		except IOError:
			print("!! Error on gzip for file id",self.id)
			return ''
		return file_content

	@property
	def fnfn_txt(self):
		return os.path.join(self.corpus.path_txt,self.id+'.txt.gz')

	@property
	def fnfn(self):
		return os.path.join(self.corpus.path_xml,self.id+'.xml.gz')

	@property
	def has_plain_text_file(self):
		return os.path.exists(self.fnfn_txt)

	@property
	def text_plain_from_file(self):
		if not self.has_plain_text_file:
			return False

		import gzip
		try:
			with gzip.open(self.fnfn_txt,'rb') as f:
				txt=f.read().decode('utf-8')
		except:
			print("!! ERROR: could not decompress:",self.id)
			return ''
		return txt


	def text_plain(self, OK_word=['wd'], OK_page=['bodyPage'], remove_catchwords=True, return_lists=False, save_when_gen=True):
		cache=self.text_plain_from_file
		if cache:
			print('>>',self.fnfn_txt,'from cache')
			return cache

		

		"""
		Get the plain text from the ECCO xml files.
		OK_word sets the tags that define a word: in ECCO, <wd>.
		OK_page sets the tags that define a page we want:
		- bodyPage are the body pages
		- frontmatter are frontmatter paggers;
			-- I've decided not to include them, but you can add them by adding it to the OK_page list
		"""

		print('>>', self.fnfn)

		txt=[]
		dom = self.dom
		body = dom.find('text')
		if not body: return ''
		for page in body.find_all('page'):
			if page.get('type','') in OK_page:
				page_txt=[]
				para_txt=[]
				line_txt=[]
				lastParent=None
				lastLineOffset=None
				for tag in page.find_all():
					if tag.name in OK_word:


						## Check for new paragraph
						if tag.parent != lastParent:
							if line_txt:
									para_txt+=[line_txt]
									line_txt=[]
							if para_txt:
								page_txt+=[para_txt]
								para_txt=[]
						lastParent=tag.parent

						## Check for new line
						lineOffset = int(tag['pos'].split(',')[0])
						if lastLineOffset is None:
							lastLineOffset=lineOffset
						elif lineOffset < lastLineOffset:
							if line_txt:
								para_txt+=[line_txt]
								line_txt=[]
						lastLineOffset = lineOffset

						text=clean_text(tag.text)
						line_txt+=[text]

				if line_txt:
					para_txt+=[line_txt]
				if para_txt:
					page_txt+=[para_txt]
				if page_txt:
					txt+=[page_txt]

		for page_i,page in enumerate(txt):
			#print '>> page:',page_i
			if remove_catchwords:
				last_word_this_page = page[-1][-1][-1]
				first_word_next_page = txt[page_i+1][0][0][0] if len(txt)>page_i+1 else None
				if last_word_this_page == first_word_next_page:
					# if last word is a catchword for first word of next page,
					# remove the last line
					page[-1][-1].pop()

			page = [para for para in page if len(para) and sum(len(line) for line in para)>0]
			txt[page_i] = page

		if return_lists:
			return txt

		## otherwise, make plain text
		plain_text = u"\n\n\n".join(u"\n\n".join(u"\n".join(u" ".join(word for word in line) for line in para) for para in page) for page in txt)

		if save_when_gen:
			self.save_plain_text(txt=plain_text,compress=True)

		return plain_text




class ECCO(Corpus):
	TEXT_CLASS=TextECCO
	EXT_XML = '.xml.gz'

	def match_estc(self):
		from lltk.corpus.estc import ESTC
		estc=ESTC()

		self.match_records(estc, id_field_1='ESTCID', id_field_2='id', match_by_id=True, match_by_title=False)

	def match_ravengarside(self):
		from lltk.corpus.ravengarside import RavenGarside
		rg=RavenGarside()
		self.match_records(rg, match_by_title=True)

	def match_eccotcp(self):
		from lltk.corpus.ecco import ECCO_TCP
		etcp = ECCO_TCP()
		self.match_records(etcp, match_by_title=True, match_by_id=True, id_field_1='id_ESTC', id_field_2='id_ESTC')






OCR_CORREX = None
def gale_xml2txt(dom, OK_word=['wd'], OK_page=['bodyPage'], remove_catchwords=True, correct_ocr=False):
	global OCR_CORREX
	

	"""
	Get the plain text from the ECCO xml files.
	OK_word sets the tags that define a word: in ECCO, <wd>.
	OK_page sets the tags that define a page we want:
	- bodyPage are the body pages
	- frontmatter are frontmatter paggers;
		-- I've decided not to include them, but you can add them by adding it to the OK_page list
	"""

	if correct_ocr and not OCR_CORREX: OCR_CORREX = tools.get_ocr_corrections()

	txt=[]
	body = dom.find('text')
	if not body: return ''
	for page in body.find_all('page'):
		if page.get('type','') in OK_page:
			page_txt=[]
			para_txt=[]
			line_txt=[]
			lastParent=None
			lastLineOffset=None
			for tag in page.find_all():
				if tag.name in OK_word:

					## Check for new paragraph
					if tag.parent != lastParent:
						if line_txt:
								para_txt+=[line_txt]
								line_txt=[]
						if para_txt:
							page_txt+=[para_txt]
							para_txt=[]
					lastParent=tag.parent

					## Check for new line
					try:
						lineOffset = int(tag['pos'].split(',')[0])
						if lastLineOffset is None:
							lastLineOffset=lineOffset
						elif lineOffset < lastLineOffset:
							if line_txt:
								para_txt+=[line_txt]
								line_txt=[]
						lastLineOffset = lineOffset
					except (KeyError,ValueError) as e:
						pass

					word=clean_text(tag.text)
					a,w,b  = tools.gleanPunc2(word)
					if correct_ocr and w in OCR_CORREX:
						w=OCR_CORREX.get(w)
						word=a+w+b

					line_txt+=[word]

			if line_txt:
				para_txt+=[line_txt]
			if para_txt:
				page_txt+=[para_txt]
			if page_txt:
				txt+=[page_txt]

	for page_i,page in enumerate(txt):
		#print '>> page:',page_i
		if remove_catchwords:
			last_word_this_page = page[-1][-1][-1]
			first_word_next_page = txt[page_i+1][0][0][0] if len(txt)>page_i+1 else None
			if last_word_this_page == first_word_next_page:
				# if last word is a catchword for first word of next page,
				# remove the last line
				page[-1][-1].pop()

		page = [para for para in page if len(para) and sum(len(line) for line in para)>0]
		txt[page_i] = page

	## otherwise, make plain text
	plain_text = u"\n\n\n".join(u"\n\n".join(u"\n".join(u" ".join(word for word in line) for line in para) for para in page) for page in txt)

	# fix dangling hyphens
	plain_text = fix_dangling_hyphens(plain_text,{'¬','-'})

	return plain_text


def fix_dangling_hyphens(body,hyphens={'¬'}):
	lines = [l.rstrip() for l in body.split('\n')]
	for i,line in enumerate(lines):
		for hyph in hyphens:
			if line.endswith(hyph) and i+1<len(lines) and lines[i+1]:
				first_word_next_line = lines[i+1].split()[0]
				next_words = lines[i+1].split()[1:]
				lines[i] = line[:-1]+first_word_next_line.strip()
				lines[i+1] = ' '.join(next_words)
				break
	return '\n'.join(lines)
