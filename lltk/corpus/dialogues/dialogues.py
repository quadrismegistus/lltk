from lltk.imports import *

class TextEnglishDialogues(Text):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']

	@property
	def meta_by_file(self,bad_tags={'textbibliography'}):
		md={}
		md['id']=self.id
		md['medium']='Dialogue'
		#num_lines=0
		#num_stanzas=0
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		header=dom('dialogueheader')[0]
		for tag in header:
			try:
				"""
				print tag
				print tag.name
				print tag.attrs
				print tag.text
				print '##'*100
				print
				"""
				if tag.name in bad_tags: continue
				md[tag.name]=tag.text
				for k,v in list(tag.attrs.items()):
					md[tag.name+'_'+k]=v

			except AttributeError:
				pass

		md['year']=int(md['speechpubdate'][3:7]) # format is: "5: 1737/1744" or "2: 1631" or etc
		return md


	def text_plain(self, dialogue_only=True, BAD=['comment']):
		txt=[]
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in BAD: [x.extract() for x in dom.findAll(tag)]
		if dialogue_only or True: # @TODO other option hasn't been implemented yet
			#txt='\n\n'.join([x.text.replace('\r\n',' ').replace('\r',' ').replace('\n',' ').replace('  ',' ').replace('  ',' ').strip() for x in dom('dialogue')])
			for dtag in dom('dialogue'):
				utt=dtag.text
				utt=utt.replace('\r\n',' ').replace('\r',' ').replace('\n',' ')
				while '  ' in utt: utt=utt.replace('  ',' ')
				utt=utt.strip()
				txt+=[utt]
		txt='\n\n'.join(txt).strip()
		return txt



class EnglishDialogues(Corpus):
	TEXT_CLASS=TextEnglishDialogues
