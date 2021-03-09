from lltk.imports import *

class TextOldBailey(Text):
	STANZA_TAGS = ['stanza','versepara','pdiv']
	LINE_TAGS = ['l','lb']

	@property
	def meta_by_file(self,bad_tags={'div1'}):
		print('>>',self.id,'...')
		md={}
		md['id']=self.id
		md['medium']='Dialogue'

		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in bad_tags: [x.extract() for x in dom.findAll(tag)]

		for tag in dom('interp'):
			try:
				md[tag.attrs['type']]=tag.attrs['value']
			except AttributeError:
				pass

		md['year']=int(md['date'][0:4]) # format is: "5: 1737/1744" or "2: 1631" or etc
		return md


	def text_plain(self, dialogue_only=True, BAD=[''], force_xml=None):
		txt=[]
		import bs4
		dom=bs4.BeautifulSoup(self.text_xml,'lxml')
		for tag in BAD: [x.extract() for x in dom.findAll(tag)]
		if dialogue_only or True: # @TODO other option hasn't been implemented yet
			for dtag in dom('u'):
				utt=dtag.text
				utt=utt.replace('\r\n',' ').replace('\r',' ').replace('\n',' ')
				while '  ' in utt: utt=utt.replace('  ',' ')
				utt=utt.strip()
				txt+=[utt]
		txt='\n\n'.join(txt).strip()
		return txt




class OldBailey(Corpus):
	"""
    [This is the PARSED Old Bailey. Full one not implemented yet.]
	"""

	TEXT_CLASS=TextOldBailey

	def load_metadata(self):
		meta=super().load_metadata()
		meta['genre']='Trial'
		meta['title']=''
		return meta