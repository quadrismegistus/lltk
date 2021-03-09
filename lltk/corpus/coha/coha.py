from lltk.imports import *

class TextCOHA(Text):
	def text_plain(self):
		txt=super().text_plain()
		txt = txt.replace('@ @ @ @ @ @ @ @ @ @','\n\n')
		return txt



class COHA(Corpus):
	TEXT_CLASS=TextCOHA
	def update_metadata(self):
		super(COHA,self).save_metadata(num_words=True,ocr_accuracy=True)
	def save_metadata(self):
		ld=tools.read_ld(self.PATH_ORIG_SOURCE_METADATA)
		# fix empty column and other things
		genre2Genre = {u'FIC':'Fiction', u'MAG':'Magazine', u'NEWS':'News', u'NF':'Non-Fiction'}
		for d in ld:
			d['note']=d['']
			del d['']
			d['medium']='Prose'
			# do genre assignments
			d['genre']=genre2Genre[d['genre']] # clean genre
			if d['note']=='[Movie script]':
				d['genre']='Film'
			elif d['note']=='[Play script]':
				d['genre']='Drama'
		# Align new and original ID's
		dd=tools.ld2dd(ld,'textID')
		for t in self.texts(from_files=True):
			textID=t.id.split('_')[-1]
			if textID in dd:
				dd[textID]['id']=t.id

		# Filter out any metadata rows that didn't correspond to files
		ld = [d for d in ld if 'id' in d and d['id']]

		timestamp=tools.now().split('.')[0]
		# tools.write2(os.path.join(self.path,'corpus-metadata.%s
		# .%s.txt' % (self.name,timestamp)), ld)