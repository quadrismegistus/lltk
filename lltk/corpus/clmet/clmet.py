from lltk.imports import *

# genre2Genre={u'Drama':'Drama',
# 	 u'LET':'Letter',
# 	 u'Narrative fiction':'Fiction',
# 	 u'Narrative non-fiction':'Biography',
# 	 u'Other':'Other',
# 	 u'Treatise':'Treatise'}

class TextCLMET(Text): pass
	

	# @property
	# def meta_by_file(self,bad_tags={'html','body'}):
	# 	import codecs,bs4,os
	# 	md={}
	# 	md['medium']='Prose'

	# 	txt=self.text_xml
	# 	meta = txt.split('<text>')[0]
	# 	dom=bs4.BeautifulSoup(meta,'lxml')
	# 	for tag in dom():
	# 		if tag.name in bad_tags: continue
	# 		md[tag.name]=tag.text

	# 	md['id_orig']=md.get('id','')
	# 	md['id']=self.id
	# 	md['genre']=self.genre2Genre.get(md.get('genre'),'')
	# 	return md


class CLMET(Corpus): pass