from lltk.imports import *
from lltk.corpus.tcp import *

class TextECCO_TCP(TextTCP):
    @property
    def meta_by_file(self):
        if not hasattr(self,'_meta'):
            fnfn=self.path_header
            mtxt=codecs.open(fnfn,encoding='utf-8').read()
            md=self.extract_metadata(mtxt)
            md['fnfn_xml']=self.fnfn
            md['id']=self.id
            md['genre'],md['medium']=self.return_genre()
            del md['notes']
            self._meta=md
        return self._meta


def clean_text(txt):
    txt=txt.replace(u'\u2223','') # weird kind of | character
    txt=txt.replace(u'\u2014','-- ') # em dash
    return txt


def fix_genre(genre,title):
    if genre in {'Verse'}: return genre
    title_l=title.lower()
    if 'essay' in title_l: return 'Essay'
    if 'treatise' in title_l: return 'Treatise'
    if 'sermon' in title_l: return 'Sermon'
    if 'letters' in title_l: return 'Letters'
    if 'proclamation' in title_l: return 'Government'
    if 'parliament' in title_l: return 'Government'
    return genre



## Corpus ####
class ECCO_TCP(TCP):
    EXT_XML = '.xml'
    TEXT_CLASS=TextECCO_TCP


    @property
    def path_hdr(self):
        return os.path.join(self.path_root,'hdr')

    def compile(self):
        self.compile_download()
        self.compile_extract(extract_in=['XML','headers'])
        self.compile_texts(fn_startswith='K',exts={'hdr','xml'})
        self.compile_metadata(path_meta=self.path_hdr,exts={'hdr'})

    def load_metadata(self,*x,**y):
        meta=super().load_metadata(*x,**y)
        if 'pubplace' in meta.columns:
            meta['pubcity']=meta.pubplace.apply(lambda x: zeropunc(x).strip().split()[0])
        return meta
