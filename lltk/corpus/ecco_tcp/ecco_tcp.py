from lltk.imports import *
from lltk.corpus.tcp import *
from lltk.corpus.eebo_tcp.eebo_tcp import _normalize_estc_id

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
    LINKS = {'estc': ('id_ESTC', 'id_estc')}
    LINK_TRANSFORMS = {'id_ESTC': _normalize_estc_id}

    @property
    def path_hdr(self):
        return os.path.join(self.path_root,'hdr')

    def compile(self):
        self.compile_download()
        self.compile_extract(extract_in=['XML','headers'])
        self.compile_texts(fn_startswith='K',exts={'hdr','xml'})
        self.compile_metadata(path_meta=self.path_hdr,exts={'hdr'})

    @property
    def path_metadata_enriched(self):
        return os.path.join(self.path, 'metadata_enriched.parquet')

    def load_metadata(self,*x,**y):
        # Fast path: enriched parquet cache
        enriched_path = self.path_metadata_enriched
        force = y.get('force', False)
        if not force and os.path.exists(enriched_path) and os.path.exists(self.path_metadata):
            if os.path.getmtime(enriched_path) >= os.path.getmtime(self.path_metadata):
                try:
                    meta = pd.read_parquet(enriched_path)
                    if self.col_id in meta.columns:
                        meta = meta.set_index(self.col_id)
                    return meta
                except Exception:
                    pass

        meta=super().load_metadata(*x,**y)
        if 'pubplace' in meta.columns:
            meta['pubcity']=meta.pubplace.apply(lambda x: zeropunc(x).strip().split()[0])
        meta = self.merge_linked_metadata(meta)
        if 'genre' in meta.columns:
            meta['medium'] = meta['genre']
        if 'estc_genre' in meta.columns:
            meta['genre'] = meta['estc_genre']
        else:
            meta['genre'] = None
        if 'estc_genre_raw' in meta.columns:
            meta['genre_raw'] = meta['estc_genre_raw']
        else:
            meta['genre_raw'] = None
        if 'medium' in meta.columns:
            meta.loc[meta['medium'] == 'Verse', 'genre'] = 'Poetry'
            meta.loc[meta['medium'] == 'Drama', 'genre'] = 'Drama'
        if 'estc_is_translated' in meta.columns:
            meta['is_translated'] = meta['estc_is_translated']

        try:
            meta.to_parquet(enriched_path)
        except Exception:
            pass

        return meta
