
import os,json
from tqdm import tqdm
from lltk import tools
from lltk.text import Text
from lltk.corpus.tcp import TextTCP,TextSectionTCP,extract_metadata
from lltk.corpus import Corpus
from lltk.corpus.tcp import TCP


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
    


class ECCO_TCP(TCP):
    TEXT_CLASS=TextECCO_TCP
    TEXT_SECTION_CLASS=TextSectionTCP

    @property
    def metadata(self):
        meta=super().metadata
        return meta.query('1700<=year<1800')

    def compile_extract(self):
        # extract all
        for root,dirs,fns in tqdm(sorted(os.walk(self.path_raw)),desc='Extracting archives'):
            for fn in fns:
                fnfn=os.path.join(root,fn)
                tools.extract(fnfn,dest=root,progress=False,overwrite=False)

    def compile_texts(self):
        for root,dirs,fns in tqdm(sorted(os.walk(self.path_raw)),desc='Moving and renaming texts'):
            for fn in fns:
                if fn.startswith('K'):
                    idx,volnum,ext=fn.split('.')
                    ofn='meta.'+ext if ext=='hdr' else 'text.'+ext
                    opath=os.path.join(self.path_texts,idx,volnum)
                    if not os.path.exists(opath): os.makedirs(opath)
                    ofnfn=os.path.join(opath,ofn)
                    ifnfn=os.path.join(root,fn)
                    os.rename(ifnfn,ofnfn)

    def compile_meta(self):
        for root,dirs,fns in tqdm(sorted(os.walk(self.path_texts)),desc='Building metadata'):
            for fn in fns:
                if fn=='meta.hdr':
                    with open(os.path.join(root,fn)) as f, open(os.path.join(root,'..','meta.json'),'w') as of, open(os.path.join(root,'meta.json'),'w') as of_vol:
                        meta=extract_metadata(f.read())
                        meta['_type']='text'
                        meta['_id']=root.split(os.path.sep)[-2]
                        meta_vol={'id_TCP':meta['id_TCP']}
                        meta_vol['_type']='volume'
                        meta_vol['_id']=os.path.sep.join(root.split(os.path.sep)[-2:])
                        del meta['id_TCP']
                        json.dump(meta,of,indent=4)
                        json.dump(meta_vol,of_vol,indent=4)

    def compile(self):
        # download and unzip raw
        self.compile_download()

        # extract archives
        self.compile_extract()

        # build text dirs
        self.compile_texts()

        # build metadata
        self.compile_meta()
        
        # save metadata
        self.save_metadata()
        
        # save plain text
        self.save_txt()

    ###
    def save_metadata(self):
        return self.save_metadata_from_meta_jsons()
    
    def split_sections(self):
        pass
