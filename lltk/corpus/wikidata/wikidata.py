from lltk.imports import *
from lltk.model.wikidata import *



class TextWikidataClass(EntityWikidata): pass
def NullTextWikidata(*x,**y): return Corpus('wikidata').text(NULL_QID)

def TextWikidata(text,_sources=None,force=False,cache=True,verbose=2,remote=True,*args,**kwargs):
    if is_wiki_text_obj(text): return text
    if not force and is_wiki_text_obj(text._wikidata): return text._wikidata

    # from sources?
    sources = ([] if not _sources else list(_sources)) + list(text._sources)
    if sources:
        res = get_wiki_text_from_sources(sources,force=force,verbose=verbose,**kwargs)
        if is_wiki_text_obj(res): return res
    
    # gen?
    qtext = NullTextWikidata()
    res = query_get_wikidata_id(text,**kwargs)
    if type(res)==tuple and len(res)==2:
        qid,qmeta=res
        #qmeta['id']=qid
        #twiki = TextWikidataClass(**qmeta) if is_valid_qid(qid) else NullTextWikidata()f
        qtext = Text(qid, 'wikidata', **qmeta)
        if log.verbose>0: log(f'Generated wiki text: {qtext}')
    
    if is_wiki_text_obj(qtext):
        if qtext.id_is_valid(): text.add_source(qtext,cache=True,viceversa=True,**kwargs)
        if qtext.is_valid() and cache: qtext.metadata(remote=False,cache=True,from_sources=True)
        #text.metadata(remote=False,cache=True,from_sources=True)        
    return qtext




class Wikidata(BaseCorpus):
    NAME='Wikidata'
    ID='wikidata'
    TEXT_CLASS=TextWikidataClass

    def init(self,force=False,by_files=True,**kwargs): self._init = True

