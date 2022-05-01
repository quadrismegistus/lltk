from lltk.imports import *




class EntityWikidata(BaseText):
    def __init__(self,id=None,_corpus=None,**kwargs):
        super().__init__(
            id=id,
            _corpus=Corpus('wikidata') if not _corpus else _corpus,
            **kwargs
        )
    
    def is_valid(self,meta=None): return self.id_is_valid() and self.meta_is_valid(meta)
    
    def id_is_valid(self,id=None,bad_ids={NULL_QID}):
        if id is None: id=self.id
        return is_valid_qid(id) and id not in bad_ids
    
    def meta_is_valid(self,meta=None,**kwargs):
        if meta is None: meta=self._meta #metadata(from_query=False,from_sources=False,**kwargs)
        isv=is_valid_meta(meta)
        # log(f'Valid? = {isv}')
        # log(f'Meta = {meta}')
        return isv
        

    @property
    def query_str(self): return wikidata_query_str(self)

    def query(self,force=False,**kwargs):
        if self.id_is_valid() and not self.meta_is_valid():
            if log>0: log(f'QUERYING: {self.id}')
            return query_get_wikidata(self.id,**kwargs)
        return {}

    def cache_keys(self,meta=None,meta_sources={},**kwargs):
        if meta is None: meta=self.meta
        metakeys=set(meta.keys()) - self.meta_keys_sources(meta_sources,**kwargs)
        return metakeys


    def meta_keys_sources(self,meta_sources=None,**kwargs): 
        if meta_sources is None:
            meta_sources=set(self.metadata_sources(**kwargs).keys())
            return meta_sources
        return set()


    def metadata_sources(self,from_sources=True,**kwargs):
        return super().metadata(
            from_sources=False,
            **kwargs
        )






###
### GETTING ID ###
###

def is_valid_qid(qid):
    if not qid or type(qid)!=str: return False
    qid=qid.strip()
    if qid == NULL_QID: return False
    if not qid or qid[0]!='Q' or qid=='Q': return False
    return True

def get_wikidata_id_from_sources(text,null_qid=NULL_QID):
    return get_source_recursive(text,corpus_id='wikidata',null_qid=null_qid)

def get_all_sources_recursive(text,sofar=set(),**kwargs):
    sources = text.get_sources(**kwargs)
    for src in text.sources:
        if log>0: log(f'{text} --?--> {src}')
        sofar|=get_all_sources_recursive(src,sofar=sofar|{text,src})
    sofar|={text} | set(sources)
    return sofar

def get_source_recursive(text,**kwargs):
    kwargs['wikidata']=False
    for src in text.get_sources(**kwargs):
        if is_wiki_text_obj(src):
            return src

def get_wiki_text_from_sources(sources,verbose=2,force=False,**kwargs):
    for t in sources:
        if is_wiki_text_obj(t):
            if t.id_is_valid():
                if log>1: log('Returning valid wikidata textobj from sources')
                return t
            
            if not force:
                if log>1: log('Will not force a retry')
                return t

def is_wiki_text_obj(x): return is_text_obj(x) and x.corpus.id=='wikidata'



def query_get_wikidata_id(
        text,
        qstr='',
        what={"work","manuscript","text"},
        lim=10,
        # cache=False,
        null=(NULL_QID,{}),
        **kwargs):
    if not qstr: qstr=wikidata_query_str(text)
    if not qstr or qstr=='None, None': return
    if log>0: log(f'Querying for ID: '+qstr)
    
    for qi,qidx in enumerate(query_iter_wikidata_id(qstr)):
        if qi>=lim: return null
        qid_meta = {k:v for k,v in Corpus('wikidata').get_text_cache_json(qidx).items() if not '__' in k and k!='id'}
        if qid_meta: 
            if log>0: log(f'qid_meta from db: {len(qid_meta)} keys')
        else:
            qid_meta = query_get_wikidata(qidx)
            if log>0: log(f'qid_meta from query: {len(qid_meta)} keys')
        
        qid_meta[COL_ID] = qidx
        try:
            Corpus('wikidata').text(_cache=True, **qid_meta)
            qid_what = qid_meta.get('what','')
            if not what or any(whatx in qid_what for whatx in what):
                return (qidx,qid_meta)
        except Exception as e:
            log.error(e)
            pass

WIKIQDB=None
def query_db():
    global WIKIQDB
    if WIKIQDB is None:
        dbfn=os.path.join(Corpus('wikidata').path_data, 'query_cache')
        WIKIQDB=DB(dbfn)
    return WIKIQDB


def get_query_db_fn():
    return os.path.join(Corpus('wikidata').path_data, 'qstr2qids.json')

def query_get_db(qstr):
    # res = read_json(get_query_db_fn())
    res=query_db().get(qstr)
    if res is not None and log.verbose>0: log(f'found result in db for "{qstr}": {res}')
    return res
def query_set_db(qstr,res):
    # if write_json(res, get_query_db_fn()):
    if query_db().set(qstr,res):
        if log>0: log(f'set result in db for "{qstr}": {pf(res)}')
        return True


def query_get_html(qstr,timeout=10,**kwargs):
    import requests
    from requests.exceptions import ConnectTimeout
    from urllib.parse import quote_plus
    
    sstr=quote_plus(clean_text(qstr))
    safeurl=f'https://www.wikidata.org/w/index.php?search={sstr}&ns0=1&ns120=1'
    html=gethtml(safeurl,timeout=timeout)
    return html


def query_iter_qids_from_html(html):
    import bs4
    dom=bs4.BeautifulSoup(html,'lxml')
    res=list(dom.select('.mw-search-result'))
    # log(f'Found {len(res)} items')
    for tag in res:
        item=tag.select_one('.wb-itemlink-id')
        itext=item.text
        qid=itext.replace('(','').replace(')','')
        yield qid

def query_iter_wikidata_id(qstr,timeout=10,**kwargs):
    qidl = query_get_db(qstr)
    if qidl is None:
        html = query_get_html(qstr,timeout=timeout,**kwargs)
        qidl = list(query_iter_qids_from_html(html))
        query_set_db(qstr,qidl)
    return qidl


def query_get_wikidata(qid,verbose=False,**kwargs):
    try:
        if log>0: log(f'Querying for data: {qid}')
        import wptools
        page = wptools.page(wikibase=qid, silent=not verbose)
        wpage = page.get_wikidata()
        wdata = wpage.data
        odat = wdata.get('wikidata')
        ods = format_wikidata_d_simple(odat)
        odx=dict(
            id=qid,
            label=wdata.get('label',''),
            what=wdata.get('what',''),
            # wd_author=wikidata_get_author(ods),
            # wd_title=wikidata_get_title(ods),
            **ods
        )
        # log(f'Returning data with {len(odx)} keys')
        return odx
    except Exception as e:
        log.error(f'Could not get wikidata [{e}]')
        return dict(id=qid)


    



def is_wikidata_key(key):
    return '|P' in key or '|Q' in key or key.startswith('wd_author') or key.startswith('wd_title') or key in {'query','qid'}

def is_valid_wikidata(meta):
    if type(meta)!=dict or not meta: return False
    # if not 'what' in meta: return False
    for k in meta.keys():
        if '|P' in k or '|Q'in k:
            return True
    return False

def is_valid_meta(meta): return is_valid_wikidata(meta)





def match_au_ti(text,qmeta):
    from fuzzywuzzy import fuzz

    t_au = qmeta['t_author'] = text.au
    t_ti = qmeta['t_title'] = text.shorttitle

    wd_au = qmeta['wd_author']=wikidata_get_author(qmeta)
    wd_ti = qmeta['wd_title']=wikidata_get_title(qmeta)

    qmeta['wd_author_match'] =fuzz.token_set_ratio(wd_au, t_au)
    qmeta['wd_title_match'] = fuzz.token_set_ratio(wd_ti, t_ti)

    return qmeta




        

        
def wikidata_query_str(text):
    return clean_text(f'{text.shorttitle} {text.au}')

def format_wikidata_str_simple(qid,name,lower=False,spaces=True,**kwargs):
    name = name.strip()
    qid = qid.strip()
    if not spaces: name = name.replace(' ','_')
    if lower: name=name.lower()
    return f'{name}|{qid}' if qid else name

def format_wikidata_str(o,simple=False,**kwargs):
    if type(o)==str:
        if '(' in o and o.endswith(')'):
            ox=o[:-1]
            qid=ox.split('(')[-1].strip()
            name='('.join(ox.split('(')[:-1]).strip()
            if qid and qid[0] in {'Q','P'}:
                return (qid,name) if not simple else format_wikidata_str_simple(qid,name,**kwargs)
        return ('',o.strip()) if not simple else o.strip()
    elif type(o)==list:
        return [format_wikidata_str(x,simple=simple) for x in o]
    elif type(o)==tuple:
        if len(o)==2:
            qid,name = o
            return (qid,name) if not simple else format_wikidata_str_simple(qid,name,**kwargs)
        return tuple([format_wikidata_str(x,simple=simple) for x in o])
    elif type(o)==dict:
        return format_wikidata_d(o,simple=simple)
    return ('',o)

def format_wikidata_d(d,simple=False):
    od={}
    for k,v in d.items():
        od[format_wikidata_str(k,simple=simple)] = format_wikidata_str(v,simple=simple)
    return od


def format_wikidata_d_simple(d):
    from unidecode import unidecode
    od={}
    for k,v in d.items():
        key=format_wikidata_str(k,simple=True,lower=True,spaces=False)
        key=unidecode(key).replace("'","").replace('"','')
        
        val = format_wikidata_str(v,simple=True,lower=False,spaces=True)
        # if type(val) in {list}: val='; '.join(str(x) for x in val)
        od[key] = val
    
    return od










def get_wikidata_from_meta(
        tmeta,
        idkey='id__wikidata',
        addrkey='_addr_wikidata',
        verbose=True,
        **kwargs):
    Qaddr = tmeta.get(addrkey,'').split('/')[-1]
    Qid = tmeta.get(idkey,'')
    if not Qid: Qid = Qaddr
    return Qid if Qid and is_valid_qid(Qid) else NULL_QID
















        
## CORPUS FUNCS
def wikidata(self,**kwargs): return list(self.iter_wikidata(**kwargs))    
def iter_wikidata(self,texts=[],force=True,**kwargs):
    stop
    for t in self.corpus_texts(texts=texts,**kwargs):
        tw=t.wikidata(force=force,**kwargs)
        yield tw
            

BaseCorpus.wikidata=wikidata
BaseCorpus.iter_wikidata=iter_wikidata





## TEXT FUNCS

@property
def wiki(self): return self.wikidata()


def wikidata(self,sources=None,force=False,force_inner=None,**kwargs):
    from lltk.corpus.wikidata import is_wiki_text_obj

    # if type(self._wikidata) == str and self._wikidata:
    #     self._wikidata=Text(self._wikidata)

    if force or not is_wiki_text_obj(self._wikidata):
        if log>0: log('...')
        from lltk.corpus.wikidata import TextWikidata
        self._wikidata = TextWikidata(
            self,
            _sources=sources,
            force=force_inner if force_inner is not None else force,
            cache=True,
            **kwargs
        )			
    return self._wikidata


def get_remote_sources(self,sources=None,wikidata=True,**kwargs):
    if log>0: log('...')
    sofar=set()
    if wikidata:
        wiki=self.wikidata(sources,**kwargs)
        if wiki is not None:
            sofar|={wiki}
    return sofar - {self}


BaseText.wiki = wiki
BaseText.wikidata=wikidata
BaseText.get_remote_sources=get_remote_sources
