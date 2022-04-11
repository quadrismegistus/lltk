from lltk.imports import *





def is_wikidata_key(key):
    return '|P' in key or '|Q' in key or key.startswith('wd_author') or key.startswith('wd_title') or key in {'query','qid'}

def is_valid_wikidata(meta):
    if type(meta)!=dict or not meta: return False
    if not 'what' in meta: return False
    for k in meta.keys():
        if '|P' in k or '|Q'in k:
            return True
    return False



def init_wikidata(
        id,
        force=False,
        keys_nec={'wd_title','wd_author'},
        corpus_id='wikidata',
        **kwargs):
    text = Text(id)
    tmeta=text.metadata(from_sources=False,**kwargs)
    qid=get_qid(text,tmeta)
    ometa={**tmeta, **kwargs}
    
    # cache this
    ometa['id']=qid
    ometa['corpus']=corpus_id
    otext = Text(**ometa)

    otext.add_source(text)
    text.add_source(otext)

    text.cache_meta_json()
    otext.cache_meta_json()
    
    return otext





def get_qid(*x,**y): return get_wikidata_id(*x,**y)

def get_wikidata_id(
        text=None,
        meta={},
        idkey='_id_wikidata',
        addrkey='_addr_wikidata',
        **kwargs
    ):
    text=Text(text)
    tmeta = text.metadata(from_sources=False) if not meta and text is not None else meta
    Qid=''
    Qid = tmeta.get(idkey,Qid)
    if not Qid: Qid = tmeta.get(addrkey,'').split('/')[-1]
    if not Qid: Qid = query_get_wikidata_id(text,**kwargs)
    

    # # add back to text?
    # changed=False
    # for k,v in Qmeta.items():
    #     if k.startswith('wd_') or k.startswith('t_'):
    #         text._meta[k]=v
    #         changed=True
    # if changed: text.cache_meta_json()

    return Qid

def query_get_wikidata_id(
        text,
        qstr='',
        lang="en",
        what={"work","manuscript","text"},
        min_match_ratio_au=50,
        min_match_ratio_ti=50,
        timeout=30,
        verbose=True,
        **kwargs):

    if not qstr: qstr=wikidata_query_str(text)
    if verbose: log.debug('Querying wikidata: '+qstr)
    
    for qid in query_iter_wikidata_id(qstr):
        qid_meta = get_wikidata_from_qid(qid)
        qid_what = qid_meta.get('what','')
        # plog(f'QID: {qid} [{qid_what}]')
        
        # filter?
        if what:
            if not any(whatx in qid_what for whatx in what):
                continue
        
        # match?
        if min_match_ratio_au or min_match_ratio_ti:
            qid_meta = match_au_ti(text,qid_meta)
            if min_match_ratio_au and qid_meta.get('wd_author_match',0) < min_match_ratio_au: continue
            if min_match_ratio_ti and qid_meta.get('wd_title_match',0) < min_match_ratio_ti: continue
        return (qid,qid_meta)    
    return '',{}

def query_iter_wikidata_id(qstr,timeout=30,**kwargs):
    import requests,bs4
    ## Querying
    safe='+'.join(clean_text(qstr).split())
    url=f'https://www.wikidata.org/w/index.php?search={safe}'
    with requests.Session() as s:
        html = s.get(url,timeout=timeout).text
    dom=bs4.BeautifulSoup(html,'html')

    qid=''
    for item in dom.select('.wb-itemlink-id'):
        itext=item.text
        qid=itext.replace('(','').replace(')','')
        yield qid


def match_au_ti(text,qmeta):
    from fuzzywuzzy import fuzz

    t_au = qmeta['t_author'] = text.au
    t_ti = qmeta['t_title'] = text.shorttitle

    wd_au = qmeta['wd_author']=wikidata_get_author(qmeta)
    wd_ti = qmeta['wd_title']=wikidata_get_title(qmeta)

    qmeta['wd_author_match'] =fuzz.token_set_ratio(wd_au, t_au)
    qmeta['wd_title_match'] = fuzz.token_set_ratio(wd_ti, t_ti)

    return qmeta





def query_get_wikidata(qid,verbose=False,**kwargs):
    import wptools
    page = wptools.page(wikibase=qid, silent=not verbose)
    wpage = page.get_wikidata()
    wdata = wpage.data
    odat = wdata.get('wikidata')
    ods = format_wikidata_d_simple(odat)
    return dict(
        id=qid,
        label=wdata.get('label',''),
        what=wdata.get('what',''),
        # wd_author=wikidata_get_author(ods),
        # wd_title=wikidata_get_title(ods),
        **ods
    )


def get_wikidata_from_qid(qid,tmeta={},cache=True,**kwargs):
    t=Text(qid,'wikidata')
    if not tmeta: tmeta=t.metadata(from_sources=False,from_cache=True,cache=False)
    if not is_valid_wikidata(tmeta): tmeta=query_get_wikidata(t,**kwargs)
    if is_valid_wikidata(tmeta): t.cache_meta_json(tmeta)
    
    return tmeta

    


class TextWikidataClass(BaseText):
    
    def is_valid(self,meta=None):
        if meta is None: meta=self._meta
        metakeys=set(meta.keys()) - {self.corpus.addr, self.corpus.id}
        return self.id and self.id[0] in {'P','Q'} and len(metakeys)
    
    @property
    def query_str(self): return wikidata_query_str(self)

    def query(self): return get_wikidata(self.id)

    def cache_keys(self,meta=None,meta_sources={},**kwargs):
        if meta is None: meta=self._meta
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

    # Wiki
    def metadata(self,meta={},from_cache=True,force=False,cache=True,**kwargs):
        # get meta
        if not meta: meta=self._meta
        if from_cache: meta={**meta, **self.init_meta_json()}
        if force or not is_valid_wikidata(meta):
            self._meta=meta={**meta, **query_get_wikidata(self.id,**kwargs)}
            if cache and is_valid_wikidata(meta): self.cache_meta_json(meta)
        return meta


        

def wikidata_query_str(text):
    return clean_text(f'{text.au}, {text.shorttitle}')

def format_wikidata_str_simple(qid,name,lower=False,spaces=True,**kwargs):
    name = name.strip()
    qid = qid.strip()
    if not spaces: name = name.replace(' ','_')
    if lower: name=name.lower()
    return f'{name}|{qid}' if qid else name

def format_wikidata_str(o,simple=False,**kwargs):
    if type(o)==str:
        if '(' in o and o.endswith(')'):
            name,qid = o[:-1].split('(',1)
            name,qid = name.strip(),qid.strip()
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


def wikidata_get_title(d): return wikidata_get_prop(d,'P1476','title').replace('_',' ')
def wikidata_get_author(d): return wikidata_get_prop(d,'P50','author').replace('_',' ')

def wikidata_get_prop(d,prop='',propname='',keep_prop=False):
    for k,v in d.items():
        if type(k)!=str: continue
        if (prop and prop in k) or (propname and propname in k):
            return v if keep_prop else v.split('|')[0]


def format_wikidata_d_simple(d):
    from unidecode import unidecode
    od={}
    for k,v in d.items():
        key=format_wikidata_str(k,simple=True,lower=True,spaces=False)
        key=unidecode(key).replace("'","").replace('"','')
        
        val = format_wikidata_str(v,simple=True,lower=False,spaces=True)
        
        od[key] = val
    
    return od





class Wikidata(BaseCorpus):
    NAME='Wikidata'
    ID='wikidata'
    TEXT_CLASS=TextWikidata

    def init(self,force=False,by_files=True,**kwargs):
        self._init = True
        #if force or not self._init:
        #    super().init(by_files=by_files,**kwargs)
            

    def metadata(self,texts=None,allow_http=True,fillna='',progress=True,**kwargs):
        #return super().metadata(allow_http=allow_http,**kwargs)
        o=[]
        okeys=set()
        iterr=get_tqdm(
            self.texts(texts),
            desc='Finding metadata',
            disable=not progress,
        )
        for t in iterr:
            ometa=t.metadata(allow_http=allow_http,**kwargs)
            if type(ometa)==dict and ometa and self.col_id in ometa:
                idx=ometa.get(self.col_id)
                numkeys = len(ometa) if ometa.get('id')!='Q0' else 0
                iterr.set_description(f'''Queried wikidata: "{ometa.get('query')}" ... found {numkeys} data points''')
                if not idx in okeys:
                    o.append(ometa)
                    okeys|={idx}

        odf=pd.DataFrame(o)
        if fillna is not None: odf=odf.fillna(fillna)
        if self.col_id in set(odf.columns): odf=odf.set_index(self.col_id)
        return odf

    def null_text(self,id_null='Q0',**kwargs):
        return super().null_text(id_null=id_null,**kwargs)

        
def wikidata_query_str(text):
    return clean_text(f'{text.au}, {text.shorttitle}')

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
        
        od[key] = val
    
    return od










def get_wikidata_from_meta(
        tmeta,
        idkey='_id_wikidata',
        addrkey='_addr_wikidata'):
    Qid=''
    Qaddr = tmeta.get(addrkey,'').split('/')[-1]
    Qid = tmeta.get(idkey,Qid)
    if not Qid: Qid = Qaddr
    return Qid


def get_wikidata_id(
        text=None,
        meta={},
        **kwargs
    ):
    text=Text(text) if not is_text_obj(text) else text
    tmeta = text.metadata(from_sources=False) if not meta and text is not None else meta
    # plog(tmeta)
    Qid = get_wikidata_from_meta(tmeta,**kwargs)
    # plog(Qid)
    if not Qid: Qid = query_get_wikidata_id(text,**kwargs)
    # plog(Qid)
    if Qid:
        okey=f'_{text.corpus.col_id}_wikidata'
        if okey not in text._meta:
            text._meta[okey]=Qid
        text.cache_meta_json()
    # plog(Qid)
    
    return Qid
    


    # return Qid

    # # add back to text?
    # changed=False
    # for k,v in Qmeta.items():
    #     if k.startswith('wd_') or k.startswith('t_'):
    #         text._meta[k]=v
    #         changed=True
    # if changed: text.cache_meta_json()

    # return Qid



def query_get_wikidata_id(
        text,
        qstr='',
        lang="en",
        what={"work","manuscript","text"},
        min_match_ratio_au=50,
        min_match_ratio_ti=50,
        timeout=30,
        verbose=True,
        **kwargs):

    if not qstr: qstr=wikidata_query_str(text)
    if verbose: log.debug('Querying wikidata: '+qstr)
    
    for qid in query_iter_wikidata_id(qstr):
        qid_meta = query_get_wikidata(qid)
        qid_what = qid_meta.get('what','')
        plog(f'QID: {qid} [{qid_what}]')
        # plog(qid_meta)
        # filter?
        if what:
            if not any(whatx in qid_what for whatx in what):
                continue
        
        plog(f'QID returned: {qid} [{qid_what}]')
        return qid
        # match?

        # if min_match_ratio_au or min_match_ratio_ti:
        #     qid_meta = match_au_ti(text,qid_meta)
        #     if min_match_ratio_au and qid_meta.get('wd_author_match',0) < min_match_ratio_au: continue
        #     if min_match_ratio_ti and qid_meta.get('wd_title_match',0) < min_match_ratio_ti: continue
        #     return qid
        # #return (qid.replace('(','').replace(')',''),qid_meta)    
    return ''

def query_iter_wikidata_id(qstr,timeout=30,**kwargs):
    import requests,bs4
    ## Querying
    safe='+'.join(clean_text(qstr).split())
    url=f'https://www.wikidata.org/w/index.php?search={safe}'
    with requests.Session() as s:
        html = s.get(url,timeout=timeout).text
    log.debug(f'Received HTML: {len(html)}')
    dom=bs4.BeautifulSoup(html,'html')

    qid=''
    for item in dom.select('.wb-itemlink-id'):
        itext=item.text
        qid=itext.replace('(','').replace(')','')
        log.debug(f'QID: {qid}')
        yield qid




class TextWikidataClass(BaseText):
    
    def is_valid(self,meta=None):
        if meta is None: meta=self._meta
        metakeys=set(meta.keys()) - {self.corpus.addr, self.corpus.id}
        return self.id and self.id[0] in {'P','Q'} and len(metakeys)
    
    @property
    def query_str(self): return wikidata_query_str(self)

    def query(self): return get_wikidata(self.id)

    def cache_keys(self,meta=None,meta_sources={},**kwargs):
        if meta is None: meta=self._meta
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

    # Wiki
    def metadata(self,meta={},from_cache=True,force=False,cache=True,**kwargs):
        # get meta
        if not meta: meta=self._meta
        if from_cache: meta={**meta, **self.init_meta_json()}
        if force or not is_valid_wikidata(meta):
            self._meta=meta={**meta, **query_get_wikidata(self.id,**kwargs)}
            if cache and is_valid_wikidata(meta): self.cache_meta_json(meta)
        return meta



def TextWikidata(text):
    from lltk.corpus.wikidata import get_wikidata_id

    text = Text(text)
    qid = get_wikidata_id(text)
    qtext = Corpus('wikidata').text(qid)
    text.add_source(qtext)
    qtext.add_source(text)

    return qtext

    