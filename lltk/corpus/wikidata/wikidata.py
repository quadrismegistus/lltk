from lltk.imports import *
from lltk.text.text import BaseText


class TextWikidata(BaseText):

    def is_valid(self):
        self.init()
        return self.id and self.id[0] in {'P','Q'}
    
    def metadata(self,force=False,cache=True,allow_http=True,**kwargs):
        # if not force:
        #     o=self.init_meta_json()
        #     if o: return o
        
        if allow_http and ((force or not 'query' in self._meta)) or (cache and not os.path.exists(self.path_meta_json)):
            text = self.source
            if text is None: return
            try:
                qstr=wikidata_query_str(text)
                with Capturing() as o:
                    res = wikidata_search(text, **kwargs)
            except Exception as e:
                #log.error(f'Wikidata query failed: {qstr} ({e})')
                self._meta={'query':qstr}
                res = None

            qid='Q0'
            if res is not None:
                # self.id=res['id']
                self._meta = res['meta_simple']
                self._meta_full = res['meta']
                for k,v in res.items():
                    if type(v) not in {dict}:
                        self._meta[k] = v
                for x in ['author','title','year']:
                    if x in self._meta and type(self._meta[k]) in {list,tuple}:
                        self._meta[x]=self._meta[x][0] if len(self._meta[x]) else ''
                qid=res.get('qid')
            
            self.id = qid
            

            self.source._meta[f'_id_wikidata']=self.id
            self.source.cache_meta_json()
            self.cache_meta_json()

        #if self.is_valid():
        self._meta[self.corpus.col_id]=self.id
        self._meta[self.corpus.col_addr]=self.addr

        return self._meta

class Wikidata(BaseCorpus):
    NAME='Wikidata'
    ID='wikidata'
    TEXT_CLASS=TextWikidata

    def init(self,force=False,by_files=True,**kwargs):
        if force or not self._init:
            super().init(by_files=by_files,**kwargs)
            

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

    def text(self,
            id_or_text,
            force=False,
            id_key='_id_wikidata',
            add=True,
            cache=True,
            init=True,
            only_valid=False,
            *args,
            **kwargs):

        id = id_or_text
        id_wiki = None
        is_valid_now = None

        if is_text_obj(id_or_text):
            log.debug(f'Is text: {id_or_text}')
            text = id_or_text
            id = id_or_text.addr
            id_wiki = text.meta.get(id_key)
            if id_wiki:
                id = id_wiki
                is_valid_now = True

        log.debug(f'Got ID: {id}')
        otext = super().text(id,**kwargs)
        log.debug(f'Gen Text: {otext} (valid now = {is_valid_now})')
        if is_valid_now or otext.is_valid():
            if add: self._textd[otext.id]=otext
            if cache: otext.cache_meta_json()
        return otext


    # def text(self,id_or_text,force=False,id_key='_id_wikidata',add=True,init=True,only_valid=False,*args,**kwargs):
    #     t = Text(id_or_text)
    #     nt = self.null_text()
    #     #t = self.get_text(id_or_text)
    #     if not force and t is not None and t.id is not nt.id: return t

    #     # init? get key
    #     t1 = self.init_text(id_or_text,**kwargs)
    #     idwd=t1.meta.get(id_key)
    #     if idwd is None:
    #         t1b = super().text(t1,add=add,**kwargs)
    #         if t1b is not None and t1b.meta is not None:
    #             idwd=t1b.meta.get(id_key)
        
    #     print([idwb])
    #     if idwd is not None:
    #         t = super().text(idwd,**kwargs)
            
    #         if t.is_valid():
    #             if add: self._textd[t.id]=t
    #             return t
        
    #     return nt
        
def wikidata_query_str(text):
    return clean_text(f'{text.au}, {text.shorttitle}')

def wikidata_search(
        qstr_or_text,
        lang="en",
        what=["work","manuscript","text"],
        min_match_ratio_au=50,
        min_match_ratio_ti=50,
        timeout=30,
        verbose=True,
        **kwargs):
    import requests,wptools,bs4
    from fuzzywuzzy import fuzz

    if issubclass(type(qstr_or_text), BaseText):
        text = qstr_or_text
        qstr=wikidata_query_str(qstr_or_text)
    else:
        text = None
        qstr=qstr_or_text

    if verbose: log.debug('Querying wikidata: '+qstr)
    
    safe='+'.join(clean_text(qstr).split())
    url=f'https://www.wikidata.org/w/index.php?search={safe}'
    with requests.Session() as s:
        html = s.get(url,timeout=timeout).text
    dom=bs4.BeautifulSoup(html,'html')

    for item in dom.select('.wb-itemlink-id'):
        itext=item.text
        qid=itext.replace('(','').replace(')','')
        
        # check type
        page = wptools.page(wikibase=qid, silent=True)
        o = page.get_wikidata()
        if o is not None and hasattr(o,'data'):
            data=o.data
            whatres = data.get('what','')
            for whatx in what:
                if not whatres or not whatx or whatx in whatres:
                    odat=data.get('wikidata')
                    od = format_wikidata_d(odat)
                    wd_au,wd_ti = wikidata_get_author(od), wikidata_get_title(od)
                    if not wd_ti: wd_ti=data.get('label')
                    # pprint([wd_au,wd_ti,od])
                    if not wd_au or not wd_ti: continue

                    if text is not None:
                        t_au,t_ti = text.au, text.shorttitle
                        au_ratio=fuzz.token_set_ratio(wd_au, t_au)
                        ti_ratio=fuzz.token_set_ratio(wd_ti, t_ti)
                        if min_match_ratio_au and au_ratio < min_match_ratio_au: continue
                        if min_match_ratio_ti and ti_ratio < min_match_ratio_ti: continue
                    else:
                        ti_ratio=np.nan
                        au_ratio=np.nan

                    return dict(
                        qid=qid,
                        meta=od,
                        meta_simple=format_wikidata_d_simple(od),
                        wd_title=wd_ti,
                        wd_author=wd_au,
                        wd_author_match=au_ratio,
                        wd_title_match=ti_ratio,
                        query=qstr,
                    )
    return dict(
        qid='Q0',
        meta={},
        meta_simple={},
        wd_title='',
        wd_author='',
        wd_author_match=0,
        wd_title_match=0,
        query=qstr,
    )

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

def wikidata_get_prop(d,prop='',propname=''):
    if type(d)==tuple and len(d)==2 and type(d[1])==dict: d=d[1]
    if type(d)==dict and 'meta' in d: d=d['meta']
    if prop or propname:
        for k in d:
            if type(k)==tuple and len(k)==2 and k[0]==prop or k[1]==propname:
                o=d[k]
                if type(o)==list: o=o[0]
                if type(o)==tuple and len(o)==2: return o[-1]
                return o
    return ''

def wikidata_get_title(d): return wikidata_get_prop(d,'P1476','title').replace('_',' ')
def wikidata_get_author(d): return wikidata_get_prop(d,'P50','author').replace('_',' ')

def format_wikidata_d_simple(d):
    from unidecode import unidecode
    od={}
    for k,v in d.items():
        key=format_wikidata_str(k,simple=True,lower=True,spaces=False)
        key=unidecode(key).replace("'","").replace('"','')
        
        val = format_wikidata_str(v,simple=True,lower=False,spaces=True)
        
        od[key] = val
    
    return od


