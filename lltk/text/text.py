from lltk.imports import *
from .utils import *


class BaseText(BaseObject):
    BAD_TAGS={'note','footnote','greek','latin'}
    # BODY_TAG=None
    XML2TXT=xml2txt_default
    TOKENIZER=tokenize
    SECTION_CLASS=None
    SECTION_CORPUS_CLASS=None
    SECTION_DIR_NAME=DIR_SECTION_NAME
    META={'genre':'', 'medium':''}

    def __eq__(self,other):
        addr1 = self.addr
        addr2 = other.addr if is_text_obj(other) else other
        return addr1==addr2
    
    def __hash__(self): return hash(self.addr)

    def __init__(self,
            id=None,
            _corpus=None,
            _section_corpus=None,
            _source=None,
            _sources=None,
            _txt=None,
            _xml=None,
            _remote=None,
            _cache=True,
            **kwargs):
        
        meta = just_meta_no_id(kwargs)
        if log>1:  log(f'<- {get_imsg(id,_corpus,_source,**meta)}')
        # if log>0:  log(f'<- remote = {_remote}')
        from lltk import Corpus
        self.corpus=Corpus(_corpus)
        self._section_corpus=_section_corpus
        self._sections={}
        self._rels={}
        self._gcache={}
        self.__meta={}
        self._reld={}
        self._meta={}
        self._gdb=None
        self._db=None
        self._init=set()
        self._cacheworthy=False
        if _txt: self._txt=_txt
        if _xml: self._xml=_xml
        self._source=_source
        self._sources={x for x in _sources} if _sources else set()
        if id is None:
            id = self.corpus.get_text_id(id, _source=_source, **meta)
            if log: log(f'blank id set to {id}')
        self.id=id
        self.corpus.add_text(self)
        self._meta=self.ensure_id(merge_dict(
            TEXT_META_DEFAULT,
            self.META,
            self._meta,
            meta,
        ))
        self._last_cache=None

        ### asynchronous leap!
        # self.init(cache=_cache)

    def init_remote(self,meta={},force=False,remote=None,cache=True,**kwargs):
        if log: log('making asychronous leap!')
        self._future = self.init_async(
            meta=meta,force=force,remote=remote,cache=cache,
            **kwargs
        )
        if log: log(f'here\'s my promise: {self._future}')    

    def init_local(self,meta={},force=False,remote=None,cache=True,**kwargs):
        if log: log('making synchronous moves')
        self._last_cache = dlocal = self.cachedb('meta').get(self.addr,{})
        if log>1: log(f'dlocal = {dlocal}')

        # quick meta
        self._meta=self.ensure_id(merge_dict(self._last_cache,self._meta))
        if log>1: log(f'self._meta = {self._meta}')

        # quick rels
        with self.cachedb('match') as odb:
            self._rels={**odb.get(self.addr,{}), **self._rels}
            if log: log(f'self._rels = {self._rels}')
        

    def init(self,**kwargs):
        self.init_remote(**kwargs)
        # self.init_local(**kwargs)
        return self
        

    def init_async(self,callback=None,**kwargs):
        # future = self.init_meta_async(callback=callback)
        future = self.init_rels_async(callback=callback)
        return future


    def init_rels_async(self,callback=None,**kwargs):
        if self._future_rels is not None: return self._future_rels

        # qstr=f"SELECT * FROM textrels WHERE id = '{self.addr}'"
        qfunc=self.cdb.getfuncs.get('rels_from_id')
        if log: log(qfunc)
        qvals=[self.addr]
        now=time.time()

        def handle_success_rels(rows):
            if log>1: log('success!')
            if len(rows)>1: log.error(f'Too many rows returned? {rows}')
            if len(rows)==1:
                tdocd = rows[0]
                log(pf(tdocd))
                if 'rels' in tdocd:
                    cached_meta = deserialize_map(dict(tdocd.get('rels')))
                    if log>1: log(f'cached meta = {cached_meta}')
                    if 'rels' in cached_meta: del cached_meta['rels']
                    
                    self._rels={**self._rels, **safebool(cached_meta)}
                    self._future_rels=None
                    if log>1: log(f'self._rels now = {self._rels}')

                    if log>1: log(f'{self} complete in {round(time.time()-now,2)} seconds')

            ## On to next call back?
            ### now get the meta?
            self.init_meta_async(callback=callback)
        
        def handle_error_rels(exc):
            if log: log.error(f"WHAT HAPPENED TO RELS? {exc}")
            self._future_rels=None
            ### now get the meta?
            self.init_meta_async(callback=callback)


        self._future_rels = future = self.cdb.execute_async(
            qfunc,qvals,
            callback=handle_success_rels,
            callback_error=handle_error_rels
        )

        return future


    @property
    def init_meta_getfunc(self):
        if not self._init_meta_getfunc:
            self._init_meta_getfunc=self.cdb.db.prepare("SELECT * FROM texts WHERE id=?")
        return self._init_meta_getfunc

    def init_meta_async(self,callback=None,**kwargs):
        if self._future_meta is not None: return self._future_meta
        if log: log(self)
        

        qfunc=self.cdb.getfuncs.get('text_from_id')
        qvals=[self.addr]
        if log: log(f'{qfunc} -> {qvals}')
        now = time.time()

        def handle_success_meta(rows):
            if log>1: log('success!')
            
            rows=list(rows)
            if len(rows)>1: log.error(f'Too many rows returned? {rows}')
            if len(rows)==1:
                tdocd = rows[0]
                if 'data' in tdocd:
                    cached_meta = deserialize_map(dict(tdocd.get('data')))
                    if log>1: log(f'cached meta = {cached_meta}')
                    if 'data' in cached_meta: del cached_meta['data']
                
                    self._last_cache = self.ensure_id(safebool(cached_meta))
                    self._meta=self.ensure_id({**self._last_cache, **self._meta})

                    if log>1: log(f'self._meta = {self._meta}')
                    self._future_meta=None
                    if log>1: log(f'{self} complete in {round(time.time()-now,2)} seconds')

            ### now get the rels!
            # self.init_rels_async(callback=callback)
            if callback: callback()


        def handle_error_meta(exc):
            STOPXXX
            if log: log.error(f"WHAT HAPPENED? {exc}")
            self._future_meta=None
            ### now get the rels ?
            # self.init_rels_async(callback=callback)
            if callback: callback()


        if log: log('? getting future...')        
        self._future_meta = future = self.cdb.execute_async(
            qfunc,qvals,
            callback=handle_success_meta,
            callback_error=handle_error_meta
        )
        # if log: log(f'? got future: {future}')        


        return future



        
    def __repr__(self):  return self.node
    

    @property
    def addr(self): return f'{IDSEP_START}{self.corpus.id}{IDSEP}{self.id}'
    @property
    def nsrc(self): return len(self._sources)
    
    @property
    def node(self,force=True):
        if force or not self._node:
            au,ti,addr = self.au, self.ti, self.addr
            if au and ti and addr:
                # starter
                ol=[f'{au}, {ti.replace("_"," ").title()[:50].strip()}']
                
                # year
                yr = self.year
                if safebool(yr): ol.append(f' ({int(yr)})')
                
                # addr
                ol.append(f' [{addr}]')
                # num src?
                
                nsrc=self.nsrc + 1
                if nsrc and nsrc>1: ol.append(f' ({nsrc})')
                self._node = ''.join(ol)
            elif addr:
                self._node=f'({addr})'
            else:
                raise Exception('every text ought to have an addr')
        
        return self._node

            
    
    @property
    def idx(self): return self.id.split('/')[-1]
    
    @property
    def col_addr(self): return self.corpus.col_addr
    @property
    def col_id(self): return self.corpus.col_id

    def ensure_id_addr(self,*x,**y): return self.ensure_id(*x,**y)

    def ensure_id(self,
            meta=None,
            col_id=COL_ID,
            col_corpus='_corpus',
            col_addr='_id',
            col_rels='_rels',
            sep=META_KEY_SEP,
            allow_sep=True):
        if meta is None: meta=self._meta
        items = [
            (k,v)
            for k,v in sorted(meta.items())
            if k and v and k.count(sep)<=(1 if allow_sep else 0)
            and k not in {col_id,col_corpus}
        ]
        return {
            col_addr:self.addr,
            col_corpus:self.corpus.id,
            col_id:self.id,
            # col_rels:self.rels,
            **{k:v for k,v in items if k.startswith(col_id+META_KEY_SEP)},
            **{k:v for k,v in items if not k.startswith(col_id+META_KEY_SEP)},
        }

    @property
    def rels(self):
        o=getattribute(self,'_rels')
        if o: return o
        return {}

    ####################################################################
    # GETTING ATTRIBUTES
    ####################################################################

    def __getitem__(self, key): return self.get(key)

    def __getattr__(self, name):
        if name.startswith('path_'): return self.get_path(name)

        res = getattribute(self, name)
        if res is not None: return res

        res = self.get(name)
        if res is not None: return res
        
        return None

        
    def __setitem__(self, key, value): return self.update({key:value})
    def __delitem__(self, key):
        if key in self._meta: del self._meta[key]
    def __iter__(self): return iter(self.meta.items())
    def __len__(self): return self.num_words

    def get(self,key,default=None,ish=True,ish_all=None,**kwargs):
        if self._gcache is None: self._gcache={}
        if key in self._gcache: return self._gcache[key]
        
        if log>1: log(f'? {key}')
        key=str(key)
        if key.startswith('_'): ish=False
        if key.endswith('_l'): return self.meta_l(key[:-2],ish=ish,**kwargs)
        if key.endswith('_1'): return self.meta_1(key[:-2],ish=ish,**kwargs)
        if key.endswith('_'): return self.meta_(key[:-1],ish=ish,**kwargs)

        meta=merge_dict(self._meta,self.__meta)
        if not ish:
            res=meta.get(key,default)
        else:
            # log(f'? {key} in {pf(meta)}')
            vals = []
            hvals = set()
            for k in meta:
                if k.startswith(key):
                    l=meta[k]
                    if type(l)!=list: l=[l]
                    for v in l:
                        if v and (not is_hashable(v) or v not in hvals):
                            if is_hashable(v): hvals|={v}
                            vals.append(v)
            if vals:
                o=(vals if ish_all else vals[0])
                res=o if o is not None else default
        res = default
        self._gcache[key]=res
        return res
        


    def get_ish(self,meta,key,before_suf='|'):
        keys=[k for k in meta.keys() if k.startswith(key)]
        numkeys=len(keys)
        if numkeys==0: return None
        if numkeys==1: 
            keyname = keys[0]
        else:

            def get_rank(k,sep=META_KEY_SEP):
                if not sep in k: return np.inf
                corp=k.split(sep)[-1]
                crnk=CORPUS_SOURCE_RANKS.get(corp,np.inf)
                return crnk

            keys.sort(key=lambda k: get_rank(k))
            keyname=keys[0]

            if log>0: log.warning(f'more than one metadata key begins with "{key}": {", ".join(keys)}. Using key: "{keyname}".')
        
        res = meta.get(keyname)

        if before_suf and type(res)==str and res:
            res = res.split('|')[0]
        
        return res
    
    def get_one(self,key,default=None,**kwargs):
        res = self.get(key,default=default,**kwargs)
        if res:
            if type(res) in {list,tuple,set}:
                res = list(res)[0]
        return res if res is not None else default

    def get_all(self,key,default=None,**kwargs):
        res = self.get(key,default=default,**kwargs)
        if res:
            if type(res) in {list,tuple,set}:
                res = list(res)[0]
        return res if res is not None else default



    @property
    def xml2txt_func(self): return self.XML2TXT.__func__
    
    

    ####################################################################
    # PATHS 
    ####################################################################


    

    def get_path_old(t,part='texts',**kwargs):
        if not t.corpus: return ''
        partattr='path_'+part
        extattr='ext_'+part
        res=getattr(t.corpus, partattr)
        if res:
            o=os.path.join(res,t.id)
            resext=getattr(t.corpus, extattr)
            if resext: o+=resext
            return o


    def get_path(self,part,**kwargs):
        try:
            if part.startswith('path_'): part=part[5:]
            path_new = self.get_path_new(part,**kwargs)
            path_old = self.get_path_old(part,**kwargs)
            if path_new:
                if path_old and os.path.exists(path_old):
                    return path_old
                return path_new
            elif path_old:
                return path_old
        except Exception as e:
            log.error(e)
        return ''
            


    def get_path_new(self,part,**kwargs):
        if part == 'txt': return os.path.join(self.path,'text.txt')
        if part == 'xml': return os.path.join(self.path,'text.xml')
        if part in {'json','meta','meta_json'}:
            return os.path.join(self.path,'meta.json')
        if part == 'sources': return os.path.join(self.path,'_sources')
        if part == 'freqs': return os.path.join(self.path,'freqs.json')
        return None

    @property
    def path(self):
        if self.corpus.path_texts and self.id:
            return os.path.join(self.corpus.path_texts,self.id)
        else:
            return ''
    @property
    def path_rel(self): return self.relpath()
    @property
    def rel_path(self): return self.relpath(reverse=True)

    def relpath(self,path=None,text=None,reverse=False):
        a = path if path else self.path
        b = text.path if text else PATH_CORPUS
        if reverse: a,b=b,a
        return os.path.relpath(a,b)

    @property
    def path_meta_json(self): return os.path.join(self.path,'meta.json')
    
    def get_path_xml(self):
        if not os.path.exists(self.path_xml):
            tsrc = self.source
            if tsrc is not None and os.path.exists(tsrc.path_xml):
                return tsrc.path_xml
        return self.path_xml

    def get_path_text(self,part='txt'):
        if part=='txt':
            return os.path.join(self.path, 'text.txt')
        elif part=='xml':
            return os.path.join(self.path, 'text.xml')
        return ''



    ####################################################################
    # Loading 
    ####################################################################


    def init_(self,remote=None,cache=True,**kwargs):
        remote=is_logged_on()
        kwargs['from_sources']=False
        kwargs['cache']=True
        kwargs['remote']=remote
        return self.init(**kwargs)
        


    def get_cacheable_meta(self,ometa={}):
        #return #self.ensure_id(
        return just_meta_no_id(
                {
                    **(self.__meta if self.__meta else {}),
                    **(self._meta if self._meta else {}),
                    **(ometa if ometa else {})
                }
            )#,
            # allow_sep=False
        # )

    def cache(self,ometa={},force=False,**y):
        # pkg
        res=None
        if log: log(f'?')
        if self._last_cache is None:
            if log: log('self._last_cache does not exist. waiting for result of last cache')
            try:
                # future = self.init_async()
                self.init()
                # future.result()
                # if log: log(f'result done. self._last_cache = {self._last_cache}')
            except Exception as e:
                if log: log(f'CANNOT CONNECT: {e}')
            
            if self._last_cache is None:
                if log: log('last cache could not be loaded')
                self._last_cache={}

        ometa = merge_dict(self._last_cache, self.get_cacheable_meta(ometa))
        if log>1: log(f'old = {self._last_cache}')
        if log>1: log(f'new = {ometa}')

        if force or ometa != self._last_cache:
            if log: log(f'<<< {self}')
            self._last_cache = ometa
            # store
            year = self.year
            dbd={}
            dbd['id']=self.addr
            dbd['corpus']=self.corpus.id
            dbd['author']=self.author[:255]
            dbd['title']=self.title[:255]
            dbd['year']= year if safebool(year) else 0
            if 'data' in ometa: del ometa['data']
            dbd['data']=serialize_map(just_meta_no_id(ometa))
            if log: log(f'-> {dbd}')

            
            ## store in cache

            with self.cachedb('meta') as odb:
                odb[self.addr]=ometa
                if log: log(f'saved to local cache: {self.addr} --> {ometa}')


            # ## and in db if possible
            # qstr=self.cdb.setfunc_text
            # qvals=[dbd['id'],dbd['corpus'],dbd['author'],dbd['title'],dbd['year'],dbd['data']]

            # def callback(x): pass
            #     # log.info(f'finished callback. res = {x}')

            # res = self.cdb.execute_async(qstr, qvals, callback=callback)
            # if log: log(f'cdb res = {res}')

        return res
        



    def update(self,meta={},_force=False,_cache=True,**metad):
        if meta or metad:
            imeta = {**meta, **metad}
            self.cache(imeta)

            # if log>1: log(f'<- {imeta}')
            # if _cache:
            #     ometa = self.cache(imeta,force=_force)
            #     self._meta = {**self._meta, **ometa}
            # else:
            #     self._meta = {**self._meta, **imeta}


    
    ####################################################################
    # Metadata 
    ####################################################################
    
    @property
    def qdb(self): return self.corpus.qdb
    
    def query(self,*x,**y): return {}

    def metadata_remote(self,meta={},sep=META_KEY_SEP,remote=REMOTE_REMOTE_DEFAULT,cache=True,force=False,**kwargs):
        if log>1: log(self)
        ometa=meta
        if self.id_is_valid() and (force or not self.meta_is_valid(meta)):
            query_meta = self.query(**kwargs)
            if query_meta:
                self._meta=merge_dict(self._meta, query_meta)
                if log: log(f'self.query({kwargs}) -> {query_meta.keys()}')
                ometa={**ometa, **query_meta}
                if '_sources' in ometa: self._sources |= set(ometa.pop('_sources'))
                if '_source' in ometa: self._sources |= {ometa.pop('_source')}
                if log: log(f'{len(ometa)} keys')
                self._cacheworthy=True
                self.cache(ometa)
        return self.ensure_id(ometa)

    def metadata_from_cache(self,meta={},**kwargs):
        if log>1: log(self)
        #return self.ensure_id(merge_dict(self.init_cache(),**meta))
        self.init_meta_async()
        return {**self._meta}

    def metadata_from_sources(self,meta={},sep=META_KEY_SEP,remote=None,force=False,**kwargs):
        if log: log(self)
        remote=is_logged_on()
        kwargs['from_sources']=False
        sources_present = {k.split(sep)[-1] for k in meta if sep in k}
        if log: log(f'sources_present = {sources_present}')
        sources_needed = [src for src in self.dsources if force or src.corpus.id not in sources_present]
        if log: log(f'sources_needed = {sources_needed}')


        odset = OrderedSetDict()
        for src in sources_needed:
            src_meta=src.metadata(remote=remote,**kwargs)
            src_meta = flattendict(src_meta)
            for k,v in src_meta.items():
                if k and k[0]!='_':
                    odset[k+sep+src.corpus.id] = v    
                        
        if log: log(odset)
        odx=merge_dict(meta,odset.to_dict())
        return self.ensure_id(odx)



    def metadata_initial(
            self,meta={},
            bad_keys={},#'_source','_sources'},
            sep=META_KEY_SEP,
            **kwargs):
        if log>1: log(self)
        imeta=merge_dict(TEXT_META_DEFAULT, self.META, self.__meta, self._meta, meta)
        ometa=self.ensure_id({k:v for k,v in imeta.items() if k not in bad_keys},allow_sep=False)
        self._meta={k:v for k,v in ometa.items() if not sep in k}
        return ometa

    def metadata(
            self,
            meta={},
            from_initial=True,
            from_query=False,
            from_cache=False,
            from_sources=False,
            cache=False,
            remote=None,
            sep=META_KEY_SEP,
            to_numeric=True,
            **kwargs):
        if log>1: log(self)
        # begin dict
        ometa={**meta}
        if log: log(f'<- remote = {remote} ?')
        remote=is_logged_on()
        
        # initial (self.__meta, self._meta, ...)
        if from_initial: ometa = self.metadata_initial(ometa)
        
        # db caches
        try:
            if from_cache: ometa = self.metadata_from_cache(ometa,**kwargs)
        except Exception as e:
            pass
        
        # query for myself?
        try:
            if remote and is_logged_on() and from_query: ometa = self.metadata_remote(ometa,**kwargs)
        except Exception as e:
            pass
        
        # source metadata (incl remote sources?)
        if from_sources:
            # make sure remote sources present
            try:
                if remote and is_logged_on(): self.get_remote_sources()
            except Exception as e:
                pass

            # get metadata
            ometa = self.metadata_from_sources(
                ometa,
                from_cache=from_cache,
                remote=remote,
                cache=cache,
                **kwargs
            )
        # set temp cache meta
        self.__meta = ometa = self.ensure_id(ometa)
        # cache?
        if cache: self.cache(ometa)
        # return final meta dict
        if to_numeric: ometa=to_numeric_dict(ometa)
        return ometa
    


    @property
    def meta(self): return self.metadata()
    @property
    def _meta_(self): return {k:v for k,v in self._meta.items() if not META_KEY_SEP in k}
    def id_is_valid(self,*x,**y):
        if self.id in {None,'','None'}: return False 
        return True
    def meta_is_valid(self,*x,**y): return True
    def is_valid(self,meta=None,**kwargs):
        """
        @TODO: Subclasses need to implement this
        """
        return True

    def metadata_source_counts(self,meta={},**kwargs):
        meta=self.metadata(meta,**kwargs)
        cd=Counter(k.split(META_KEY_SEP)[1] if META_KEY_SEP in k else t.corpus.id for k in meta)
        return cd

    def metadata_by_source_corpus(self,meta={},sep=META_KEY_SEP,**kwargs):
        odset=defaultdict(dict)
        meta=self.metadata(meta,**kwargs)
        for k,v in meta.items():
            k2,corp = (k,self.corpus.id) if sep not in k else k.split(sep,1)
            odset[corp][k2]=v
        return odset



    
    def meta_(self,
            key='',
            ish=True,
            init=False,
            **kwargs):
        
        sources = set(self._sources if not init else self.sources) | {self}
        vals = set()
        o = []
        for t in sources:
            for k in t._meta:
                if ish:
                    if not k.startswith(key): continue
                else:
                    if k != key: continue
                
                val = t._meta[k]
                if is_hashable(val):
                    if val in vals: continue
                    vals|={val}
                ot = (t,k,val)
                o.append(ot)
        return o

    def meta_l(self,*args,**kwargs):
        return SetList([v for t,k,v in self.meta_(*args,**kwargs)])
    
    def meta_1(self,*args,**kwargs):
        l=self.meta_l(*args,**kwargs)
        return l[0] if l else None

    def metadata_by_source(
            self,
            key_startswith='',
            meta={},
            sep=META_KEY_SEP,
            inverse=False,
            init=False,**kwargs):
        # self.init(meta=meta,sep=sep,**kwargs)
        
        sources = set(self._sources if not init else self.sources) | {self}

        def filtermeta(d):
            return d if not key_startswith else {
                k:v for k,v in d.items() if k.startswith(key_startswith)
            }

        if inverse:
            return {k:v for k,v in [(t,filtermeta(t._meta)) for t in sources] if k and v}
        else:
            od=OrderedSetDict()
            for t in sources:
                for k,v in t._meta.items():
                    od[k]=[(t,v)]
            return filtermeta(dict((k,dict(v)) for k,v in od.items()))

    




    def num_words(self,keys=['num_words','length']):
        for k in keys:
            if k in self.meta:
                return int(self.meta[k])
        return sum(self.counts().values())
    @property
    def words_recognized(self):
        wordlist=get_wordlist(lang=self.lang)
        return [w for w in self.words if w in wordlist or w.lower() in wordlist]
    @property
    def ocr_accuracy(self):
        return float(len(self.words_recognized)) / len(self.words) if len(self.words) else np.nan
    
    def yearbin(self,ybin,as_str=False,zfill=4):
        try:
            binval=self.year//ybin*ybin
            return binval if not as_str else f'{str(binval).zfill(zfill)}-{str(binval+ybin).zfill(zfill)}'
        except Exception:
            return np.nan
    @property
    def halfdecade(self): return self.yearbin(5)
    @property
    def decade(self): return self.yearbin(10)
    @property
    def quartercentury(self): return self.yearbin(25)
    @property
    def halfcentury(self): return self.yearbin(50)
    @property
    def century(self): return self.yearbin(100)
    @property
    def halfdecade_str(self): return self.yearbin(5,as_str=True)
    @property
    def decade_str(self): return self.yearbin(10,as_str=True)
    @property
    def quartercentury_str(self): return self.yearbin(25,as_str=True)
    @property
    def halfcentury_str(self): return self.yearbin(50,as_str=True)
    @property
    def century_str(self): return self.yearbin(100,as_str=True)
    @property
    def title(self): return str(self.get('title','',ish=True))
    @property
    def author(self): return str(self.get('author','',ish=True))
    @property
    def au(self):
        from lltk.corpus.utils import to_authorkey
        return to_authorkey(self.author)
    @property
    def ti(self): return ensure_snake(self.shorttitle,lower=False)

    @property
    def years(self,keys=YEARKEYS):
        years = SetList()
        for trykey in keys:
            for val in self[trykey+'_l']:
                v = zeropunc(str(val))[:4]
                if v.isdigit():
                    vnum = pd.to_numeric(v,errors='coerce')
                    if safebool(vnum):
                        years.append(vnum)
        if not years: return []
        years.sort()
        return years
        # return self._years

    @property
    def year(self):
        years = self.years  # comes sorted
        if len(years)==0: return np.nan
        if len(years)==1: return years[0]
        if len(years)==2: return years[0]
        if len(years)==3: return years[1]
        if len(years)==4: return years[1]
        imedian = len(years) // 2
        return years[imedian]


    
    @property
    def shorttitle(self,
            puncs=':;.([,!?',
            ok={'Mrs','Mr','Dr'},
            title_end_phrases={
                'edited by','written by',
                'a novel','a tale','a romance','a history','a story',
                'a domestic tale',
                'by the author','by a lady','being some','by Miss','by Mr',
                'an historical','the autobiography',
                'being',
                ' by ',
                ' or'
            },
            replacements={
                ' s ':"'s ",
            },
            replacements_o={"'S ":"'s "}
            ):

        ti=self.title
        ti=ti.strip().replace('—','--').replace('–','-')
        ti=ti.title()
        for x,y in replacements.items(): ti=ti.replace(x.title(),y)
        if any(x in ti for x in puncs):
            for x in puncs:
                o2=ti.split(x)[0].strip()
                if o2 in ok: continue
                ti=o2
        else:
            l=list(title_end_phrases)
            l.sort(key = lambda x: -len(x))
            for x in l:
                # log(x+' ?')
                ti=ti.split(x.title())[0].strip()
        o=ti.strip()
        for x,y in replacements_o.items(): o=o.replace(x,y)
        return o
    
    @property
    def qstr(self):
        return clean_text(f'{self.shorttitle} {self.au}')
    @property
    def qstr_plus(self):
        from urllib.parse import quote_plus
        return quote_plus(self.qstr)
    
    @property
    def shortauthor(self):
        au=clean_text(self.author)
        if not au: return ''
        if not ',' in au: return au
        
        parts=[x.strip() for x in au.split(',') if x.strip() and x.strip()[0].isalpha()]
        if len(parts)==0: return au
        if len(parts)==1: return parts[0]
        oparts=[parts[1]] + [parts[0]]

        # parentheses
        def grabparen(x):
            if '(' in x and ')' in x: return x.split('(',1)[-1].split(')',1)[0].strip()
            return x
        oparts=[grabparen(x) for x in oparts]
        ostr=' '.join(oparts)
        return ostr




    ####################################################################
    # Matches 
    ####################################################################


    def match(self,other,yn='',rel=MATCHRELNAME,rel_type='',cache=True,viceversa=True,**kwargs):
        self.cdb # make sure active
        if is_textish(other): 
            other = Text(other)
            if self != other and self.id_is_valid() and other.id_is_valid():
                if log: log(f'{self.addr} --> {other.addr} ?')
                relmeta=dict(yn=yn,rel=rel,rel_type=rel_type,**just_meta_no_id(kwargs))
                self._rels[other.addr]=relmeta
                other._rels[self.addr]=relmeta

                # local
                if log: log('caching local')
                with self.cachedb('match') as odb:
                    odb[self.addr]=self.rels
                    odb[other.addr]=other.rels

                # remote
                if log: log('caching remote')
                promise1 = self.cdb.execute_async(self.cdb.setfunc_match, [self.addr,serialize_map(self.rels)])
                promise2 = self.cdb.execute_async(self.cdb.setfunc_match, [other.addr,serialize_map(other.rels)])

                return (promise1,promise2)
        return(None,None)
    
    add_source=match


        
    def match_net(self,other,yn='',rel=MATCHRELNAME,cache=True,**kwargs):
        other = Text(other)
        if log: log(f'{self.addr} --> {other.addr} ?')
        
        self.gdb.add_node(self.addr)#, **no_id(self._meta))
        self.gdb.add_node(other.addr)#, **no_id(other._meta))

        meta=dict(yn=yn, rel=rel, **just_meta_no_id(kwargs))
        self.gdb.add_edge(self.addr, other.addr, **meta)


        ### CDB
        

    
    def get_matches(self,node=None,as_text=True,rel=MATCHRELNAME,depth=1,**kwargs):
        return set(self.get_matchgraph().nodes()) - {self.addr}

    def get_matchgraph(self,as_text=True,rel=MATCHRELNAME,depth=1,node_name='addr',**kwargs):
        # g=nx.Graph()
        # g.add_node(self.addr)
        # opts=[f"'{other_addr}'" for other_addr in list(self.rels.keys()) + [self.addr]]
        # optstr=','.join(opts)
        # qstr=f'SELECT * FROM textrels WHERE id IN ({optstr});'
        # if log: log(f'? {qstr}')
        # resld=self.cdb.execute(qstr)
        # for d1 in resld:
        #     id1=d1['id']
        #     g.add_node(id1)
        #     for id2,d2 in d1['rels'].items():
        #         g.add_edge(id1,id2,**ujson.loads(d2))

        
        g=self.cdb.ego_graph(self.addr)
        g.add_node(self.addr)
        for addr in self.rels:
            g.add_node(addr)
            g.add_edge(self.addr,addr)

        for node in list(g.nodes()):
            if IDSEP_START+TMP_CORPUS_ID+IDSEP in node:
                g.remove_node(node)
        
        if node_name!='addr':
            labeld=dict((addr,Text(addr).node) for addr in g.nodes())
            nx.relabel_nodes(g,labeld,copy=False)
        return g
                
    def matchgraph(self,rel=MATCHRELNAME,draw=True,node_name='addr',**kwargs):
        g=self.get_matchgraph(rel=rel,node_name=node_name)
        if not draw:
            return g
        else:
            from lltk.model.networks import draw_nx
            return draw_nx(g,**kwargs)


    @property
    def matches(self): return self.get_matches()
    


    
    
    def get_sources(self,force=False,**kwargs):
        if not self.id_is_valid(): return []
        if force or not self._sources:
            srcs=set(self.get_matches())
            self._sources=[
                t
                for t in sorted(srcs,key=lambda tx: tx.addr)
                if t.id_is_valid() and t!=self
            ]
        return self._sources


    def queue_remote_sources(self,callback=None,**kwargs):
        if self.au and self.shorttitle and self.qstr:
            log.info(f'querying remote sources for "{self.qstr}"')
            import time
            now=time.time()
            meta=just_meta_no_id(self._meta)
            if not meta: return
            num_srcs_now=len(self.matches)
            code="""
d=%s
self=lltk.Text('%s',**d)
with lltk.online: self.get_remote_sources()
""" % (ujson.dumps(safejson(meta)),self.addr)
            def callback_srcs(res):
                self.init_local()
                num_srcs_nownow=len(self.matches)
                log.info(f'processing completed in {round(time.time()-now,2)}s. {num_srcs_nownow} found, {num_srcs_nownow-num_srcs_now} of which are new')
            obj = llcode(code,callback=callback_srcs,**kwargs)
            return obj

    def get_remote_sources(self,corpora=None,cache=True,remote=REMOTE_REMOTE_DEFAULT,lim=1,progress=False,*args,**kwargs):
        from lltk.corpus.corpus import Corpus
        if corpora is None: corpora = self.corpus.REMOTE_SOURCES
        o=[]
        # other corpora?
        if corpora:
            if log: log(f'corpora = {corpora}')
            desc=f'[{self.addr}] '
            iterr=corpora
            if progress: iterr=get_tqdm(iterr,desc=desc,position=0)
            for i,c in enumerate(iterr):
                C=Corpus(c)
                if progress: iterr.set_description(f'{desc}: Querying {C.name}')
                
                if log: log(f'Remote corpus: {C} ({self.addr})')
                cl=[]
                for tsrc in C.texts_from(self,remote=remote,**kwargs):
                    if tsrc is not None:
                        cl.append(tsrc)
                        self.match(tsrc)
                        if progress: iterr.set_description(f'{desc}: Found {tsrc})')
                        if len(cl)>=lim: break
                o+=cl
                
        return o
    
    @property
    def sources(self): return [Text(x) for x in self.matches]

    @property
    def dsources(self,rel=MATCHRELNAME):
        #dneighbs={Text(addr) for addr in self.gdb.get_neighbs(self.addr,rel=rel,direct=True)} - {self.addr}
        #return [src for src in self.get_sources() if src in dneighbs]
        return {Text(addr) for addr in self.rels}
    
    def linked(self,**kwargs):
        return set(self.links(**kwargs).keys())
    def links(self,**kwargs):
        return self.gdb.get_links(self.addr)
    def edges(self,**kwargs):
        return self.gdb.get_edges(self.addr)
    

    @property
    def source(self):
        if self._source is not None: return Text(self._source)
        srcs=[x for x in self._sources]
        if srcs: return Text(srcs[0])
        srcs=self.sources
        if srcs: return list(srcs)[0]



    ####################################################################
    # Txt/xml 
    ####################################################################


    # load text?
    
    @property
    def txt(self): return self.get_txt()

    @property
    def xml(self):
        if self._xml: return self._xml
        path_xml = self.get_path_xml()
        if not os.path.exists(path_xml): return ''
        with open(path_xml) as f: return clean_text(f.read())
    
    
    
    # xml
    @property
    def dom(self):
        if self._dom is not None: return self._dom
        import bs4
        xml=self.xml
        if xml:
            dom=bs4.BeautifulSoup(xml,'lxml')
            for tag in self.BAD_TAGS:
                for x in dom(tag):
                    x.extract()
        else:
            dom=bs4.BeautifulSoup()

        if self.BODY_TAG is not None:
            dom = dom.find(self.BODY_TAG)
        
        return dom



    def text_plain(self, force_xml=None):
        """
        This function returns the plain text file. You may want to modify this.
        """
        # Return plain text version if it exists
        if self.path_txt and os.path.exists(self.path_txt) and not force_xml:
            with open(self.path_txt,encoding='utf-8',errors='ignore') as f:
                return f.read()
        # Otherwise, load from XML?
        if os.path.exists(self.path_xml): return self.XML2TXT.__func__(self.path_xml)
        return ''

    def get_txt(self,force=False,prefer_sections=False,section_type=None,force_xml=False):
        if force or not self._txt:
            if not prefer_sections:
                self._txt=self.text_plain(force_xml=force_xml)
                self._txt_offsets={}
            else:
                secs=self.sections(section_type)
                if secs is not None and secs.txt:
                    self._txt=secs.txt
                    self._txt_offsets=secs._txt_offsets
                else:
                    self._txt=self.text_plain(force_xml=force_xml)
                    self._txt_offsets={}
        return clean_text(self._txt) if self._txt else ''

    
    # freqs
    def save_freqs_json(self,ofolder=None,force=False):
        if not self.id: return {}
        if not os.path.exists(self.path_txt): return {}
        if not force and os.path.exists(self.path_freqs): return
        return save_freqs_json((self.path_txt,self.path_freqs,self.corpus.TOKENIZER.__func__))

    def freqs(self,lower=True,modernize_spelling=None):
        if not hasattr(self,'_freqs') or not self._freqs:
            # print('loading from file')
            if not os.path.exists(self.path_freqs): self.save_freqs_json()
            if not os.path.exists(self.path_freqs): return {}
            with open(self.path_freqs) as f: freqs=Counter(json.load(f))
            self._freqs=freqs
        return filter_freqs(self._freqs,modernize=modernize_spelling,lower=lower)

    @property
    def length(self): return sum(self.freqs().values())

    def tokens(self,lower=True):
        return self.TOKENIZER.__func__(self.txt.lower() if lower else self.txt)
    @property
    def words(self,lower=False):
        tokens=[noPunc(w) for w in self.tokens(lower=lower)]
        return [w for w in tokens if w]
    def sents(self):
        import nltk
        return nltk.sent_tokenize(self.txt)
    @property
    def counts(self,*x,**y): return self.freqs(*x,**y)
    def len():
        return self.num_words()
    @property
    def tfs(self,*x,**y): 
        counts=self.freqs(*x,**y)
        total=self.length
        return dict((w,v/total) for w,v in counts.items())
    @property
    def fpm(self,*x,**y):
        return dict((w,v*1000000) for w,v in self.tfs(*x,**y).items())
    

    @property
    def prose_or_verse(t):
        for g in [t.medium, t.genre, t.major_genre, t.canon_genre]:	
            if g in {'Prose','Non-Fiction','Fiction','Biography','Oratory'}:
                return 'Prose'
            elif g in {'Verse','Poetry'}:
                return 'Verse'
        if t.corpus.name in {'Chadwyck'}:
            return 'Prose'
        elif t.corpus.name in {'ChadwyckPoetry'}:
            return 'Verse'
        # else:
        # 	txt_verse, txt_prose = t.txt_verse, t.txt_prose
        # 	if txt_verse or txt_prose:
        # 		return 'Verse' if len(txt_verse)>len(txt_prose) else 'Prose'
        return ''
    @property
    def is_prose(self): return self.prose_or_verse=='Prose'
    @property
    def is_verse(self): return self.prose_or_verse=='Verse'

    @property
    def txt_prose(self):
        paras=self.paras_xml
        if not paras and self.is_prose: paras=self.paras
        return '\n\n'.join(paras if paras else [])
    @property
    def txt_verse(self):
        lines=self.lines_xml
        if not lines and self.is_verse: return self.txt
        return '\n'.join(lines if lines else [])

    @property
    def lines_xml(self):
        dom=self.dom
        for x in dom('p'): x.extract()
        return [clean_text(e.text).strip() for e in dom('l')]
    
    @property
    def paras_xml(self):
        dom=self.dom
        for x in dom('l'): x.extract()
        paras=[e.text.strip() for e in dom('p')]
        return [para for para in paras if para]
    @property
    def paras_txt(self):
        paras=[para.strip() for para in self.txt.split('\n\n')]
        return [para for para in paras if para]
    @property
    def paras(self):
        return self.paras_txt
        # paras = self.paras_xml
        # if not paras: paras = self.paras_txt
        # return paras

    @property
    def nltk(self):
        import nltk
        return nltk.Text(self.tokens())
    @property
    def blob(self):
        from textblob import TextBlob
        return TextBlob(self.txt)
    def stanza_paras(self,lang=None,num_proc=1):
        if lang is None: lang=self.lang
        txt=self.txt
        if not txt: return
        yield from pmap_iter(
                do_parse_stanza,
                # self.paras,
                [(para,lang) for para in self.paras],
                desc='Parsing paragraphs with Stanza',
                num_proc=num_proc)
    @property
    def stanza(self,lang=None):
        if lang is None: lang=self.lang
        #return do_parse_stanza(self.txt)
        return list(self.stanza_paras(lang=lang))
    @property
    def spacy(self,lang=None,num_proc=1):
        if lang is None: lang=self.lang
        objs=[(para,lang) for para in self.paras]
        if not objs: return []
        return pmap(
            do_parse_spacy,
            objs,
            desc='Parsing paragraphs with spaCy',
            num_proc=num_proc
        )
    
    # def minhash(self,cache=True,force=False):
    #     from datasketch import MinHash,LeanMinHash
    #     m = MinHash(num_perm=128*2)
    #     from base64 import b64decode,b64encode

    #     if self._minhash:
    #         if isinstance(self._minhash,bytes):
    #             self._minhash = LeanMinHash.deserialize(b64decode(self._minhash))
    #     else:
    #         if not os.path.exists(self.path_txt): return
    #         words = self.words
    #         if words:
    #             m = MinHash(num_perm=128*2)
    #             for word in words: m.update(word.encode('utf-8'))
    #             self._minhash = lm = LeanMinHash(m)

    #             if cache:
    #                 buf = bytearray(lm.bytesize())
    #                 lm.serialize(buf)
    #                 buf64=b64encode(buf)
    #                 self.update(_minhash = buf64)
    #                 self.cache()
    #     return self._minhash
    
    def minhash(self,cache=True,force=False):
        from datasketch import MinHash,LeanMinHash
        from base64 import b64decode,b64encode

        qkey=self.addr
        db = self.cachedb('minhash',engine='sqlite')
        buf64 = db.get(qkey) if not force and cache else None
        if buf64 is not None:
            self._minhash = LeanMinHash.deserialize(b64decode(buf64))
        else:
            if not os.path.exists(self.path_txt): return
            words = self.words
            if words:
                m = MinHash(num_perm=128*2)
                for word in words: m.update(word.encode('utf-8'))
                self._minhash = lm = LeanMinHash(m)

                if cache:
                    buf = bytearray(lm.bytesize())
                    lm.serialize(buf)
                    buf64=b64encode(buf)
                    db.set(qkey,buf64)
        return self._minhash

    def hashdist(self,text,cache=True):
        m1=self.minhash(cache=cache)
        m2=text.minhash(cache=cache)
        return 1 - m1.jaccard(m2)

    def get_section_class(self,section_class=None):
        if section_class is not None: return section_class
        if self.SECTION_CLASS is not None: return self.SECTION_CLASS
        return TextSection

    def get_section_corpus_class(self,section_corpus_class=None):
        if section_corpus_class is not None: return section_corpus_class
        if self.SECTION_CORPUS_CLASS is not None: return self.SECTION_CORPUS_CLASS
        from lltk.corpus.corpus import SectionCorpus
        return SectionCorpus


    


    # @property
    @property
    def letters(self): return self.sections(_id='letters')
    @property
    def chapters(self): return self.sections(_id='chapters')

    def sections(self,_id=None,section_class=None,section_corpus_class=None,force=False):
        if _id is None: _id=self.SECTION_DIR_NAME
        if force or _id not in self._sections:
            SectionCorpusClass = self.get_section_corpus_class(section_corpus_class)
            self._sections[_id]=SectionCorpusClass(
                # id=os.path.join(self.id, _id),
                id=_id,
                _source=self,
                _id_allows='_/',
                _id=_id
            )
        return self._sections.get(_id)

    @property
    def text_root(self):
        if not issubclass(self.__class__,BaseText): return
        if issubclass(self.__class__,TextSection): return self.source
        return self

    def characters(self,id='default',systems={'booknlp'},**kwargs):
        if type(self._characters)!=dict: self._characters={}
        if not id in self._characters:
            from lltk.model.characters import CharacterSystem
            CS=self._characters[id]=CharacterSystem(self.text_root)
            for sysname in systems:
                system=getattr(self,sysname)
                CS.add_system(system)
        return self._characters[id]

    def get_character_id(self,char_tok_or_id,**kwargs):
        return self.characters().get_character_id(char_tok_or_id,**kwargs)

    @property
    def charsys(self): return self.characters()
    def interactions(self,**kwargs): return self.charsys.interactions(**kwargs)

    @property
    def booknlp(self):
        if self._booknlp is None: self._booknlp={}
        if not self.addr in self._booknlp:
            from lltk.model.booknlp import ModelBookNLP
            self._booknlp[self.addr]=ModelBookNLP(self)
        return self._booknlp[self.addr]



    ###
    def testx(self,*x,**y):
        time.sleep(random.random() * 5)
        return time.time()








class TextSection(BaseText):
    _type='sections'
    @property
    def corpus(self): return self._section_corpus
    @property
    def path_txt(self): return self.get_path_text('txt')
    @property
    def path_xml(self): return self.get_path_text('xml')
    
    

def get_addr_from_d(d,keys=['_id','_addr','id']):
    for k in keys:
        if k in d and d[k] and is_textish(d[k]):
            return d[k]
    return None





TEXT_CACHE=defaultdict(type(None))
def Text(
        id=None,
        _corpus=None,
        _source=None,
        _force=False,
        _new=False,
        _add=True,
        _init=False,
        _cache=False,
        _use_db=USE_DB,
        # _col_id=COL_ID,
        **_params_or_meta):
    global TEXT_CACHE
    t=None
    text=id
    if is_text_obj(text) and not _corpus: return text
    if is_corpus_obj(text): return text

    if _new: _force=True
    
    meta = just_meta_no_id(_params_or_meta)
    if is_addr_str(text): 
        taddr=text
    elif is_dictish(text):
        tdata=text.get('data')
        if tdata and text.get('id'):
            id=text.get('id')
            meta=dict(tdata)
            taddr = id
        else:
            meta = {**meta, **just_meta_no_id(text)}
            taddr = get_addr_from_d(text)
    else:
        if log>1: log(f'<- {get_imsg(text,_corpus,_source,**meta)}')
        taddr = get_addr_str(**{
            **_params_or_meta,
            **dict(
                text=text,
                corpus=_corpus,
                source=_source,
            )
        })
    if not taddr:
        taddr = get_addr_str(get_idx(),TMP_CORPUS_ID)
        if log: log(f"cannot get address for {(text,_corpus)}")
    if taddr and not is_textish(taddr):
        taddr=get_addr_str(taddr,TMP_CORPUS_ID)
    
    if log>1: log(f'<- addr = {taddr}')

    # set kwargs
    

    if not _force and is_text_obj(TEXT_CACHE.get(taddr)) and TEXT_CACHE[taddr].is_valid():
        if log>1: log('found in `TEXT_CACHE`')
        t = TEXT_CACHE[taddr]
        if is_text_obj(t) and meta: t.update(meta)
        t = t if is_valid_text_obj(t) else NullText()
        return t
    
    tcorp,tid = to_corpus_and_id(taddr)
    if tcorp and tid:
        if log>1: log(f'Corpus( {tcorp} ).text( {tid} ) ->')
        
        from lltk.corpus.corpus import Corpus
        t = Corpus(tcorp).text(
            id=tid,
            _source=_source,
            _add=_add,
            _init=_init,
            _cache=_cache,
            _force=_force,
            _new=_new,
            **meta
        )
        if is_valid_text_obj(t): TEXT_CACHE[t.addr] = t
    
    t = t if is_valid_text_obj(t) else NullText()
    if log>1: log(f'-> {t}')
    return t




class NullText(BaseText):
    def id_is_valid(self, *x, **y): return False












def proc_minhash(taddr):
    try:
        Text(taddr).minhash()
    except Exception as e:
        log.error(e)
# def proc_minhash(t): t.minhash()