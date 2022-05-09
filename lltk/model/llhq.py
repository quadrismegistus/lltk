from lltk.imports import *

NOT_CORPORA={'bpo'}

class BaseLLTK(TextList):
    col_id=COL_ID
    col_corpus=COL_CORPUS
    cols_corpus = {'_corpus', 'corpus', COL_CORPUS, 'C'}

    def __init__(self,log=None):
        super().__init__()
        self.log=log if log is not None else Log()
    

    def __enter__(self): return self.log.__enter__()
    def __exit__(self,*x): return self.log.__exit__(*x)

    def text(self,*args,**kwargs): return Text(*args,**kwargs)
    def corpus(self,*args,**kwargs): return Corpus(*args,**kwargs)
    T = text
    C = corpus
    
    def __call__(self, id=None, au=None, ti=None, **kwargs): 
        # a text/corpus id?
        if id: return self[id]
        if au: kwargs['author']=au
        if ti: kwargs['title']=ti

        # # corpus given by keyword?
        # for cc in self.cols_corpus:
        #     if cc in kwargs and type(kwargs[cc])==str and kwargs[cc]:
        #         return Corpus(kwargs[cc])
        
        if kwargs:
            return self.search(**safejson(kwargs))
        else:
            kwargs={**dict(_progress=True,_unique=True),**kwargs}
            return iter(self.iter_texts_uniq(**kwargs))
        
    def __getitem__(self, addr):

        if type(addr)==str and addr:
            addr=addr.strip()
            if is_textish(addr):
                return Text(addr)
            elif len(addr)<100:
                return Corpus(addr.strip())
    
    def iter_texts_each(
            self,
            text_iter=None,
            _progress=True,
            # _unique=True,
            _desc='[LLTK] iterating all texts',
            **kwargs):
        if log: log('querying for all texts')

        if not text_iter:
            text_iter = self.data_all
            if self.data_all:
                _progress = False
        if not text_iter: text_iter = self.mdb.get()
        if log: log('done')

        if _progress: text_iter = get_tqdm(text_iter,desc='[LLTK] Iterating over all texts')
        done=set()
        o=[]
        for d in text_iter: 
            t=Text(d)
            if t not in done and is_valid_text_obj(t):
                yield t
                done|={t}
                o.append(t)
        self.data_all=o


    def get_iter(self,au=None,ti=None,**kwargs):
        if au: kwargs['author']=au
        if ti: kwargs['title']=ti
        res = self.mdb.where(**kwargs)
        yield from self.iter_texts(res,**kwargs)
    
    find = get_iter
    
    def get(self,id=None,lim=None,**meta):
        if id: return Text(self.mdb.get(id))
        return TextList([x for i,x in enumerate(self.get_iter(**meta)) if not lim or i<lim]).init()
        
    
    ## Finding by string contains
    
    def search(self,lim=None,**meta):
        return TextList([
            x
            for i,x in enumerate(self.mdb.search(**meta))
            if not lim or i<lim
        ]).init()


    def search_iter(self,_progress=True,_unique=True,au=None,ti=None,**kwargs):
        if au: kwargs['author']=au
        if ti: kwargs['title']=ti
        res = self.mdb.search(**kwargs)
        yield from self.iter_texts(res,progress=_progress,_unique=_unique,**kwargs)

    def get_cache_key(self,conds,search_type='search'):
        o=f'{search_type}___{sorted(conds)}'
        self.log(o)
        return o
    
    def get_cache(self,conds,search_type='search'):
        key = self.get_cache_key(conds=conds,search_type=search_type)
        return self.cachedb.get(key)
    
    def set_cache(self,conds,res,search_type='search'):
        key = self.get_cache_key(conds=conds,search_type=search_type)
        self.cachedb[key]=res

    @property
    def cachedb(self):
        return {}


                    
    def to_text(self,d):
        if d is not None and 'id' in d:
            t=Text(d)
            return t


    def sync(self,corpus_list=INIT_DB_WITH_CORPORA,not_corpora=NOT_CORPORA):
        corpus_list=sorted([c for c in corpus_list])
        outer=get_tqdm(corpus_list,desc='[LLTK] Syncing across corpora')
        for c in outer:
            outer.set_description(f'[LLTK] Syncing: {c}')
            try:
                C=Corpus(c)
                # if C.id in not_corpora: continue
                C.sync(progress=True)
            except Exception as e:
                log.error(e)
            

    ## Other
    def log_on(self): return log_on()
    def log_off(self): return log_off()
    def is_logged_on(self): return is_logged_on()
    @property
    def online(self): return online_access()
    @property
    def offline(self): return online_access()