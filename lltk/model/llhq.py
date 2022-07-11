from lltk.imports import *
import time

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
    
    # def get(self,id=None,lim=None,**meta):
    #     now=time.time()
    #     if log: log(f'<- {id}')
    #     if id: return Text(id).init()
    #     tl=TextList([x for i,x in enumerate(self.get_iter(**meta)) if not lim or i<lim])
    #     if log: log(f'-> {tl} in {round(time.time()-now,2)}s')
    #     return tl
    
    ## Finding by string contains

    def search(self,lim=None,progress=True,as_text=False,**meta):
        now=time.time()
        if log: log(f'<- {meta}')
        if len(meta)==1 and 'corpus' in meta: return Corpus(meta['corpus'])

        future_id_ld,extra_conds=self.cdb.search(**meta)
        # if log: log(f'got future: {future_id_ld}')
        tl=TextList(future_id_ld,extra_conds=extra_conds)
        if log: log(f'-> finished in {round(time.time()-now,2)}s')
        return tl
        
    def get(self,**kwargs):
        return self.search(_keys_like=set(),**kwargs)


    def sync(self,corpus_list=INIT_DB_WITH_CORPORA,not_corpora=NOT_CORPORA,progress=True):
        if type(corpus_list)==str:
            corpus_list = [x.strip() for x in corpus_list.split(',') if x.strip()]
        else:
            corpus_list=list(corpus_list)

        if len(corpus_list)==1:
            progress=False
        else:
            corpus_list.sort()
        
        outer = corpus_list if not progress else get_tqdm(corpus_list,desc='[LLTK] Syncing across corpora')
        
        for c in outer:
            if progress: outer.set_description(f'[LLTK] [Syncing]: {c}')
            try:
                C=Corpus(c)
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




