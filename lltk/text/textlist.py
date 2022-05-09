from lltk.imports import *
from .utils import *
from .text import *


class TextList(BaseObject, UserList):
    def __init__(self, l=[], progress=False, unique=True):
        if l and progress: l = get_tqdm(l,desc='[LLTK] init texts',leave=True)
        self.data_all = [Text(x).init() for x in l if x is not None and x]
        self.data_uniq = None
        self.unique = unique

    def __iter__(self): yield from self.iter_texts_uniq(_progress=False)

    # def addrs(self,**kwargs):
    #     if not self._addrs: self._addrs=[t.addr for t in self.iter_texts_each(_progress=True)]
    #     return self._addrs

    @property
    def data(self):
        if not self.unique: return self.data_all
        if not self.data_uniq: self.data_uniq = self.filtered()
        if not self.data_uniq: return self.data_all
        return self.data_uniq

    @property
    def addrs(self):
        return [t.addr for t in self.iter_texts_each(_progress=True)]
    
    

    def filtered(self,text_iter=None,progress=True):
        if text_iter is None: text_iter = self.data_all
        return sorted(
            self.iter_texts_uniq(
                self.data_all,
                _progress=progress,
                _desc='[LLTK] filtering texts',
                _leave=False,
            ),
            key=lambda t: t.year
        )
    
    def iter_texts(self,text_iter=None,_unique=True,**kwargs):
        if _unique:
            yield from self.iter_texts_uniq(text_iter,**kwargs)
        else:
            yield from self.iter_texts_each(text_iter,**kwargs)


    def init(self,progress=True,**kwargs):
        for t in self.iter(progress=progress,desc='initializing texts'):
            t.init(**kwargs)
        # self.find_matches()
        return self

    def iter_texts_uniq(
            self,
            text_iter=None,
            _progress=True,
            _desc='[LLTK] iterating distinct texts',
            _leave=True,
            **kwargs):
        if text_iter is None:
            text_iter = self.data_uniq
            if text_iter is not None:
                if not _progress:
                    yield from text_iter
                else:
                    text_iter=get_tqdm(text_iter,desc=_desc,leave=_leave)
                    for t in text_iter:
                        # text_iter.set_description(f'{_desc}: {t}')
                        yield t
        else:
            # text_iter = [Text(t) for t in text_iter if t is not None]
            o = []            
            import networkx as nx
            g = nx.Graph()
            for t in text_iter:
                t = Text(t)
                g.add_node(t.addr)
                for t2 in t.sources:
                    g.add_edge(t.addr, t2.addr)
            cmps=list(nx.connected_components(g))
            if _progress: cmps=get_tqdm(cmps,desc=_desc,leave=_leave)
            for nodeset in cmps:
                nset=list(nodeset)
                nset.sort(key=lambda x: CORPUS_SOURCE_RANKS.get(to_corpus_and_id(x)[0],1000))
                t=Text(nset[0])
                # if _progress: cmps.set_description(f'{_desc}: {t}')
                yield t
                o.append(t)
            self.data_uniq = o


        


    def iter_texts_each(
            self,
            text_iter=None,
            _progress=True,
            # _unique=True,
            _desc='[LLTK] iterating all texts',
            **kwargs):

        done = set()
        if text_iter is None:
            text_iter = self.data_all
            if self.data_all:
                _progress = False
        if _progress: text_iter=get_tqdm(text_iter,desc=_desc)
        for t1 in text_iter:
            tlx = [t1] + t1.sources
            for t in tlx:
                if t.addr not in done:
                    yield t
                    done|={t.addr}

    texts = iter_texts
    iter_all_texts = iter_texts
    iter = iter_texts


    def quiet(self): self.progress=False
    def verbose(self): self.progress=True
    
    
    # def init(self,progress=True,**kwargs):
    #     for t in self.iter(progress=progress,desc='initializing texts'):
    #         t.init(**kwargs)

    def sort(self, obj = None, *args, by_nsrc=False, by_addr=False, by_year=True, **kwds):
        if obj is None: obj = self.data
        if by_year: obj.sort(key=lambda t: t.year)
        

    @property
    def t(self): return random.choice(self.data)

    def sample(self,n): 
        if n < len(self.data): return random.sample(self.data,n)
        o = [x for x in self.data]
        random.shuffle(o)
        return o







    def run(self,func,text_iter=None,*args,**kwargs):
        return llmap(
            self.addrs,
            func,
            *args,
            **kwargs
        )
    map = run
                




































    def matchgraph(self,draw=True,**kwargs):
        from lltk.model.networks import draw_nx
        
        g = None
        for t in self.iter_texts_uniq(**kwargs):
            tg=t.matchgraph(draw=False)
            g = tg if g is None else nx.compose(g,tg)
        # g = nx.Graph()
        # for t in text_iter:
        #     for t2 in t.sources:
        #         g.add_edge(t.addr,t2.addr)
        
        if log: log(f'number of nodes = {g.order()}, number of edges = {g.size()}')
        
        return g if not draw else draw_nx(g)



    def find_matches(self,**kwargs):
        if log: log('finding by title...')
        matchdf = self.find_matches_by_title(**kwargs)
        if log: log(f'found {len(matchdf)} matches by title')
        
        if log: log('finding by hash...')
        hmatchl = self.find_matches_by_hash(**kwargs)
        if log: log(f'found {len(hmatchl)} matches by hash')
        
        self.data_uniq = self.filtered()

    def find_matches_by_hash(self, texts_iter=None, lsh=None, threshold=0.95, progress=True):
        # Approximate
        if texts_iter is None: texts_iter = self.iter_texts_each()
        texts = []
        if lsh is None:
            from datasketch import MinHashLSH
            lsh = MinHashLSH(threshold=threshold, num_perm=128*2)
            
            for t in texts_iter:
                try:
                    minhash = t.minhash()
                    if minhash:
                        lsh.insert(t.addr, minhash)
                        texts.append(t)
                except Exception as e:
                    self.log.error(e)
        
        # for t in LLTK(author='Gibson'):
        o=[]
        if texts:
            iterr = texts
            if progress and len(texts)>=0: iterr=get_tqdm(texts,desc='[LLTK] Matching texts')
            for t in iterr:
                for t2addr in lsh.query(t.minhash()):
                    if t2addr!=t.addr:
                        t2=Text(t2addr)
                        t.add_source(t2,reltype='minhash')
                        o.append((t.addr,t2.addr))
                        # iterr.set_description(f'[LLTK] Matching: {t.addr} -> {t2.addr}')
        return o


    def find_matches_by_title(self,texts_iter=None,compare_by=DEFAULT_COMPAREBY,method_string='levenshtein',full=False,force=False,cache=True,add=True,**kwargs):
        # get corpora
        if texts_iter is None: texts_iter = self.iter_texts_each()
        df = pd.DataFrame(
            dict(id=t.addr, author=t.au, title=t.shorttitle)
            for t in texts_iter
            if t.au and t.shorttitle
        )
        if not len(df): return df

        df=df.set_index('id').dropna()

        # set up index
        import recordlinkage as rl
        indexer = rl.Index()
        indexer.block(left_on='author', right_on='author') if not full else indexer.full()
        # get candidates
        candidates = indexer.index(df, df)
        # set up comparison model
        c = rl.Compare()
        for k,v in compare_by.items():
            c.string(k,k,threshold=v,method=method_string) if v<1.0 else c.exact(k,k)
        res = c.compute(candidates, df, df)
        res.columns = [f'match_{k}' for k in compare_by]
        res['match_sum'] = res.sum(axis=1)
        res['match_rel'] = res['match_sum'] / len(compare_by)
        res['match'] = res['match_rel'] == 1
        res=res.reset_index()
        res = res[res.id_1 != res.id_2]
        res = res[res.match==True]

        ## to match df
        def renamedf(df,n=1): return df.reset_index().rename({col:f'{col}_{n}' for col in df.reset_index().columns},axis=1)
        res = res.merge(renamedf(df,1),on='id_1')
        res = res.merge(renamedf(df,2),on='id_2')
        cols_sort = [c for c in res.columns if c.split('_')[0] in set(compare_by)]
        cols_unsort = [c for c in res.columns if c not in set(cols_sort)]
        res = res[cols_unsort + list(sorted(cols_sort))]
        res = res.set_index(['id_1','id_2']).sort_index()

        if add: add_matches_from_df(res)        
        
        return res

def add_matches_from_df(matchdf,key1='id_1',key2='id_2',progress=True):
    mdf=matchdf.reset_index()
    iterr=zip(mdf[key1],mdf[key2])
    # if progress: iterr=get_tqdm(list(iterr),desc='Adding matches')
    for id1,id2 in iterr: Text(id1).add_source(id2,reltype='title')
    return True
        