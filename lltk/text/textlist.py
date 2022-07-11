from lltk.imports import *
from .utils import *
from .text import *

from cassandra.cluster import ResponseFuture

class TextList(BaseObject, UserList):
    def __init__(self, l_or_promise_of_l=[], progress=False, unique=True,extra_conds={}):
        self.unique = unique
        self._l = l_or_promise_of_l
        self._data_all = None
        self._data_uniq = None
        self.extra_conds=extra_conds
        self._g=None

        if is_iterable(self._l):
            self._data_all = [x for x in self._l]
        elif isinstance(self._l, ResponseFuture):
            future = self._l
            # if log: log(f'I am a future: {future}. Adding callbacks...')
            def callback(*x,**y): self.data_all # init
            future.add_callback(callback)

    
    @property
    def data_all(self,force=True):
        if not self.__data_all:

            if not self._data_all: 
                if log: log('Nothing here yet! Waiting...')
                return []
                # if self._l is not None: l = list(self._l.result())
            
            l=self._data_all
            # if log: log(f'Got result: {len(l)}')    
            # self._l is None
            # # filter?
            l = self.filter_extra_conds(l)
            if log: log(f'After filtering: {len(l)}')
            if not l: return []  
            # l=[Text(x,_cache=False) for x in l]
            if log: log(f'After textifying: {len(l)}')

            log.info('converting to text...')
            self.__data_all=self._data_all=[Text(x) for x in l]
            log.info('done')
            
        return self.__data_all


        return sorted(self._data_all,key=lambda t: t.year)
    all=data_all

    def filter_extra_conds(self,res_ld,extra_conds=None):
        if extra_conds is None: extra_conds=self.extra_conds
        if not extra_conds: return res_ld
        o=[]
        for d in res_ld:
            for condkey in extra_conds:
                if condkey in d:    
                    for condop,condval in extra_conds[condkey]:
                        if condkey and condop and condval:
                            # run
                            dval = d.get(condkey)
                            if condop=='LIKE':
                                if str(condval) in str(dval):
                                    o.append(d)
                            else:
                                if condval == dval:
                                    o.append(d)
        return o
        
    
    def queue_remote_sources(self):
        for t in self: t.queue_remote_sources()
    def init(self):
        for t in self: t.queue_remote_sources()


    
    @property
    def data_uniq(self):
        if not self._data_uniq: self.filter()
        return self._data_uniq
    uniq=data_uniq

    def __iter__(self): yield from self.data #iter_texts_uniq(_progress=False)
    def __len__(self): return len(self.data_all)

    def __repr__(self,maxnum=25):
        pref='TextList('
        if type(self._data_all)!=list: return f'[TextList] (loading...)'
        iterr = self._data_uniq if self._data_uniq else self.data_all
        
        # iterr=self.sort(iterr)
        o=[]
        for i,t in enumerate(sorted(iterr,key=lambda t: t.year)):
            if i:
                prefx=' '*(len(pref)+1)
            else:
                prefx='['
            o+=[prefx + str(t)]
        o='\n'.join(o)
        if o: return pref + o + '])'
        return f'[TextList]({len(self.data_all)} texts)'

    @property
    def data(self):
        o=None
        if self.unique and self._data_uniq: o=self._data_uniq
        if not o: o=self.data_all
        if not o: o=[]
        return sorted(o,key=lambda t: t.year)

    @property
    def addrs(self):
        return [t.addr for t in self.iter_texts_each(_progress=True)]
    
    

    def filter(self,text_iter=None,**kwargs):
        if text_iter is None: text_iter = self.data_all
        self._data_uniq = sorted(
            list(self.iter_texts_uniq(self.data_all,**kwargs)),
            key=lambda t: t.year
        )
        if log: log(f'data_all = {len(self.data_all)}, _data_uniq = {len(self._data_uniq)}')
        return self._data_uniq
    filtered=filter
    
    def iter_texts(self,text_iter=None,_unique=True,**kwargs):
        if _unique and self._data_uniq:
            yield from self._data_uniq
        else:
            yield from self.data_all


    def init(self,progress=True,**kwargs):
        for t in self.data_all: t.init()
        return self

    def iter_texts_uniq(
            self,
            progress=False,
            force=True,
            force_inner=True,
            desc='[LLTK] iterating distinct texts',
            leave=True,
            **kwargs):

        if False: #not force and self._data_uniq:
            yield from self._data_uniq
        else:
            self._g = g = self.get_matchgraph() if (True or not self._g) else self._g
            if log: log(f'<- matchgraph! = {g}')
            if g and isinstance(g,nx.Graph):
                cmps=list(nx.connected_components(g))
                if 0: cmps=get_tqdm(cmps,desc=desc,leave=leave)
                for i,nodeset in enumerate(cmps):
                    nset=list(nodeset)
                    nset.sort(key=lambda x: CORPUS_SOURCE_RANKS.get(to_corpus_and_id(x)[0],1000))
                    t=Text(nset[0])
                    if log: log(f'{i} {t}')
                    if 0: cmps.set_description(f'{desc}: {t}')
                    yield t


        


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

    def sort(self, obj = None, *args, by_nsrc=False, by_addr=False, by_year=True, **kwds):
        if obj is None: 
            if self._data_all: self._data_all.sort(key=lambda t: t.year)
            if self._data_uniq: self._data_uniq.sort(key=lambda t: t.year)
        else:
            obj.sort(key=lambda t: t.year)
        self
        
        

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
                
    def get_matchgraph(self,node_name='addr'):
        g = nx.Graph()
        for t in self.data_all:
            tg=t.matchgraph(draw=False,node_name='addr')
            g = nx.compose(g,tg)

        for node in list(g.nodes()):
            if IDSEP_START+TMP_CORPUS_ID+IDSEP in node:
                g.remove_node(node)

        if node_name!='addr':
            labeld=dict((addr,Text(addr).node) for addr in g.nodes())
            nx.relabel_nodes(g,labeld,copy=False)
        return g
        

    def matchgraph(self,draw=True,node_name='node',**kwargs):
        from lltk.model.networks import draw_nx
        g=self.get_matchgraph(node_name=node_name)
        return g if g is None or not draw else draw_nx(g)



    def match(self,callback=None,verbose=True,**kwargs):
        # input
        if log: log('...')
        df = pd.DataFrame(
            dict(id=t.addr, author=t.au, title=t.shorttitle)
            for t in self
            if t.au and t.shorttitle
        ).set_index('id')
        ofn=f'.tmp.data.{zeropunc(str(time.time()))}.pkl'
        df.to_pickle(ofn)
        wasnum_uniq = len(self.data_uniq)
        if log: log('llcode...')
        code="""

df=pd.read_pickle('%s')

now=time.time()

if log: log('finding by title...')
from lltk.model.matcher import match_by_title

res=match_by_title(df)
if %s:
    log.info(
        f'found {len(res)} title matches in {round(time.time()-now,2)}s'
    )
""" % (ofn,verbose)

        def callback(res):
            rmfn(ofn)
            if verbose:
                num_uniq = len(self.filtered())
                if wasnum_uniq != num_uniq:
                    log.info(f'Filtered from {wasnum_uniq} to {num_uniq} distinct texts:\n{self}')

        log.info('searching for matches in background process')
        llcode(code,callback=callback)
        # return self

def hellofunc():
    o=[]
    for n in range(5):
        x=f'Hello, {n}...'
        print(x)
        time.sleep(random.random())
        o.append(x)
    return o
def callback(x): print('call back done!!')

def do_and_then(func,*args,callback=None,**kwargs):
    print(func,args,kwargs)
    if callback: print(f'callback = {callback}')
    pool = mp.Pool(1)
    res = pool.apply_async(
        func,
        args=args,
        kwds=kwargs,
        callback=callback
    )
    return res#.get(timeout=15)
