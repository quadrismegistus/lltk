from lltk.imports import *
from .utils import *
from .text import *

class TextList(BaseObject, UserList):
    def __init__(self, data=[], progress=False, unique=True):
        self.unique = unique
        self._data_all = None
        self._data_uniq = None
        self._g=None
        self._gdb=None

        if is_iterable(data):
            self._data_all = list(data)

    @property
    def data_all(self):
        if self._data_all is None:
            return []
        return self._data_all
    all=data_all

    def __iter__(self): yield from self.data
    def __len__(self): return len(self.data_all)

    def __repr__(self,maxnum=25):
        pref='TextList('
        if self._data_all is None: return f'[TextList] (loading...)'
        iterr = self._data_uniq if self._data_uniq else self.data_all
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

    @property
    def data_uniq(self):
        if not self._data_uniq: self.filter()
        return self._data_uniq

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
            desc='[LLTK] iterating distinct texts',
            leave=True,
            **kwargs):

        self._g = g = self.get_matchgraph()
        if g and isinstance(g,nx.Graph):
            cmps=list(nx.connected_components(g))
            for i,nodeset in enumerate(cmps):
                nset=list(nodeset)
                nset.sort(key=lambda x: CORPUS_SOURCE_RANKS.get(to_corpus_and_id(x)[0],1000))
                t=Text(nset[0])
                yield t

    def iter_texts_each(
            self,
            text_iter=None,
            _progress=True,
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

    def sort(self, obj=None, **kwds):
        if obj is None:
            if self._data_all: self._data_all.sort(key=lambda t: t.year)
            if self._data_uniq: self._data_uniq.sort(key=lambda t: t.year)
        else:
            obj.sort(key=lambda t: t.year)

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
