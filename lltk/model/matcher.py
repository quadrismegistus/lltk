from lltk.imports import *
import networkx as nx
from collections.abc import MutableMapping
MATCH_FN='matches.sqlite'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9)
DEFAULT_MATCHER_ID='semantic_web'

MATCHERMODELD={}
def Matcher(id=DEFAULT_MATCHER_ID,force=False,**kwargs):
    global MATCHERMODELD
    idx=id.id if is_corpus_obj(id) else id
    if force or type(MATCHERMODELD.get(idx)) != MatcherModel:
        if log.verbose>0: log(f'<- {idx}')
        MATCHERMODELD[idx] = m = MatcherModel(id,**kwargs)
        if log.verbose>0: log(f'-> {m}')
    return MATCHERMODELD[idx]


class MatcherModel(BaseModel,MutableMapping):
    REL=MATCHRELNAME

    def __init__(self,
            id=DEFAULT_MATCHER_ID,
            path=PATH_LLTK_MATCHES,
            init=True,
            corpus=None,
            clear=False,
            *args,
            **kwargs):
        
        self._G=None
        if is_corpus_obj(id):
            C=id
            self.id=C.id
            self.path=C.path_matches
        else:
            self.id=id
            self.path=path
        
        
        if clear: self.clear()
        elif init: self.init(force=True)


    
        
    def __getitem__(self, key): return set(self.get_connected(key))
    def __setitem__(self, key, value): self.match(key,value)
    def __delitem__(self, key): self.remove_node(key)
    def __iter__(self): return iter(self.G.nodes())
    def __len__(self): return self.G.order()
    
    def __getattr__(self,name):
        res = getattribute(self,name)
        if res is None:
            if name.startswith('path_'):
                return self.pathd.get(name[5:])


    
    
    @property
    def G(self): 
        if self._G is None: self.init()
        return self._G
    g=G




    @property
    def pathd(self):
        return dict(
            root=self.path,
            db=self.path,
            triples=os.path.join(self.path, 'triples.txt'),
            edgelist=os.path.join(self.path, 'edgelist.txt')
        )
    
    def db(self,*args,**kwargs): return DB(self.path_db)
    
    
    
    def cache(self,db=False,g=True):
        if db: self.cache_db()
        if g: self.cache_g_triples()

    def init(self,force=False,g=True,db=False):
        if force or self._G is None:
            if log.verbose>1: log(f'...')
            if g: self._G=self.init_g()
            elif db: self._G=self.init_db()
        if self._G is None: self._G=nx.MultiGraph()

    def cache_db(self,g=None):
        g=g if g else self.g
        if g is not None:
            if log.verbose>0: log(f'caching match db')
            self.db().set(self.id,g)

    def cache_g_edgelist(self,g=g,data=True,**kwargs):
        g=g if g else self.g
        if g is not None:
            nx.write_edgelist(
                g,
                self.path_edgelist,
                data=data,
                delimiter='\t',
                **kwargs
            )
            if log.verbose>0: log(f'cached edgelist to {self.path_edgelist}')
    
    def cache_g_triples(self,g=None,pref='lltk:'):
        g=g if g else self.g
        if g is not None and self.path_triples:
            ensure_dir_exists(self.path_triples)
            with open(self.path_triples,'w') as of:
                for u,v,d in g.edges(data=True):
                    if 'rel' in d:
                        rel=d.pop('rel')
                    else:
                        rel=MATCHRELNAME

                    # o=f'lltk:{str(u):<50} {str(rel):<12} lltk:{str(v):<50} {str(d):<12}\n'
                    # o=f'{u}\t{rel}\t{v}\t{d if d else ""}'
                    o=f'{pref}{u}\t{rel}\t{pref}{v}\t{d if d else ""}'
                    of.write(o.strip() + '\n')
                if log.verbose>0: log(f'cached graph to {self.path_triples}')

    
    
    def init_g(self): return self.init_g_triples()            
    def init_g_triples(self,pref='lltk:'):
        if os.path.exists(self.path_triples):
            with open(self.path_triples) as f:
                g=nx.MultiGraph()
                for ln in f:
                    ln=ln.strip()
                    if ln:
                        try:
                            lndat = ln.split('\t')
                            if len(lndat)==3:
                                u,rel,v = lndat
                                d={}
                            elif len(lndat)==4:
                                u,rel,v,dstr = lndat
                                d=json.loads(dstr) if dstr else {}
                            else:
                                continue
                            ux,vx=u[len(pref):],v[len(pref):]
                            edge = (ux,vx,rel)
                            if self.add_edge_to_graph(edge, g=g):
                                #g.add_edge(ux,vx,key=rel,rel=rel,**d)
                                if log.verbose>1: log(f'{u} --{rel}--> {v}')
                        except Exception as e:
                            log.error(e)
                
                if log.verbose>0: log(f'read graph from {self.path_triples}')
                return g


    def init_g_edgelist(self):
        if os.path.exists(self.path_edgelist):
            g=nx.read_edgelist(
                self.path_edgelist,
                delimiter='\t',
                create_using=nx.MultiGraph
            )
            # log(pf(list(g.edges(data=True))[:5]))
            if g is not None:
                if log.verbose>0: log(f'initializing match graph from {self.path_edgelist}')
                return g
    
    def init_db(self):
        G=self.db().get(self.id)
        if G is not None:
            if log.verbose>1: log(f'initializing match graph from db')
            return G
    

    

    def clear(self):
        self.db().delete(self.id)
        if log.verbose>0: log(f'removing: {self.path_g}')
        rmfn(self.path_g)
        self._G=nx.MultiGraph()




    def match(self, text, source, rel=MATCHRELNAME, force=False, cache=True, **edged):
        u,v = Text(text), Text(source)
        if log.verbose>1: log(f'Match? {u} --{rel}--> {v} [cache={cache}, force={force}]')
        if rel:# and u.id_is_valid() and v.id_is_valid():
            edge = (u.addr, v.addr, rel)
            if log.verbose>2: log(f'In graph? {edge}')
            if self.add_edge_to_graph(edge, force=force, **edged):
                if log.verbose>1: log(f'Matching: {u} --> {v}')
                if cache: self.cache()
                return True


    def add_edge_to_graph(self,edge,g=None,force=False,verbose=False,**edged):
        g=self.G if g is None else g
        u,v,rel=edge
        if u and v and rel:
            if not g.has_node(u): g.add_node(u,node_type='text',namespace='lltk')
            if not g.has_node(v): g.add_node(v,node_type='text',namespace='lltk')
            if not g.has_edge(u,v,key=rel):
                g.add_edge(u,v,key=rel,rel=rel)#,**edged)
                if log.verbose>1: log(f'{u} --{rel}--> {v}')
                return True
            else:
                return None
        return False

    def remove_node(self,node,cache=True):
        if self.G.has_node(node):
            if log.verbose>0: log(f'removing {node} from matchdb')
            self.G.remove_node(node)
            if cache: self.cache()
    
    def remove_nodes(self,nodes,cache=True):
        for node in nodes: self.remove_node(node,cache=False)
        if cache: self.cache()
        
    





















    def find_matches(self,C1,C2=None,compare_by=DEFAULT_COMPAREBY,method_string='levenshtein',full=True,force=False,cache=True,**kwargs):
        # get corpora
        C1=Corpus(C1)
        C2=C1 if C2 is None else Corpus(C2)

        # get minimal records
        df1,df2=reclink_get_df(C1),reclink_get_df(C2)

        # set up index
        import recordlinkage as rl
        indexer = rl.Index()
        indexer.block(left_on='year', right_on='year') if not full else indexer.full()
        # get candidates
        candidates = indexer.index(df1, df2)
        # set up comparison model
        c = rl.Compare()
        for k,v in compare_by.items(): c.string(k,k,threshold=v,method=method_string) if v<1.0 else c.exact(k,k)
        res = c.compute(candidates, df1, df2)
        res.columns = [f'match_{k}' for k in compare_by]
        res['match_sum'] = res.sum(axis=1)
        res['match_rel'] = res['match_sum'] / len(compare_by)
        res['match'] = res['match_rel'] == 1
        res=res.reset_index()
        res = res[res.id_1 != res.id_2]
        res = res[res.match==True]
        
        ## to match df
        def renamedf(df,n=1): return df.reset_index().rename({col:f'{col}_{n}' for col in df.reset_index().columns},axis=1)
        res = res.merge(renamedf(df1,1),on='id_1')
        res = res.merge(renamedf(df2,2),on='id_2')
        cols_sort = [c for c in res.columns if c.split('_')[0] in set(compare_by)]
        cols_unsort = [c for c in res.columns if c not in set(cols_sort)]
        res = res[cols_unsort + list(sorted(cols_sort))]
        res = res.set_index(['id_1','id_2']).sort_index()

        # to matches
        for addr1,addr2 in res.index:
            self.match(addr1, addr2,force=force, cache=False, **kwargs)
        if cache: self.cache()

        return res

    
    def data(self,g=None,**kwargs):
        if g is None: g=self.G
        o=[]
        for u,v,d in list(g.edges(data=True)):
            text=Text(u)
            source=Text(v)
            matchd=dict(
                title1=text.title,
                title2=source.title,
                author1=text.author,
                author2=source.author,
                query1=text.qstr,
                query2=source.qstr,
            )
            od={**d, **matchd}
            o.append(od)
        return pd.DataFrame(o).fillna('').sort_values('id')        






    def get_rel(self,rel=None,**kwargs): return self.REL if not rel else rel
    def get_ent(self,text,**kwargs): return f'<lltk:{Text(text).addr}>'
    def get_key(self,t1,t2,rel=None,**kwargs): return f'{self.get_ent(t1)} {self.get_rel(rel)} {self.get_ent(t2)}'
    
    def get_neighbs(self,key):
        if is_text_obj(key): key=key.addr
        if not self.G.has_node(key): return set()
        return set(self.G.neighbors(key)) - {key}
    
    def get_connected(self,key):
        if is_text_obj(key): key=key.addr
        if not self.G.has_node(key): return set()
        return set(nx.shortest_path(self.G,key).keys()) - {key}







def reclink_get_df(C,key_author='au',key_title='shorttitle',key_year='year',**kwargs):
    odf=pd.DataFrame([
        {
            'id':t.addr,
            'author':getattr(t,key_author),
            'title':getattr(t,key_title),
            'year':getattr(t,key_year),
        }
        for t in C.texts(**kwargs)
    ])
    if len(odf):
        odf=odf.set_index('id')
        odf['year']=odf['year'].apply(force_float)
    return odf