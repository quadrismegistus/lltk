from lltk.imports import *
import networkx as nx
from collections.abc import MutableMapping
MATCH_FN='matches.sqlite'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9, year=1.0)


MATCHERMODEL=None
def get_matcher(force=False,**kwargs):
    global MATCHERMODEL
    if force or type(MATCHERMODEL) != MatcherModel: MATCHERMODEL = MatcherModel()
    return MATCHERMODEL
    

class MatcherModel(BaseModel,MutableMapping):
    REL=MATCHRELNAME

    def __init__(self, *args, **kwargs):
        self.G=nx.Graph()
        self._init=False

    def __getitem__(self, key): return set(self.get_neighbs(key))
    def __setitem__(self, key, value): self.match(key,value)
    def __delitem__(self, key): self.G.remove_node(key) if self.G.has_node(key) else None
    def __iter__(self): return iter(self.G.nodes())
    def __len__(self): return self.G.order()


    def db(self,*args,**kwargs): return DB('matches')

    def init(self,force=False,db=None,verbose=False):
        if force or not self._init:
            log.debug('Loading match db...')
            with self.db() as db:
                for estr,edged in db.items():
                    u,v,rel=edge=tuple(estr.split('||'))
                    if verbose: log.debug(f'{u} -> {v}')
                    self.add_edge_to_graph(edge,force=force,**edged)
            self._init=True

    def match(self, text, source, rel=MATCHRELNAME, force=False, save=True, **edged):
        u,v = Text(text), Text(source)
        # log.debug(f'u = {u}')
        # log.debug(f'v = {v}')
        # log.debug(f'rel = {rel}')
        # log.debug(f'u.is_valid = {u.is_valid()}')
        # log.debug(f'v.is_valid = {v.is_valid()}')
        if rel and u.id_is_valid() and v.id_is_valid():
            edge = (u.addr, v.addr, rel)
            if self.add_edge_to_graph(edge, force=force, **edged):
                log.debug(f'{u} --> {v}')
                self.add_edge_to_db(edge,**edged)



    def add_edge_to_graph(self,edge,force=False,verbose=False,**edged):
        g=self.G
        u,v,rel=edge
        did=False
        if u and v and rel:
            if not g.has_node(u): g.add_node(u,node_type='text',namespace='lltk')
            if not g.has_node(v): g.add_node(v,node_type='text',namespace='lltk')
            if not g.has_edge(u,v):
                g.add_edge(u,v,rel=rel,**edged)
                if verbose: log.debug(f'Adding to graph: {u} --> {v}')
                did=True
            else:
                #g.edges[(u,v)] = merge_dict(g.edges[(u,v)], edged)
                for ek,ev in edged.items(): 
                    try:
                        g.edges[(u,v)][ek]=ev
                    except KeyError as e:
                        log.error(f'{u} --X?--> {v}')
    
        return did

    def add_edge_to_db(self,edge,**edged):
        u,v,rel=edge
        log.debug(f'[{self.id}] Adding to DB: {u} --> {v}')
        odx={k:v for k,v in edged.items() if k and k[0]!='_'}
        estr='||'.join(edge)
        self.db().set(estr,odx)













    def find_matches(self,C1,C2,compare_by=DEFAULT_COMPAREBY,method_string='levenshtein',full=False,force=False,**kwargs):
        # get corpora
        C1,C2=Corpus(C1),Corpus(C2)

        # get minimal records
        df1,df2=reclink_get_df(C1),reclink_get_df(C2)
        # display(df1)
        # display(df2)

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
            # print(f'{addr1} --> {addr2}')
            self.match(addr1, addr2,force=force,**kwargs)


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
        if not self.G.has_node(key): return []
        return list(self.G.neighbors(key))







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