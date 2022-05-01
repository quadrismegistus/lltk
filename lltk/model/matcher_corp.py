from lltk.imports import *
import networkx as nx
from collections.abc import MutableMapping
MATCH_FN='matches.sqlite'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9, year=1.0)

# def MatcherModel(*args, corpus=None, **kwargs):
#     key=f'MatcherModel(corpus="{corpus}")'
#     if MATCHER is not None: return Match

class MatcherModel(BaseModel,MutableMapping):
    REL=MATCHRELNAME

    def __init__(self, *args, corpus=None, **kwargs):
        self.corpus=Corpus(corpus)
        self.id=self.corpus.id
        self.G=nx.Graph()
        self.store={}
        self.dd=self.store
        self._df=None
        self._db=None
        self._done=set()
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        # load?

    def __getitem__(self, key): return self.get_neighbs(key)
    def __setitem__(self, key, value): self.match(Text(key), Text(value))
    def __delitem__(self, key): self.G.remove_node(key) if self.G.has_node(key) else None
    def __iter__(self): return iter(self.G.nodes())
    def __len__(self): return self.G.order()
    def _keytransform(self, key): return key

    def get_rel(self,rel=None,**kwargs): return self.REL if not rel else rel
    def get_ent(self,text,**kwargs): return f'<lltk:{Text(text).addr}>'
    def get_key(self,t1,t2,rel=None,**kwargs): return f'{self.get_ent(t1)} {self.get_rel(rel)} {self.get_ent(t2)}'
    def get_neighbs(self,key):
        if not self.G.has_node(key): return []
        return list(self.G.neighbors(key))

    def get_db_sqlite(self,mode="c"):
        from sqlitedict import SqliteDict
        logging.getLogger('sqlitedict').setLevel(logging.WARNING)
        path=os.path.splitext(self.path_data)[0]+'.sqlite'
        ensure_dir_exists(path)
        return SqliteDict(path, outer_stack=False, tablename='edged', autocommit=True)
        
    def get_db_pickle(self):
        import pickledb
        path=os.path.splitext(self.path_data)[0]+'.pkldb'
        ensure_dir_exists(path)
        if log.verbose>0: log('Opening '+path)
        return pickledb.load(path, True)

    # def get_db_json(self):
    #     path=os.path.splitext(self.path_data)[0]+'.json'
    #     ensure_dir_exists(path)
    #     # return db
    #     return open(path,'w')

    def get_db_shelve(self,*args,**kwargs):
        import shelve
        path=os.path.splitext(self.path_data)[0]
        ensure_dir_exists(os.path.dirname(path), fn=False)
        return shelve.open(path)

    def get_db_pup(self):
        from pupdb.core import PupDB
        path=os.path.splitext(self.path_data)[0]+'.json'
        ensure_dir_exists(path)
        db = PupDB(self.path_data)
        return db

    def get_db(self,*args,**kwargs):
        if len(args) and args[0] is not None:
            odb=args[0]
        else:
            odb=self.get_db_shelve(*args,**kwargs)
        return odb


    def load(self,force=False,db=None,verbose=False):
        # log('Loading match db...')
        with self.get_db(db) as db:
            for estr in db.keys():
                edged = db.get(estr)
                u,v,rel=edge=tuple(estr.split('||'))
                if log.verbose>0: log(f'{u} -> {v}')
                self.add_edge_to_graph(edge,force=force,**edged)


    @property
    def path_data(self,match_fn=MATCH_FN): return os.path.join(self.corpus.path_data,match_fn)

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

    def match(self, text, source, rel=MATCHRELNAME, force=False, save=True, **edged):
        u,v,rel = (Text(text), Text(source), rel)
        if u and v and u.id_is_valid() and v.id_is_valid() and rel:
            edge = (u.addr, v.addr, rel)
            if log.verbose>0: log(f'Edge: {edge}')
            if self.add_edge_to_graph(edge, force=force, **edged):
                self.add_edge_to_db(edge,**edged)

    def match_records(self,C1,C2,compare_by=DEFAULT_COMPAREBY,method_string='levenshtein',full=False,force=False,**kwargs):
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

    


    def add_edge_to_graph(self,edge,force=False,verbose=False,**edged):
        if force or edge not in self._done:
            g=self.G
            u,v,rel=edge
            u,v,rel=u.strip(),v.strip(),rel.strip()
            if u and v and rel:
                if not g.has_node(u): g.add_node(u,node_type='text',namespace='lltk')
                if not g.has_node(v): g.add_node(v,node_type='text',namespace='lltk')
                if not g.has_edge(u,v):
                    g.add_edge(u,v,rel=rel,**edged)
                    if log.verbose>0: log(f'[{self.id}] Adding to graph: {u} --> {v}')
                    self._done|={edge}
                    return True
                else:
                    for k,v in edged.items():
                        if log.verbose>0: log(f'Adding to edge: {u} --> {v} ({k} = {v})')

                        try:
                            g.edges[u,v][k]=v
                        except KeyError:
                            pass
        return False

    def set_db_key(self,key,val,db=None):
        return
        if not val: return
        oldval = self.get_db_key(key,db=db)
        if val != oldval:
            with self.get_db(db) as odb:
                #odb.set(key,val)
                odb[key]=val
            # log(f'Set in DB: "{key}" = {pformat(val)}')
        
    def get_db_key(self,key,db=None):
        return 
        with self.get_db(db) as odb: val=odb.get(key)
        # if val is not None: 
            # log(f'Got from DB: "{key}" = {pformat(val)}')
        return val

    def add_edge_to_db(self,edge,**edged):
        u,v,rel=edge
        if log.verbose>0: log(f'[{self.id}] Adding to DB: {u} --> {v}')
        odx={k:v for k,v in edged.items() if k and k[0]!='_'}
        estr='||'.join(edge)
        self.set_db_key(estr,odx)









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