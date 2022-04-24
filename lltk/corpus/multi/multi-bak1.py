from re import L
from lltk.imports import *
from lltk.model.networks import *

BAD_PROPVALS = {'','Q0',None,np.nan}
BAD_PROPS={'query','wd_author_match','wd_title_match','corpus','ocr_accuracy','dob'}
OK_PROPS = None
BAD_NODES= {'Vnull','Q0','P0','V0','X0'}

def get_node_type(node): return node.split('=',1)[0]


class MultiText(BaseText): pass

class MultiCorpus(BaseCorpus):
    ID='multi_corpus'
    NAME='MultiCorpus'
    TEXT_CLASS = MultiText

    def __init__(self,corpora=[],id=None,*args,**kwargs):
        super().__init__(id = id if id else self.ID, *args,**kwargs)
        self._corpusd={}
        if corpora: self.add_corpus(corpora)
    
    def corpus_texts(self,*x,**y):
        for corpus in self.corpora():
            yield from corpus.texts()

    def add_corpora(self,corpora): self.add_corpus(corpora)

    def add_corpus(self,corpus):    
        if type(corpus) in {list,tuple,dict}:
            for corp in corpus:
                self.add_corpus(corp)
                return
        
        if type(corpus)==str:
            if ',' in corpus:
                for corp in corpus.split(','):
                    self.add_corpus(corp.strip())
                return
        
        # otherwise
        corpusobj = Corpus(corpus)
        self._corpusd[corpusobj.id]=corpusobj
        # for t in corpusobj.texts(): self.text(t.addr)

    def corpora(self): return list(self._corpusd.values())
    def metadata(self,fillna='',**kwargs):
        o=[c.metadata(**kwargs) for c in self.corpora()]
        return pd.concat(o).fillna(fillna) if o else pd.DataFrame()

    def graph(self,
            node_types={},
            g=None,
            texts=None,
            force=True,
            ok_props=OK_PROPS,
            bad_props=BAD_PROPS,
            bad_propvals=BAD_PROPVALS,
            bad_nodes=BAD_NODES,
            incl_text_nodes=True,
            rel_types={},
            only_identical=False,
            col_id=COL_ADDR,
            min_degree=None,
            min_weight=None,
            remove_isolates=True,
            **kwargs):
        #if force or self._g is None:
        #    self._g = g = nx.MultiGraph()


        if g is None:
            g = nx.MultiGraph()
            for t in self.texts(texts):
                tmeta = t.metadata(wikidata=False)
                #tnode = tmeta[self.col_addr] = t.addr
                #tnode = t.meta.get(self.col_addr)
                tnode = t.addr
                
                for propname,propval in tmeta.items():
                    if is_text_obj(propval): propval=propval.addr
                    u, v, rel = tnode, propval, propname
                    if u == v: continue
                    if rel_types and rel not in rel_types: continue
                    if u in bad_nodes or v in bad_nodes: continue
                    if rel.startswith('_id_'):
                        id_corpus=rel.split('_id_',1)[-1]
                        rel='_addr_'+id_corpus
                        # if rel in tmeta: continue
                        v = IDSEP_START + id_corpus + IDSEP + v
                    if u in bad_nodes or v in bad_nodes: continue 
                    if only_identical:
                        if not rel.startswith(self.col_addr):
                            continue
                        else:
                            u1,u2 = to_corpus_and_id(u)
                            v1,v2 = to_corpus_and_id(v)
                            if u2 in bad_nodes or v2 in bad_nodes:
                                continue

                    # if u.startswith(IDSEP_START + self.id + IDSEP):
                        # continue

                    for node in [u,v]:
                        noded=dict(node_type = 'text' if rel.startswith(self.col_addr) else 'prop')
                        if noded['node_type']=='text':
                            noded['id_corpus'],noded['id_text'] = to_corpus_and_id(node)
                            if noded['id_corpus'] == self.id:
                                noded['node_type']='text_root'
                        if not g.has_node(node): g.add_node(node, **noded)

                    log.debug(f'Adding edge: {u} --({rel})--> {v}')
                    g.add_edge(u, v, rel = rel)

        g = filter_graph(g=g,min_degree=min_degree,min_weight=min_weight,remove_isolates=remove_isolates,**kwargs)
        return g


    def yield_addrs(meta,addr_prefix='_addr_', id_prefix='_id_'):
    od={}
    for k,v in meta.items():
        rel,addr,corpid = '','',''
        if k.startswith(addr_prefix):
            rel=k
            addr = v
            corpid=k[len(addr_prefix):]
        elif k.startswith(id_prefix):
            corpid=k[len(id_prefix):]
            rel=addr_prefix+corpid
            addr=f'{IDSEP_START}{corpid}{IDSEP}{v}'
        if rel and addr and corpid and not rel in od and corpid != meta.get('_corpus'):
            od[rel]=addr
    yield from od.items()


def graph_is(self,g=None,id_prefix='_id_',rel_is='_is_',**kwargs):
    g = nx.MultiGraph()
    for i,text in enumerate(self.corpus_texts()):
        meta = text.meta
        for rel,addr in yield_addrs(meta):
            u,v,r=text.addr, addr, rel
            uT,vT=Text(u),Text(v)
            if uT.is_valid() and vT.is_valid():
                g.add_edge(u,v,rel=r,uT=uT, vT=vT)
                # print(f'{u} --{r}--> {v}')
    return g
        
    

    def top_nodes(self,g=None,topn=25,**kwargs):
        if g is None: g=self.graph(**kwargs)
        sdeg = pd.Series({
            k:v
            for k,v in dict(g.degree()).items()
            if not k.startswith('_id') and not k.startswith('_addr')
        }).sort_values(ascending=False)
        return sdeg.head(topn)
    
    def connect(self,texts=None):
        pass

    def neighbors(self,node,g=None,data=True,**kwargs):
        if g is None: g=self.graph(**kwargs)
        yield (node, g.nodes[node], {})
        for neighb in g.neighbors(node):
            if not data:
                yield neighb
            else:
                node_dat = g.nodes[neighb]
                dat = g.get_edge_data(node,neighb)
                if dat and 0 in dat: dat=dat[0]
                yield (neighb,node_dat,dat)

    def neighbors_d(self,node,rel_key='rel'):
        d={}
        for neighb,edge_dat in self.neighbors(node):
            if rel_key in edge_dat:
                key=edge_dat[rel_key]
                if key not in d:
                    d[key]=neighb
                elif type(d[key])==list:
                    d[key]+=[neighb]
                else:
                    d[key]=[d[key], neighb]
        return d

    # def neighbors_df(self,node,**kwargs):
    #     return pd.DataFrame(

    #     )
        
    
    def neighbors_graph(self,node,remove_seed=True,**kwargs):
        neighbs = self.neighbors(node,**kwargs)
        gsub = nx.Graph()
        for neighb in neighbs:
            for neighbs_neighb in self.neighbors(neighb):
                gsub.add_edge(neighb,neighbs_neighb)
        if remove_seed and gsub.has_node(node): gsub.remove_node(node)
        return filter_graph(gsub,**kwargs)
    
    
    # def neighbors_df(self,node,fillna='',**kwargs):
    #     neighbs_graph = self.neighbors_graph(node,**kwargs)
    #     odf=pd.DataFrame([
    #         self.text(idx.split('=',1)[-1]).meta
    #         for idx in neighbs_graph.nodes()
    #         if idx.startswith(f'{self.id}=')
    #     ]).fillna(fillna)
    #     if self.col_id in set(odf.columns): odf=odf.set_index(self.col_id)
    #     odf = odf.loc[odf.index.drop_duplicates()]
    #     return odf
        
    

    def nodes(self,node_types={},g=None,data=False,**kwargs):
        if g is None: g=self.graph(**kwargs)
        return [
            node if not data else (node,g.nodes[node])
            for node in g.nodes()
            if not node_types or get_node_type(node) in set(node_types)
        ]
    
    def subgraph(self,nodes=None,g=None,add_neighbors=True,**kwargs):
        if g is None: g=self.graph(force=True,**kwargs)
        if nodes is None: nodes=self.nodes(g=g,**kwargs)
        
        if add_neighbors:
            gsub = nx.Graph()
            for node in nodes:
                for neighb in g.neighbors(node):
                    edge = g.edges[(node,neighb)]
                    if not gsub.has_node(neighb): gsub.add_node(neighb)
                    gsub.add_edge(node,neighb,**edge)
        else:
            gsub = g.subgraph(nodes)
        gsub = filter_graph(gsub,**kwargs)
        return gsub
                

    def edges(**kwargs): pass
        

    def nodes_of_type(self,typename='text', g=None, **kwargs):
        if not g: g=self.graph(**kwargs)
        for node in g.nodes():
            if g.nodes[node].get('node_type') == typename:
                yield node

    def text_nodes(self,g=None,**kwargs):
        yield from self.nodes_of_type(g=g,typename='text')

    def graph_identity(self,g=None,id_prefix='_id_',rel_is='_is_',**kwargs):
        g = nx.MultiGraph()
        for i,t in enumerate(self.texts()):
            if i>3: break
            tmeta = t.metadata(from_sources=True)
            for k,v in tmeta.items():
                if k.startswith(id_prefix):
                    corpid=k[len(id_prefix):]
                    if corpid==t.corpus.id: continue
                    textid=v
                    addr=f'_{corpid}/{textid}'
                    g.add_edge(t.addr, addr, rel=rel_is)
        return g

    def graph_identity_matches(self,g=None,**kwargs):
        if not g: g=self.graph_identity(**kwargs)
        return g

    def components_identity(self,g=None,**kwargs):
        if not g: g=self.graph_identity(**kwargs)
        yield from nx.connected_components(g)

    def graph_identity_texts(self,g=None,**kwargs):
        g = self.graph_identity(g=g,**kwargs)
        gnew=nx.Graph()
        for connected_nodes in self.components_identity(g=g):
            connected_nodes_text = [node for node in connected_nodes if g.nodes[node].get('node_type')=='text']
            for node in connected_nodes_text:
                if not gnew.has_node(node): gnew.add_node(node,**g.nodes[node])
            edges=[(n1,n2) for n1 in connected_nodes_text for n2 in connected_nodes_text if n1<n2]
            rel='_is'
            for n1,n2 in edges: gnew.add_edge(n1,n2,rel=rel)
        
        return gnew

    def iter_identity_texts(self,g=None,**kwargs):
        if not g: g = self.graph_identity_texts(g=g,**kwargs)
        nodes=set()
        for node in get_tqdm(list(g.nodes())):
            node_text = Text(node)
            nodes|={node_text}
            for neighb in g.neighbors(node):
                neighb_text = Text(neighb)
                nodes|={neighb_text}
                node_text._is|={neighb_text}
                neighb_text._is|={node_text}
        yield from nodes
