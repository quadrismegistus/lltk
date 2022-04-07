from lltk.imports import *
from lltk.model.networks import *

BAD_PROPVALS = {'','Q0',None,np.nan}
BAD_PROPS={'query','wd_author_match','wd_title_match','corpus','ocr_accuracy','dob'}
OK_PROPS = None

def get_node_type(node): return node.split('=',1)[0]


class TextCorpusGraph(BaseText): pass

class CorpusGraph(BaseCorpus):
    ID='corpus_graph'
    NAME='CorpusGraph'
    TEXT_CLASS = TextCorpusGraph
    def __init__(self,corpora=[],*args,**kwargs):
        super().__init__(*args,**kwargs)
        self._corpusd={}
        if corpora: self.add_corpus(corpora)

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
        for t in corpusobj.texts(): self.text(t)

    def corpora(self): return list(self._corpusd.values())
    def metadata(self,fillna='',**kwargs):
        o=[c.metadata(**kwargs) for c in self.corpora()]
        return pd.concat(o).fillna(fillna) if o else pd.DataFrame()

    def graph(self,
            node_types={},
            texts=None,
            force=True,
            ok_props=OK_PROPS,
            bad_props=BAD_PROPS,
            bad_propvals=BAD_PROPVALS,
            incl_text_nodes=True,
            col_id=COL_ADDR,
            min_degree=None,
            min_weight=None,
            remove_isolates=True,
            **kwargs):
        if force or self._g is None:
            self._g = g = nx.Graph()
            for t in self.texts(texts):
                tmeta = t.metadata(wikidata=False)
                tnode = t.meta.get(self.col_addr)

                #if t.source is not None and t.source.id:
                #    tid_src=f'{col_id}={tid}'
                #    tmeta['_id_source']=t.source.id
                
                if not g.has_node(tnode):
                    g.add_node(tnode, **tmeta)
                
                for propname,propval in tmeta.items():
                    if propname == self.col_id: continue
                    if propname in bad_props: continue
                    try:
                        if propval in bad_propvals: continue
                    except TypeError:
                        pass
                    if ok_props and propname not in ok_props: continue
                    
                    propnode=f'{propname}={propval}'
                    if not g.has_node(propnode):
                        g.add_node(propnode)
                    
                    g.add_edge(tnode, propnode)
        

        g = self._g if self._g is not None else nx.Graph()
        if type(node_types) == str: node_types=[node_types]
        if node_types:
            node_types = set(node_types)
            if incl_text_nodes: node_types|={col_id}
            for node in list(g.nodes()):
                if get_node_type(node) not in node_types:
                    g.remove_node(node)
        g = filter_graph(g,min_degree=min_degree,min_weight=min_weight,remove_isolates=remove_isolates,**kwargs)
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

    def neighbors(self,node,g=None,**kwargs):
        if g is None: g=self.graph(**kwargs)
        return g.neighbors(node)
    
    def neighbors_graph(self,node,remove_seed=True,**kwargs):
        neighbs = self.neighbors(node,**kwargs)
        gsub = nx.Graph()
        for neighb in neighbs:
            for neighbs_neighb in self.neighbors(neighb):
                gsub.add_edge(neighb,neighbs_neighb)
        if remove_seed and gsub.has_node(node): gsub.remove_node(node)
        return filter_graph(gsub,**kwargs)
    
    
    def neighbors_df(self,node,fillna='',**kwargs):
        neighbs_graph = self.neighbors_graph(node,**kwargs)
        odf=pd.DataFrame([
            self.text(idx.split('=',1)[-1]).meta
            for idx in neighbs_graph.nodes()
            if idx.startswith(f'{self.id}=')
        ]).fillna(fillna)
        if self.col_id in set(odf.columns): odf=odf.set_index(self.col_id)
        odf = odf.loc[odf.index.drop_duplicates()]
        return odf
        
    

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
        

