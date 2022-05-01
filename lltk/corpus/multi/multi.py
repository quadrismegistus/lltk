# from re import L
from lltk.imports import *
from lltk.model.networks import *

BAD_PROPVALS = {'',NULL_QID,None,np.nan,'nan'}
BAD_PROPS={'query','wd_author_match','wd_title_match','corpus','ocr_accuracy','dob'}
OK_PROPS = None
BAD_NODES= {'Vnull',NULL_QID,'P0','V0','X0'}



def get_node_type(node): return node.split('=',1)[0]


class MultiText(BaseText):
    
    def wikidata(self,*x,**y):
        for t in self.get_sources(*x,**y):
            if t.corpus.id=='wikidata':
                return t

    def metadata(self,*args,**kwargs):
        od=MaybeListDict(
            (k,v)
            for st in self.sources
            for k,v in st.metadata(*args,**kwargs).items()
        )
        return self.ensure_id_addr(od)
    
    # def iter_metadata(self,from_sources=True,*args,**kwargs):
    #     for t in self.get_sources(*args,**kwargs):
    #         tmeta=t.metadata(from_sources=False,from_cache=True,*args,**kwargs)
    #         for k,v in yield_addrs(tmeta):
    #             yield t.addr,k,v
    #         for k,v in tmeta.items():
    #             if k and k[0]!='_':
    #                 ks,vs=f'{t.corpus.id}:{k}',str(v)
    #                 if vs and vs[0].isdigit():
    #                     try:
    #                         vs=int(float(vs))
    #                     except Exception as e:
    #                         log.error(e)
    #                         pass
    #                 if vs not in BAD_PROPVALS:
    #                     yield (t.addr,ks,vs)
    
    def get_numsrc(self,*args,**kwargs):
        return len(list(self.get_sources(*args,**kwargs)))
    def get_srcs(self,*args,**kwargs):
        return '; '.join([src.addr for src in self.get_sources(*args,**kwargs)])
    

    # def metadata(self,*args,**kwargs):
    #     o=[(k,v) for a,k,v in self.iter_metadata(*args,**kwargs)]
    #     o.append(('_addr_'+self.corpus.id,self.addr))
    #     o.append(('_id',self.id))
    #     o.append(('_corpus',self.corpus.id))
    #     o.append(('_numsrc',self.get_numsrc(*args,**kwargs)))
    #     o.append(('_srcs',self.get_srcs(*args,**kwargs)))

    #     return dict(sorted(o))


class MultiCorpus(BaseCorpus):
    ID='multi'
    NAME='Multi'
    TEXT_CLASS = MultiText

    def __init__(self,corpora=[],id=None,*args,**kwargs):
        super().__init__(id = id if id else self.ID, *args,**kwargs)
        self._corpusd={}
        if corpora: self.add_corpus(corpora)
    
    def corpus_texts(self,*x,**y):
        for corpus in self.corpora():
            yield from corpus.texts(*x,**y)

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
        o=[t.metadata(**kwargs) for t in self.texts(**kwargs)]
        return pd.DataFrame(o).fillna(fillna) if o else pd.DataFrame()

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

                    if log>0: log(f'Adding edge: {u} --({rel})--> {v}')
                    g.add_edge(u, v, rel = rel)

        g = filter_graph(g=g,min_degree=min_degree,min_weight=min_weight,remove_isolates=remove_isolates,**kwargs)
        return g




    def graph_is(
            self,
            g=None,
            progress=False,
            force=False,
            wikidata=True,
            from_sources=True,
            ok_corps={'wikidata'},
            **kwargs):
        gfn=os.path.join(self.path_data,'graph_is.gexf')
        if not force and os.path.exists(gfn):
            return nx.read_gexf(gfn)

        # g = nx.MultiGraph()
        g = nx.Graph()
        for i,text in enumerate(self.corpus_texts(progress=progress)):
            meta = text.metadata(wikidata=wikidata,from_sources=from_sources)
            if not g.has_node(text.addr): g.add_node(text.addr)
            if text.is_valid():
                for rel,addr in yield_addrs(meta, ok_corps=ok_corps):
                    # log(f'?: {text.addr} {addr} ({rel})')
                    if addr!=text.addr and Text(addr).id_is_valid():
                        # log(f'>> {text.addr} {addr} ({rel})')
                        g.add_edge(text.addr, addr)
        ensure_dir_exists(gfn)
        nx.write_gexf(g, gfn)
        return g

    def init(self):
        if not self._init:
            self._init=True
            list(self.texts())

    def texts(self,force=False,progress=False,lim=None,**kwargs):
        if not self._init: self.init()
        _i=0
        if force or not self._textd:
            g_is = self.graph_is(progress=False,force=force,**kwargs)
            ol=list(nx.connected_components(g_is))
            ol.sort(key=lambda olx: -len(olx))
            if progress: ol=get_tqdm(ol)
            for text_addrs in ol:
                texts = [Text(taddr) for taddr in sorted(list(text_addrs))]
                if texts:
                    id = texts[0].id
                    text = self.text(id=id, _sources=texts)
                    self._textd[id]=text
                    if not lim or _i<lim:
                        yield text
                        _i+=1

        else:
            ol=list(self._textd.values())
            if progress: ol=get_tqdm(ol)
            for text in ol:
                if not lim or _i<lim:
                    yield text
                    _i+=1



    