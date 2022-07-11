from lltk.imports import *


class LLDBGraph(LLDBBase):
    ext='.graphdb'
    
    def __init__(self,*x,**y):
        super().__init__(*x,**y)
        self.open()

    def open(self):
        if self._db is None:
            from simple_graph_sqlite import database as gdb
            gdb.initialize(self.path)
            self._db=gdb
        return self._db

    def close(self,*x,**y): return

    def set(self,id,meta,**kwargs):
        ikwargs={**meta, **kwargs, **{'id':id}}
        return self.add_node(**ikwargs)

    def get(self,id=None,default={},**kwargs):
        if id is not None: res=self.get_node(id,**kwargs)
        else: res=self.where(**kwargs)
        return res if res is not None else default

    def add_node(self,id,**meta):
        return self.db.atomic(
            self.path,
            self.db.upsert_node(id, safejson(meta))
        )
    
    def add_nodes(self,ids_metas,col_id='id'):
        ids,metas=zip(*ids_metas)
        metas=[safejson(d) for d in metas]
        return self.db.atomic(
            self.path,
            self.db.upsert_nodes(metas, ids)
        )
    
    def insert(self,*lt): return self.add_nodes(lt)

    
    def add_edge(self,source,target,**meta):
        return self.db.atomic(
            self.path,
            self.db.connect_nodes(source, target, safejson(meta))
        )
    
    def get_node(self,id=None,**meta):
        return self.db.atomic(
            self.path,
            self.db.find_node(id if id else safejson(meta))
        )
    
    def where(self,id=None,**meta):
        if id: return [self.get_node(id)]
        
        inp=safejson(meta)
        return self.db.atomic(
            self.path,
            self.db.find_nodes(inp)
        )
    get_nodes = where
    
    def search(self,**meta):
        return self.db.atomic(
            self.path,
            self.db.find_nodes(safejson(meta), self.db._search_like, self.db._search_starts_with)
        )
    find_nodes = search
    
    def get_neighbs(self,id,data=False,rel=None,direct=True,**kwargs):
        if self.log>1: self.log(id)
        if not direct:
            if not data:
                return self.db.traverse(self.path, id, **safejson(kwargs))
            else:
                return self.get_rels(id,rel=rel,**kwargs)
        else:
            if not data:
                return self.get_linked_to(id,rel=rel,**kwargs)
            else:
                return self.get_links(id,rel=rel,**kwargs)

    def get_rels(self,id,rel=None,**kwargs):
        if self.log>1: self.log(id)
        res=self.db.traverse_with_bodies(self.path, id, **safejson(kwargs))
        o=[]
        if type(res)==list and res:
            u = id
            for v,datatype,dstr in res:
                if u != v and datatype!='()':
                    d=orjson.loads(dstr)
                    relx=d.get('rel')
                    if not rel or rel==relx:
                        o.append((v,relx))
        return o

    def iter_edges(self,id,**kwargs):
        if self.log>1: self.log(id)
        try:
            res=self.db.atomic(
                self.path,
                self.db.get_connections(id)
            )
            for resx in res:
                if resx and len(resx)==3:
                    u,v,dstr=resx
                    d=orjson.loads(dstr)
                    yield u,v,d
        except AssertionError as e:
            self.log.error(e)
            self.log.error([id,kwargs])
        
    
    def get_edges(self,id,**kwargs):
        if self.log>1: self.log(id)
        return list(self.iter_edges(id,**kwargs))

    def get_links(self,id,rel='rdf:type',**kwargs):
        if self.log>1: self.log(id)
        from lltk.tools.tools import OrderedSetDict
        odset=OrderedSetDict()
        for u,v,d in self.get_edges(id,**kwargs):
            # self.log([u,v,d])
            if not rel or d.get('rel')==rel:
                n = (u if u!=id else v)
                odset[n]=set(d.items())
        return {k:dict(v) for k,v in odset.to_dict().items() if k!=id}

    def get_linked_to(self,id,rel=None,**kwargs):
        if self.log>1: self.log(id)
        return set(self.get_links(id,rel=rel,**kwargs).keys()) - {id}

    # def get_matches(self,id,rel='rdf:type',min_overlap=2,bad_corpora={},**kwargs):
    #     # self.log(f'{id}')
    #     o=set()
    #     rels={k for k,v in self.get_rels(id) if not rel or rel==v}
    #     dsrcs=set(self.get_neighbs(id,direct=True))
    #     o|=dsrcs
    #     for src in (rels - dsrcs):
    #         if src == id: continue
    #         src_dsrcs=set(self.get_neighbs(src,direct=True))
    #         overlap = dsrcs & src_dsrcs
    #         # self.log(f'overlap between {t} and {src} = {overlap}')
    #         if len(overlap)>=min_overlap:
    #             o|={src}
    #     return o
    def get_matches(self,id,rel='rdf:type',min_overlap=2,depth=1,max_depth=2,goneround=None,**kwargs):
        from collections import Counter
        # self.log(f'{id}')
        o=set()
        rels={k for k,v in self.get_rels(id) if not rel or rel==v}
        dsrcs=set(self.get_neighbs(id,direct=True))
        o|=dsrcs
        goneround=set() if goneround==None else set(goneround)
        for src in (rels - dsrcs):
            if src == id: continue
            src_dsrcs=set(self.get_neighbs(src,direct=True))
            overlap = dsrcs & src_dsrcs
            # self.log(f'overlap between {t} and {src} = {overlap}')
            if len(overlap)>=min_overlap:
                o|={src}
                # if depth<max_depth:
                #     o|=self.get_matches(src,rel=rel,min_overlap=min_overlap,depth=depth+1,max_depth=max_depth,**kwargs)

        if depth<max_depth:
            if self.log>1: self.log('going around again')
            todo = set(o) - set(goneround)
            goneround|=set(o)
            for addr in todo:
                if self.log>1: self.log(f'going: {addr}')
                o|=self.get_matches(addr,rel=rel,min_overlap=min_overlap,depth=depth+1,max_depth=max_depth,goneround=goneround,**kwargs)
                
        # if depth<max_depth:
        #     if self.log>1: self.log('going around again')
        #     todo=[addr for addr in o if goneround[addr]<max_depth]
        #     for addr in o: goneround[addr]+=1
        #     if self.log>1: self.log(f'goneround: {goneround}')
        #     for addr in todo:
        #         if self.log>1: self.log(f'going: {addr}')
        #         o|=self.get_matches(addr,rel=rel,min_overlap=min_overlap,depth=depth+1,max_depth=max_depth,goneround=goneround,**kwargs)

        return o





