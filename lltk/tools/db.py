from lltk.imports import *
from lltk.tools.tools import safeget,ensure_dir_exists,safejson,is_iterable
import orjson

COL_ID='id'
DBSD={}
DEFAULT_DB='texts'
DEFAULT_FN='db.shelf'
LLDB_ENGINE='tinydb'
IS_REMOTE=REMOTE_DEFAULT

def log_on(): os.environ['LLTK_LOGGED_ON']='True'
def log_off(): os.environ['LLTK_LOGGED_ON']='False'
def is_logged_on(): return os.environ.get('LLTK_LOGGED_ON')=='True'


class online_access():
    def __init__(self,verbose=1,log=None):
        self.was_logged_on = is_logged_on()
    def __enter__(self): 
        log_on()
    def __exit__(self,*x):
        if not self.was_logged_on: log_off()

class offline_access():
    def __init__(self,verbose=1,log=None):
        self.was_logged_on = is_logged_on()
    def __enter__(self): 
        log_off()
    def __exit__(self,*x):
        if self.was_logged_on: log_on()


















class LLDBBase():    
    ext='.db'
    table='data'

    def __init__(self,fn,**kwargs):
        if type(fn)!=str or not fn: raise Exception('No fn for db given')
        ensure_dir_exists(fn,fn=True)
        self._fn=fn
        self._db=None
        self._cachedb=None
        self._conn=None

    def __enter__(self): return self.open()
    def __exit__(self,*x): pass #return self.close()
    # def __exit__(self,*x): return
    
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): return self.set(key,value)
    def __delitem__(self, key): return self.delete(key)
    def __iter__(self): return self.iteritems()
    def __len__(self): return self.length


    @property
    def cachedb(self):
        return {}
        # if not self._cachedb:
        #     self._cachedb=DB(self.path+'.cache', engine='sqlite')
        #     self.log(self._cachedb.path)
        # return self._cachedb
    
    def keys(self):
        with self as db: return list(db.keys())
    def values(self):
        with self as db: return list(db.values())

    def query(self,url,force=False,**kwargs):
        isremote = is_logged_on()

        if self.log>0: self.log(f'? {url}')
        key=f'_query_{url}'
        res = self.get(key) if not force else None
        if not res:
            if not isremote:
                res=''
            else:
                try:
                    from lltk.tools.tools import gethtml
                    res = gethtml(url,**kwargs)
                    if self.log>1: self.log(f'setting {key}')
                    self.set(key,res)
                except AssertionError as e:
                    self.log.error(e)
                    log_off()
                    self.log.error(f'Logging off ... [is_logged_on = {is_logged_on()}]')
                    res = ''
                    
        # self.log(f'-> {len(res) if is_iterable(res) else "..."}')
        if self.log>0: self.log(f'-> {type(res)} ({len(res) if is_iterable(res) else "..."})')
        return res if res else ''

    @property
    def path(self): return os.path.splitext(self._fn)[0]+self.ext
    @property
    def fn(self): return self.path

    @property
    def db(self): 
        if self._db is None: self.open()
        return self._db

    @property
    def log(self):
        if not hasattr(self,'_log'):
            from lltk import log
            self._log=log
        return self._log

    def open(self):
        import shelve
        if self._db is None:
            self._db=shelve.open(self.path)
        return self._db

    def close(self):
        if self._db is not None and hasattr(self._db,'close'):
            self._db.close()
        self._db=None

    def get(self,k,default=None,**kwargs):
        with self as db:
            v=safeget(db,k)
            if v is not None:
                # log(f'Getting: "{k}"')# -> {v}')
                return v
        return default
    
    def set(self,k,v,**kwargs):
        with self as db: 
            if safeget(db,k) != v:
                db[k]=v
                return True

    def delete(self,k,**kwargs):
        from lltk import log
        if log>0: log(f'removing {k} from db')
        with self as db:
            if k in db:
                del db[k]
                return True

    def drop(self):
        from lltk.tools import rmfn
        if os.path.exists(self.path):
            from lltk import log
            if log>0: log(f'removing: {self.path}')
            rmfn(self.path)

    def copy_from(self,db):
        return sum(
            int(bool(self.set(k,v)))
            for k,v in db
        )

    @property
    def length(self):
        with self as db: return len(list(self.iteritems()))
    def iterkeys(self): 
        with self as db: return iter(db)
    def iteritems(self): 
        with self as db: return iter(db.items())























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










































import tinydb
from tinydb import TinyDB,Query,where
from tinydb.table import Document
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Union,
    cast,
    Tuple
)


class TinyTable(tinydb.table.Table):
    document_id_class = str
    def _get_next_id(self):
        from lltk.text.utils import get_idx
        return get_idx()

    def insert_multiple(self, documents: Iterable[Mapping]) -> List[int]:
        """
        Insert multiple documents into the table.

        :param documents: an Iterable of documents to insert
        :returns: a list containing the inserted documents' IDs
        """
        doc_ids = []

        def updater(table: dict):
            from lltk.imports import log
            from lltk.text.utils import safebool

            for document in documents:

                # Make sure the document implements the ``Mapping`` interface
                if not isinstance(document, Mapping):
                    log.error('Document is not a Mapping')
                    continue

                if isinstance(document, Document):
                    # Check if document does not override an existing document
                    if document.doc_id in table:
                        old = table[document.doc_id]
                        new = {**safebool(old), **safebool(old)}
                        if old != new:
                            doc_id_l = self.upsert(document)
                            if not doc_id_l: 
                                log.error('no doc id returned on upsert')
                                continue
                            doc_id = doc_id_l[0]
                        else:
                            # if log: log('cache unchanged')
                            doc_id = document.doc_id
                    else:
                        # Store the doc_id, so we can return all document IDs
                        # later. Then save the document with its doc_id and
                        # skip the rest of the current loop
                        doc_id = document.doc_id
                    doc_ids.append(doc_id)
                    table[doc_id] = dict(document)
                    continue

                # Generate new document ID for this document
                # Store the doc_id, so we can return all document IDs
                # later, then save the document with the new doc_id
                doc_id = self._get_next_id()
                doc_ids.append(doc_id)
                table[doc_id] = dict(document)

        # See below for details on ``Table._update``
        self._update_table(updater)

        return doc_ids


TinyDB.table_class = TinyTable




class LLDBTinydb(LLDBBase):
    ext='.json'
    def open(self):
        if self._db is None:
            self._db=TinyDB(self.path)
            self._db.default_table_name = self.table
        return self._db
    
    def insert(self,*lt):
        with self as db: return db.insert_multiple([
            Document(safejson(d),doc_id=id)    
            for id,d in lt
        ])

    def set(self,id,val):
        assert type(val) == dict
        self.log(f'{id} = {val}')
        self.insert((id,val))
    
    def get(self,id=None,**meta):
        if id is not None:
            with self as db: return db.get(doc_id=id)
        else:
            return self.where(**meta)

    def to_conds(self,meta):
        conds = []
        for k,v in safejson(meta).items():
            k=str(k)
            v=str(v)
            for vx in v.split():
                conds.append((k,vx))
        return conds

    def get_cache_key(self,conds,search_type='search'):
        o=f'{search_type}___{sorted(conds)}'
        self.log(o)
        return o
    
    def get_cache(self,conds,search_type='search'):
        key = self.get_cache_key(conds=conds,search_type=search_type)
        return self.cachedb.get(key)
    
    def set_cache(self,conds,res,search_type='search'):
        key = self.get_cache_key(conds=conds,search_type=search_type)
        self.cachedb[key]=res
        

    def where(self,_force=False,**meta):
        conds = self.to_conds(meta)
        res = self.get_cache(conds=conds,search_type='where') if not _force else None
        if res: return res
        
        with self as db:
            if len(conds)==0:
                return db.all()
            elif len(conds)==1:
                res = db.search(where(conds[0][0])==conds[0][1])
            elif len(conds)==2:
                res = db.search((where(conds[0][0])==conds[0][1]) & (where(conds[1][0])==conds[1][1]))
            elif len(conds)==3:
                res = db.search((where(conds[0][0])==conds[0][1]) & (where(conds[1][0])==conds[1][1]) & (where(conds[2][0])==conds[2][1]))
            elif len(conds)==4:
                res = db.search((where(conds[0][0])==conds[0][1]) & (where(conds[1][0])==conds[1][1]) & (where(conds[2][0])==conds[2][1]) & (where(conds[3][0])==conds[3][1]))
            elif len(conds)==5:
                res = db.search((where(conds[0][0])==conds[0][1]) & (where(conds[1][0])==conds[1][1]) & (where(conds[2][0])==conds[2][1]) & (where(conds[3][0])==conds[3][1]) & (where(conds[4][0])==conds[4][1]))
            else:
                raise Exception('Limited to 5 conditions now')

        self.set_cache(conds=conds,res=res,search_type='where')
        return res


    def search(self,_force=False,**meta):
        self.log(f'meta = {meta}')

        conds = self.to_conds(meta)

        self.log(f'conds = {conds}')

        res = self.get_cache(conds=conds,search_type='search') if not _force else None
        self.log(f'cache res = {res}')
        if res: 
            if self.log: self.log('loading res from db')
            return res


        if self.log: self.log(conds)
        test_func = lambda value, search: search.lower() in value.lower()
        with self as db:
            if len(conds)==0:
                return db.all()
            elif len(conds)==1:
                res = db.search(where(conds[0][0]).test(test_func, conds[0][1]))
            elif len(conds)==2:
                res = db.search(where(conds[0][0]).test(test_func, conds[0][1]) &  where(conds[1][0]).test(test_func, conds[1][1]))
            elif len(conds)==3:
                res = db.search(where(conds[0][0]).test(test_func, conds[0][1]) & where(conds[1][0]).test(test_func, conds[1][1]) & where(conds[2][0]).test(test_func, conds[2][1]))
            elif len(conds)==4:
                res = db.search(where(conds[0][0]).test(test_func, conds[0][1]) & where(conds[1][0]).test(test_func, conds[1][1]) & where(conds[2][0]).test(test_func, conds[2][1]) & where(conds[3][0]).test(test_func, conds[3][1]))
            elif len(conds)==5:
                res = db.search(where(conds[0][0]).test(test_func, conds[0][1]) & where(conds[1][0]).test(test_func, conds[1][1]) & where(conds[2][0]).test(test_func, conds[2][1]) & where(conds[3][0]).test(test_func, conds[3][1]) & where(conds[4][0]).test(test_func, conds[4][1]))
            else:
                raise Exception(f'Limited to 5 conditions now but {len(conds)} conditions now: {conds}')
            
        if self.log: self.log('setting res in db')
        self.set_cache(conds=conds,res=res,search_type='search')
        return res
        


class LLDBMonty(LLDBBase):
    ext='.sqlite'

    def open(self):
        if self._db is None:
            from montydb import set_storage, MontyClient
            set_storage(
                self.path,
                storage="sqlite",     # storage name, default "flatfile"
                mongo_version="4.0",    # try matching behavior with this mongodb version
                use_bson=True,         # default None, and will import pymongo's bson if None or True
                # any other kwargs are storage engine settings.
                # cache_modified=10,       # the only setting that flat-file have
            )                                

            client = MontyClient(
                self.path,
                busy_timeout=5000,
                # synchronous=1
                # synchronous="FULL",
                # automatic_index=True
            )
            self._db=getattr(client.db,self.table)
        return self._db
    
    def set(self,id,val,col_id=COL_ID):
        assert type(val)==dict
        self.db.update_one({col_id:id}, {'$set':val}, upsert=True)
    
    def get(self,id=None,col_id=COL_ID,**meta):
        from lltk.tools.tools import safebool
        if id is not None: return self.db.find_one({col_id:id})
        return self.db.find(safebool(meta))
    where = get

    
    def insert(self,*lt,col_id=COL_ID,batch_size=10000):
        batch = []
        from lltk.tools.tools import get_tqdm
        for id,d in get_tqdm(lt,desc='[LLTK] syncing database'):
            odx={**d, **{col_id:id}}
            batch.append(odx)
            if len(batch)>=batch_size:
                self.log('inserting batch')
                self.db.insert_many(batch)
                self.log('done')
                batch = []
        if batch: self.db.insert_many(batch)



        
class LLDBUnqlite(LLDBBase):
    ext='.unqlite'
    def open(self):
        if self._db is None:
            from unqlite import UnQLite
            self._conn=UnQLite(self.path)
            self._db=self._conn.collection(self.table)
            self._db.create()
            self._conn.begin()
        return self._db

    def search(self,**meta):
        def isok(obj,meta):
            for k,v in meta.items():
                if not obj.get(k).startswith(v): return False
            return True
        return self.db.filter(lambda obj: isok(obj,meta))

    def get(self,id=None,**meta):
        if id: return self.db.filter(lambda obj: obj['id']==id)
        def isok(obj,meta):
            for k,v in meta.items():
                if obj.get(k) != v: return False
            return True
        return self.db.filter(lambda obj: isok(obj,meta))
    where = get

    def insert(self,*lt,col_id=COL_ID,batch_size=10000):
        batch = []
        from lltk.tools.tools import get_tqdm
        for id,d in get_tqdm(lt):
            odx={**d, **{col_id:id}}
            batch.append(odx)
            if len(batch)>=batch_size:
                self.log('inserting batch')
                last_id = self.db.store(batch)

                # first_id = (last_id+1) - len(batch)
                # for bd,__id in zip(batch, range(first_id,last_id+1)):
                #     self.db[bd['id']]=__id
                #     self.db[__id]=bd['id']

                self._conn.commit()

                self.log('done')
                batch = []
        if batch:
            self.db.store(batch)
            self._conn.commit()



class LLDBShelve(LLDBBase):
    ext='.shelf'
    

class LLDBSqlite(LLDBBase):
    ext='.sqlite'
    def open(self):
        if self._db is None:
            from sqlitedict import SqliteDict
            self._db=SqliteDict(self.path,tablename=self.table,autocommit=True)
        return self._db
    





# from blitzdb import Document as BDoc
# class BTextDoc(BDoc): pass
# class BCorpusDoc(BDoc): pass


# _TYPE='Text'
# DOCD={'Text':BTextDoc, 'Corpus':BCorpusDoc}
# DEFAULTDOC=BTextDoc

# class LLDBBlitz(LLDBBase):
#     ext='.blitz'
#     col_id='id'

#     def open(self):
#         if self._db is None:
#             from blitzdb import FileBackend
#             self._db = FileBackend(self.path)
#             self._db.create_index(TextDoc,'_corpus')
#             # self._db.create_index(TextDoc,'__id')
#             # self._db.create_index(TextDoc,'_id')
#             self._db.create_index(TextDoc,'id')
#         return self._db


#     def upsert(self,id,d={},_type=_TYPE,**meta):
#         obj = self.get(id)
#         ometa={**d, **meta, **{self.col_id:id}}
#         if obj:
#             for k,v in ometa.items(): setattr(obj,k,v)
#         else:
#             Doc = DOCD.get(_type)
#             obj = Doc(ometa)
#         self.db.save(obj)
#         self.db.commit()
#         return obj

#     set = upsert
    
#     def get(self,id=None,_type=_TYPE,**meta):
#         Doc=DOCD.get(_type)
#         if id:
#             qdx={self.col_id:id}
#             try:
#                 return self.db.get(Doc,qdx)
#             except Doc.DoesNotExist:
#                 return None
#             except Doc.MultipleDocumentsReturned:
#                 #more than one 'Charlie' in the database
#                 return self.db.filter(Doc,qdx)
        
#         return self.where(**meta)
    
#     def where(self,_type=_TYPE,**meta):
#         Doc = DOCD.get(_type)
#         return self.db.filter(Doc,meta)

#     def insert(self,*lt):
#         from lltk.tools.tools import get_tqdm
#         for id,d in get_tqdm(lt):
#             self.upsert(id,d)
    
    






class LLDBSos(LLDBBase):
    ext='.sos'
    def open(self):
        if self._db is None:
            import pysos
            self._db=pysos.Dict(self.path)
        return self._db

class LLDBRdict(LLDBBase):
    ext='.rdict'
    def open(self):
        if self._db is None:
            from rocksdict import Rdict
            self._db=Rdict(self.path)
        return self._db

class LLDBZodb(LLDBBase):
    ext='.zodb'
    
    def open(self):
        if self._db is None:
            import ZODB
            self._zdb=ZODB.DB(self.path)
            self._conn=self._zdb.open()
            self._db=self._conn.root()
        return self._db

    def add(self,obj):
        self._conn.add(obj)
        import transaction
        from ZODB.utils import z64
        transaction.commit()
        return (obj._p_changed, bool(obj._p_oid), obj._p_serial == z64)

    def set(self,*x,**y):
        res=super().set(*x,**y)
        import transaction
        transaction.commit()
        return res



def LLDB(path,engine=LLDB_ENGINE,**kwargs):
    classname=f'LLDB{engine.title()}'
    func = globals().get(classname)
    if func is not None: return func(path,**kwargs)
    

def DB(
        path:str=None,
        force:bool=False,
        engine:str=LLDB_ENGINE,
        **kwargs) -> LLDBBase:
    """Get a sqlitedict or shelve persistent dictionary at file `path`.

    Parameters
    ----------
    path : str, optional
        A path without a file extension (which will be supplied depending on database engine, by default `PATH_LLTK_DB_FN`
    
    engine : str, optional
        Options are: 'sqlite' for sqlitedict; 'rdict' for rdict; otherwise shelve. Default: 'sqlite'.

    force : bool, optional
        Force creation of database object?, by default False

    Returns
    -------
    LLDBBase
        A simple persistent dictionary database.
    """
    
    if not path: 
        from lltk import PATH_LLTK_DB_FN
        path=PATH_LLTK_DB_FN
    
    key=(path,engine)
    if force or DBSD.get(key) is None:
        DBSD[key]=LLDB(path,engine=engine,**kwargs)
    return DBSD[key]


def close_dbs():
    for key,dbobj in list(DBSD.items()):
        dbobj.close()
        del DBSD[key]




def GDB(): return DB(engine='graph')