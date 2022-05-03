from lltk.imports import *
from lltk.tools.tools import safeget,ensure_dir_exists,safejson
import orjson

DBSD={}
DEFAULT_DB='texts'
DEFAULT_FN='db.shelf'


class LLDBBase():    
    ext='.db'
    table='data'

    def __init__(self,fn,**kwargs):
        if type(fn)!=str or not fn: raise Exception('No fn for db given')
        ensure_dir_exists(fn,fn=True)
        self._fn=fn
        self._db=None

    def __enter__(self): return self.open()
    # def __exit__(self,*x): return self.close()
    def __exit__(self,*x): return
    
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): return self.set(key,value)
    def __delitem__(self, key): return self.delete(key)
    def __iter__(self): return self.iteritems()
    def __len__(self): return self.length
    
    
    def keys(self):
        with self as db: return list(db.keys())
    def values(self):
        with self as db: return list(db.values())

    @property
    def path(self): return os.path.splitext(self._fn)[0]+self.ext
    @property
    def fn(self): return self.path

    @property
    def db(self): return self._db

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



class LLDBShelve(LLDBBase):
    ext='.shelf'
    

# class LLDBSqlite1(LLDBBase):
#     ext='.sqlite'

#     def __init__(self,fn,**kwargs):
#         super().__init__(fn,**kwargs)

#         from sqlitedict import SqliteDict
#         self._db=SqliteDict(
#             self.path,
#             tablename=self.table,
#             autocommit=True
#         )
#         # return self._db
    
#     def __enter__(self): return self._db
#     def __exit__(self,*x): pass

class LLDBSqlite(LLDBBase):
    ext='.sqlite'
    def open(self):
        if self._db is None:
            from sqlitedict import SqliteDict
            self._db=SqliteDict(self.path,tablename=self.table,autocommit=True)
        return self._db
    


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

    def get(self,id,default={},**kwargs):
        res=self.get_node(id,**kwargs)
        return res if res is not None else default

    def add_node(self,id,**meta):
        return self.db.atomic(
            self.path,
            self.db.upsert_node(id, safejson(meta))
        )
    
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
    
    def get_nodes(self,id=None,**meta):
        return self.db.atomic(
            self.path,
            self.db.find_nodes(id if id else safejson(meta))
        )
    
    def get_neighbs(self,id,data=False,rel=None,**kwargs):
        if not data:
            return self.db.traverse(self.path, id, **safejson(kwargs))
        else:
            return self.get_rels(id,rel=rel,**kwargs)

    def get_rels(self,id,rel=None,**kwargs):
        res=self.db.traverse_with_bodies(self.path, id, **safejson(kwargs))
        o=[]
        if type(res)==list and res:
            u = id
            for v,datatype,dstr in res:
                if u != v and datatype!='()':
                    d=orjson.loads(dstr)
                    relx=d.get('rel')
                    if not rel or rel==relx: o.append((v,relx))
        return o

    def iter_edges(self,id,**kwargs):
        
        res=self.db.atomic(
            self.path,
            self.db.get_connections(id)
        )
        for resx in res:
            if resx and len(resx)==3:
                u,v,dstr=resx
                d=orjson.loads(dstr)
                yield u,v,d
    
    def get_edges(self,id,**kwargs):
        return list(self.iter_edges(id,**kwargs))




def LLDB(path,engine='rdict',**kwargs):
    classname=f'LLDB{engine.title()}'
    func = globals().get(classname)
    if func is not None: return func(path,**kwargs)
    

def DB(
        path:str=None,
        force:bool=False,
        engine:str='rdict',
        **kwargs) -> LLDBBase:
    """Get a sqlitedict or shelve persistent dictionary at file `path`.

    Parameters
    ----------
    path : str, optional
        A path without a file extension (which will be supplied depending on database engine, by default `PATH_LLTK_DB_FN`
    
    engine : str, optional
        Options are: 'sqlite' for sqlitedict; 'rdict' for rdict; otherwise shelve. Default: 'rdict'.

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