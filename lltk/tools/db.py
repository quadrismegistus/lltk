from lltk.imports import *
DBSD={}
DEFAULT_DB='texts'
DEFAULT_FN='db.shelf'


class LLDB():
    ext='.db'
    table='data'

    def __init__(self,fn,**kwargs):
        from lltk.tools import ensure_dir_exists
        if type(fn)!=str or not fn: raise Exception('No fn for db given')
        ensure_dir_exists(fn,fn=True)
        self._fn=fn
        self._db=None

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db is not None: 
            self._db.close()
            self._db = None
    
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): return self.set(key,value)
    def __delitem__(self, key): return self.delete(key)
    def __iter__(self): return self.iteritems()
    def __len__(self): return self.length

    @property
    def path(self): return os.path.splitext(self._fn)[0]+self.ext
    @property
    def fn(self): return self.path

    @property
    def db(self):
        with self as db: return db

    def get(self,k,default=None,**kwargs):
        with self as db:
            v=db.get(k)
            if v is not None:
                # log.debug(f'Getting: "{k}"')# -> {v}')
                return v
        return default
    
    def set(self,k,v,**kwargs):
        with self as db: 
            if db.get(k) != v:
                db[k]=v
                return True

    def delete(self,k,**kwargs):
        with self as db:
            if k in db:
                del db[k]
                return True

    def drop(self):
        from lltk.tools import rmfn
        rmfn(self.path)

    @property
    def length(self):
        with self as db: return len(db)
    def iterkeys(self): 
        with self as db: return iter(db)
    def iteritems(self): 
        with self as db: return iter(db.items())


class LLDBShelve(LLDB):
    ext='.shelf'
    
    def __enter__(self):
        import shelve
        self._db=shelve.open(self.path)
        return self._db
    
class LLDBSqlite(LLDB):
    ext='.sqlite'

    def __enter__(self):
        from sqlitedict import SqliteDict
        self._db=SqliteDict(
            self.path,
            tablename=self.table,
            autocommit=True
        )
        return self._db



def DB(name=DEFAULT_DB,path=None,fn=None,force=False,engine='sqlite',**kwargs):
    if force or DBSD.get(name) is None:
        if not path:
            from lltk import PATH_LLTK_DB
            path = PATH_LLTK_DB
        if not fn: fn=os.path.join(path, name)

        if engine=='sqlite':
            db=LLDBSqlite(fn, **kwargs)
        else:
            db=LLDBShelve(fn, **kwargs)
        DBSD[name]=db
    return DBSD[name]




















def picklify(obj):
    # return obj
    from lltk.text.utils import is_text_obj,is_corpus_obj
    if is_text_obj(obj): return obj.addr
    if is_corpus_obj(obj): return obj.id
    if type(obj)==set: return {picklify(x) for x in obj}
    if type(obj)==list: return [picklify(x) for x in obj]
    if type(obj)==tuple: return set([picklify(x) for x in obj])
    if type(obj)==dict: return {picklify(k):picklify(v) for k,v in obj.items()}
    return obj

def unpicklify(obj,objtype=None):
    # return obj
    from lltk.text.utils import id_is_addr
    from lltk.text.text import Text
    from lltk.corpus.corpus import Corpus

    if type(obj)==set: return {unpicklify(x) for x in obj}
    if type(obj)==list: return [unpicklify(x) for x in obj]
    if type(obj)==tuple: return set([unpicklify(x) for x in obj])
    if type(obj)==dict: 
        return {
            unpicklify(k):unpicklify(v,objtype='Corpus' if k in {'_corpus','_section_corpus'} else '')
            for k,v in obj.items()
        }
    if objtype=='Corpus' and type(obj)==str: return Corpus(obj)
    if type(obj)==str and id_is_addr(obj): return Text(obj)
    return obj






