from lltk.imports import *
import ZODB, transaction
from lltk.tools import ensure_dir_exists
from ZEO.ClientStorage import ClientStorage
from ZODB import DB
from ZODB.FileStorage import FileStorage



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


class BaseObject(object):
    def __getstate__(self,bad_pkl_keys=BAD_PKL_KEYS,bad_pref=''):
        # self.log(f'Bad pickle keys: {bad_pkl_keys}')
        od={
            k:v
            for k,v in self.__dict__.items()
            if k not in set(bad_pkl_keys)
            and (not bad_pref or not k.startswith(bad_pref))
        }
        return picklify(od)
    
    def __setstate__(self, d):
        # if broken_key in d: d=d[broken_key]
        d=unpicklify(d)
        self.__dict__ = d
        # self.__init__(**d.get('_meta'))

    @property
    def _dbid(self): return self.addr

    def zsave(self): get_zodb().set(self._idz, self)

    def log(self,*x,**y):
        from pprint import pformat
        o=' '.join(pformat(_x,indent=2) if type(_x)!=str else _x for _x in x)
        o=f'{self}: {o if o not in {None,""} else ""}'
        log.debug(o)


class ZDB(object):
    ### CLASS ATTRS
    conn = None
    root = None
    key_broken = '__Broken_state__'
    
    ### ATTR METHODS
    def __init__(self,path=None,open=True,**kwargs):
        if path: self.path = path
        ensure_dir_exists(self.path,fn=True)
        self.db = ZODB.DB(self.path)
        # self.conn = self.db.open()
        # self.root = self.conn.root()

    ## Convenience methods
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): self.set(key,value)

    # def __iter__(self): 
    #     with self.db.transaction() as conn: return iter(conn.root().items())
    # def __len__(self):
    #     with self.db.transaction() as conn: return len(conn.root())
    
    
    def get(self,k): 
        with self.db.transaction() as conn: return conn.root().get(k)
        # with ZConnection(self.db) as (conn,root): return root.get(k)
        # return self.root.get(k)

    def set(self,key,value):
        if key is None or value is None: return
        key=str(key)
        with self.db.transaction() as conn:
            root=conn.root()
            root[key]=value
            log.debug(f'DB["{key}"] = {value}')
        # with ZConnection(self.db) as (conn,root):
            oldvalue = root.get(key)
            #if value != oldvalue:
            # transaction.commit()
        
        
    @property
    def path_lock(self): return self.path+'.lock'
    @property
    def path(self):
        from lltk.imports import PATH_LLTK_ZODB
        return PATH_LLTK_ZODB




ZODB_OBJ=None
def get_zodb(force=False,**kwargs):
    global ZODB_OBJ
    if force or ZODB_OBJ is None:
        if ZODB_OBJ is not None: ZODB_OBJ.close()
        ZODB_OBJ = ZDB(**kwargs)
        # list(ZODB_OBJ.root.items())
        # list(ZODB_OBJ.root.items())
        # ZODB_OBJ.root
    return ZODB_OBJ





class ZDatabase():
    """ Provides a ZODB database context manager """

    def __init__(self, uri, **kwargs):
        self.storage = create_storage(uri)
        self.db = DB(self.storage, **kwargs)

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_value, traceback):
        self.db.close()
        return False


class ZConnection():
    """ Provides a ZODB connection with auto-abort (default).
    Provides a tuple of connection and root object:
        with ZConnection(db) as (cx, root):
            root.one = "ok"
    ZConnection implements a connection context manager.
    Transaction context managers in contrast do auto-commit:
        a) with db.transaction() as connection, or
        b) with cx.transaction_manager as transaction, or
        c) with transaction.manager as transaction  (for the thread-local transaction manager)
    See also http://www.zodb.org/en/latest/guide/transactions-and-threading.html
    """
    def __init__(self, db, auto_commit=False, transaction_manager=None):
        self.db = db
        self.auto_commit = auto_commit
        self.transaction_manager = transaction_manager
        self.cx = None

    def __enter__(self):
        if self.transaction_manager:
            self.cx = self.db.open(self.transaction_manager)
        else:
            self.cx = self.db.open()
        return self.cx, self.cx.root()

    def __exit__(self, exc_type, exc_value, traceback):
        if self.auto_commit:
            self.cx.transaction_manager.commit()
        self.cx.close()
        return False


def create_storage(uri):
    """ supported URIs
    file://e:/workspaces/zeo/bots.fs
    zeo://localhost:8001
    e:/workspaces/zeo/bots.fs
    @see https://en.wikipedia.org/wiki/Uniform_Resource_Identifier
    """
    if uri.startswith("file://"):
        storage = FileStorage(uri[7:])
    elif uri.startswith("zeo://"):
        addr, port = uri[6:].split(":")
        # addr_ = addr.encode("ASCII")
        storage = ClientStorage((addr, int(port)))
    else:
        storage = FileStorage(uri)
    return storage


def database(uri):
    """ convenience function for single thread, return one connection from the pool """
    storage = create_storage(uri)
    db = DB(storage)
    return db


def connection(db):
    """ Convenience function for multi thread, returns
    connection, transaction manager and root
    """
    cx = db.open()
    return cx, cx.root()