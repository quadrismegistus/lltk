from re import S
from lltk.imports import *
import random
from lltk.tools import ensure_dir_exists,getattribute,get_tqdm,no_id,just_metadata
from lltk.tools.logs import *
from lltk.tools.db import log_on,log_off,is_logged_on,LLDB_ENGINE
COL_ID='id'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9)





class BaseObject(object):
    def __init__(self,**kwargs):
        for k,v in kwargs.items(): setattr(self,k,v)

    def __bool__(self): return True
    def __nonzero__(self): return True
    def __getattr__(self,key): return getattribute(self,key)

    def __lt__(self, other): return self.addr < other.addr
    def __gt__(self, other): return self.addr > other.addr
    def __le__(self, other): return self.addr <= other.addr
    def __ge__(self, other): return self.addr >= other.addr
    def __eq__(self, other): return self.addr == other.addr
    def __ne__(self, other): return self.addr != other.addr

    #__gt__, __ge__, __le__, __eq__, __ne__ all implemented the same way

    # def __getstate__(self,bad_pkl_keys=BAD_PKL_KEYS,bad_pref=''):
    #     # self.log(f'Bad pickle keys: {bad_pkl_keys}')
    #     od={
    #         k:v
    #         for k,v in self.__dict__.items()
    #         if k not in set(bad_pkl_keys)
    #         and (not bad_pref or not k.startswith(bad_pref))
    #     }
    #     o=picklify(od)
    #     self.log(o)
    #     return o
    
    # def __setstate__(self, d):
    #     d=unpicklify(d)
    #     self.__dict__ = d

    @property
    def userkey(self):
        from lltk.tools.tools import get_user_email,get_passkey
        email = get_user_email('For encryption purposes, what is your email? ')
        if email: return get_passkey(email)

    @property
    def _dbid(self): return self.addr

    def cache(self,*x,**y): 
        from lltk import log
        log.error('Not implemented at top level')

    # def log(self,*x,**y):
    #     from pprint import pformat
    #     o=' '.join(pformat(_x,indent=2) if type(_x)!=str else _x for _x in x)
    #     # o=f'{self.addr}: {o if o not in {None,""} else ""}'
    #     # o=f'{o if o not in {None,""} else ""}'
    #     if log>0: log(o)

    @property
    def gdb(self):
        if self._gdb is None: 
            from lltk.tools.db import DB
            self._gdb=DB(engine='graph')
        return self._gdb

    def same_as(self): return self.rels('rdf:type')
    
    def get_rels(self,rel=None): return self.gdb.get_rels(self.addr,rel=rel)
    @property
    def rels(self): return self.get_rels()

    @property
    def dbmeta(self): return self.gdb.get(self.addr,{})
    
    def cachedb(self,name,engine=LLDB_ENGINE):
        from lltk.imports import PATH_LLTK_DB
        from lltk.tools.db import DB
        from lltk.tools import ensure_snake

        db=DB(
            os.path.join(
                PATH_LLTK_DB,
                ensure_snake(name.strip())
            ),
            engine=engine
        )
        # if log: log(f'-> {db} of {db.path}')
        return db

    # def db(self,**kwargs):
    #     from lltk.model.orm import CDB
    #     return CDB(**kwargs)

    @property
    def cdb(self):
        if self._cdb is None:
            from lltk.model.orm import CDB
            self._cdb = CDB()
        return self._cdb
    mdb = cdb


    @property
    def doctype(self):
        from lltk.model.orm import TextDoc
        return TextDoc
    @property
    def doctype_rels(self):
        from lltk.model.orm import TextRels
        return TextRels
    @property
    def doctype_minhash(self):
        from lltk.model.orm import MinHashDoc
        return MinHashDoc
