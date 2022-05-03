from lltk.imports import *
from lltk.tools import ensure_dir_exists
from lltk.tools.logs import *


class BaseObject(object):
    def __bool__(self): return True
    def __nonzero__(self): return True

    def __getstate__(self,bad_pkl_keys=BAD_PKL_KEYS,bad_pref=''):
        # self.log(f'Bad pickle keys: {bad_pkl_keys}')
        od={
            k:v
            for k,v in self.__dict__.items()
            if k not in set(bad_pkl_keys)
            and (not bad_pref or not k.startswith(bad_pref))
        }
        o=picklify(od)
        self.log(o)
        return o
    
    def __setstate__(self, d):
        d=unpicklify(d)
        self.__dict__ = d

    @property
    def _dbid(self): return self.addr

    def cache(self,*x,**y): log.error('Not implemented at top level')

    # def log(self,*x,**y):
    #     from pprint import pformat
    #     o=' '.join(pformat(_x,indent=2) if type(_x)!=str else _x for _x in x)
    #     # o=f'{self}: {o if o not in {None,""} else ""}'
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
