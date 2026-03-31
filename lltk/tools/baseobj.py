from lltk.imports import *
from lltk.tools import ensure_dir_exists,get_tqdm,no_id,just_metadata
from lltk.tools.logs import *
from lltk.tools.db import log_on,log_off,is_logged_on,LLDB_ENGINE
COL_ID='id'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9)


class BaseObject(object):
    _gdb = None

    def __init__(self,**kwargs):
        for k,v in kwargs.items(): setattr(self,k,v)

    def __bool__(self): return True
    def __nonzero__(self): return True

    def __lt__(self, other): return self.addr < other.addr
    def __gt__(self, other): return self.addr > other.addr
    def __le__(self, other): return self.addr <= other.addr
    def __ge__(self, other): return self.addr >= other.addr
    def __eq__(self, other): return self.addr == other.addr
    def __ne__(self, other): return self.addr != other.addr

    @property
    def gdb(self):
        if self._gdb is None:
            from lltk.tools.db import DB
            self._gdb=DB(engine='graph')
        return self._gdb

    def get_rels(self,rel=None): return self.gdb.get_rels(self.addr,rel=rel)
    @property
    def rels(self): return self.get_rels()

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
        return db

