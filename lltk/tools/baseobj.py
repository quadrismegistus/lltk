from lltk.tools.tools import ensure_dir_exists, get_tqdm, just_metadata

COL_ID='id'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9)


class BaseObject(object):
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
    def rels(self): return {}
