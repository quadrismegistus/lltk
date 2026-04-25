from lltk.imports import *
from lltk.tools import ensure_dir_exists,get_tqdm,just_metadata
from lltk.tools.logs import *

COL_ID='id'
MATCHRELNAME='rdf:type'
DEFAULT_COMPAREBY=dict(author=0.9, title=0.9)


def log_on(): os.environ['LLTK_LOGGED_ON']='True'
def log_off(): os.environ['LLTK_LOGGED_ON']='False'
def is_logged_on(): return os.environ.get('LLTK_LOGGED_ON')=='True'


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
