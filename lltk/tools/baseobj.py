from lltk.imports import *
from persistent import Persistent



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


class BaseObject(Persistent):
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
    def _zid(self): return self.addr

    def zsave(self):
        db=get_zodb()
        db[self._idz]=self
        db.save()

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
    
    @property
    def path(self):
        from lltk.imports import PATH_LLTK_ZODB
        return PATH_LLTK_ZODB


    ### ATTR METHODS
    def __init__(self,path=None,open=True,**kwargs):
        if path: self.path = path
        if open: self.open()
    
    @property
    def path_lock(self): return self.path+'.lock'

    ## Convenience methods
    def __getitem__(self, key, tryagain=True):
        from lltk.text.utils import is_text_obj,is_broken_obj
        from lltk.text.text import Text

        if not self.opened: self.open()
        t = self.root.get(str(key))
        # log.debug([t,is_text_obj(t),is_broken_obj(t)])
        # if hasattr(t,'__dict__'): log.debug(t.__dict__)
        # if is_text_obj(t) and is_broken_obj(t):
            # from lltk.imports import BROKENSTATE
            # d = t.__dict__.get(BROKENSTATE,{})
            # t = t.__init__(**d.get('_meta',{}))
        return t


    def __setitem__(self, key, value):
        if key is not None and value is not None:
            key=str(key)
            oldval = self.root.get(key)
            if value != oldval:
                self.root[key]=value
                log.debug(f'DB["{key}"] = {value}')
                self.save()
        
    def __delitem__(self, key):
        if key in self.root: del self.root[key]
    def __iter__(self): return iter(self.root.items())
    def __len__(self): return len(self.root)
    def get(self,k): return self[k]



    ### DB METHODS
    @property
    def opened(self):
        from ZODB.Connection import Connection
        return type(self.conn) == Connection and self.conn.opened

    def open(self,force=False):
        #if not force and self.opened: return

        import ZODB
        from lltk.tools import ensure_dir_exists

        self.close()
        ensure_dir_exists(self.path,fn=True)

        self.conn = ZODB.connection(self.path)
        self.conn.open()
        self.root = self.conn.root()
        
        

    def close(self):
        from lltk.tools import rmfn
        
        if self.opened: self.conn.close()
        rmfn(self.path_lock)

    def save(self):
        log.debug('Saving ZDB')
        import transaction
        transaction.commit()


    




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











def show_t(t):
    if t is None: return
    o=f'''
t._meta = {t._meta}
t.author = {t.author}
t.title = {t.title}
t.year = {t.year}

t.matches = {t.matches}
t._sources = {t._sources}
    '''
    o+=f'''
t.sources = {t.sources}
t.wiki = {t.wiki}
    '''
    o+=f'''
t.sources = {t.sources}
    '''
    t.log(o)