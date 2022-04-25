from lltk.imports import *
DBSD={}
DEFAULT_DB='texts'


class LLDB():
    def __init__(self,fn=None,**kwargs):
        from lltk.imports import PATH_LLTK_SHELF
        self.fn=fn if fn else PATH_LLTK_SHELF
        self.db=None
    
    def __enter__(self):
        from lltk.tools import ensure_dir_exists
        ensure_dir_exists(self.fn,fn=True)
        import shelve
        self.db=shelve.open(self.fn)
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db is not None: 
            self.db.close()
            self.db = None
    
    def __getitem__(self, key): return self.get(key)
    def __setitem__(self, key, value): return self.set(key,value)
    def __delitem__(self, key): return self.delete(key)
    def __iter__(self): return self.iterkeys()
    def __len__(self): return self.length

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

    @property
    def length(self):
        with self as db: return len(db)
    def iterkeys(self): 
        with self as db: return iter(db)


def DB(name=DEFAULT_DB,path=None,fn=None):
    if not DBSD.get(name):
        if not path:
            from lltk import PATH_LLTK_DB
            path = PATH_LLTK_DB
        if not fn: fn=os.path.join(path, name+'.shelf')
        DBSD[name]=LLDB(fn)
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






