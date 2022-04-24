from lltk.imports import *
LLDB=None





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










class GlobalDB():
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
        return False

    def delete(self,k,**kwargs):
        with self as db:
            if k in db:
                del db[k]
                return True
        return False


def lltk_db():
    global LLDB
    if LLDB is None: LLDB=GlobalDB()
    return LLDB

def lltk_db_get(k,default=None,**kwargs): return lltk_db().get(k,default,**kwargs)
def lltk_db_set(k,v,**kwargs): return lltk_db().set(k,v,**kwargs)

def lltk_db_get_text(taddr):
    from lltk.text.utils import to_corpus_and_id
    from lltk.imports import COL_CORPUS, COL_ID
    c,i = to_corpus_and_id(taddr)
    res = lltk_db_get(taddr,{})
    # log.debug(f'Got raw data from DB: {res}')
    tcorp = res.get(COL_CORPUS,c)
    tid = res.get(COL_ID,i)
    tmeta = {}
    if res:
        print(res.keys())
        from lltk.text.utils import merge_dict
        tmeta = merge_dict(res, res.get('_meta',{}), tmeta)
        if '_meta' in tmeta: del tmeta['_meta']

        # from lltk.corpus.corpus import Corpus
        # tmeta['_corpus'] = Corpus(tmeta.get('_corpus'))
        # tmeta['_sources'] = unpicklify(tmeta.get('_sources',set()))
        numkeys = len(tmeta)
        log.debug(f'^^ Text({taddr}) [# keys: {numkeys}]')
    return (tcorp,tid,tmeta)

def lltk_db_set_text(text, taddr=None):
    if not taddr: taddr=text.addr
    o = picklify(text.__dict__)
    numkeys=len(o.get('_meta',{}))
    if lltk_db_set(taddr, o):
        log.debug(f'>> Text({taddr}) [# keys: {numkeys}]')

