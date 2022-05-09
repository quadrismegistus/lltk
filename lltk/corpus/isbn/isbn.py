from lltk.imports import *




class TextISBN(BaseText):

    def __init__(self,*x,**y):
        super().__init__(*x,**y)
        self._meta['isbn']=self.id


    def get_query_url(self): return f'https://www.googleapis.com/books/v1/volumes?q=isbn:{self.id}'

    def meta_is_valid(self,meta={}): return bool({'kind'} & (set(meta.keys())|set(self._meta.keys())))

    def query(self,force=False,progress=True,**kwargs):
        if log: log(self.id)
        url = self.get_query_url()
        if log: log(url)
        dat = self.qdb.get(url)
        if log: log(dat)
        if force or not dat:
            if log: log(f'? {self.id}')
            dat = requests.get(url).json()
            if log: log(f'{len(dat)} keys in json: {pf(dat)}')
            self.qdb.set(url,dat)
    
        items = dat.get('items',[])
        if log: log(items)
        
        if not items or not items[0] or not 'kind' in dat:
            odx={**dat}
        else:
            itemd=items[0]
            odx={'kind':dat['kind']}
            if 'volumeInfo' in itemd: odx={**odx, **itemd.pop('volumeInfo')}
            odx={**odx, **itemd}
            
        if 'id' in odx: odx['gid']=odx['id']
        odx['id']=self.id
        odx={k:(v[0] if type(v)==list and len(v)==1 else v) for k,v in odx.items()}
        if log: log(f'-> {odx}')
        return odx







class Isbn(BaseCorpus):
    NAME='ISBN'
    ID='isbn'
    TEXT_CLASS=TextISBN
    REMOTE_SOURCES=[]


    def texts_from(self,text,remote=REMOTE_REMOTE_DEFAULT,cache=True,**kwargs):
        #if log: log(f'<- remote = {remote} ?')
        if log: log(f'!? {text} {text.isbn_l}')
        for i,isbn in enumerate(text.isbn_l):
            if log: log(f'{i} {isbn}')
            isbn=str(isbn).replace('-','').strip().split()[0]
            if log: log(isbn)
            t=self.text(isbn).init_(remote=remote,cache=cache,**kwargs)
            t.add_source(text)
            yield t


        