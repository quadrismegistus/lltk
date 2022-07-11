from lltk.imports import *
from lltk.text import *
from .utils import *

class BaseCorpus(TextList):
    ID=None
    NAME=None
    EXT_TXT='.txt'
    EXT_XML='.xml'
    EXT_NLP='.jsonl'
    EXT_POS='.pkl'
    EXT_FREQS='.json'
    EXT_CADENCE_SCAN='.pkl'
    EXT_CADENCE_PARSE='.pkl'
    COL_ID='id'
    # COL_ID='_addr'
    COL_ADDR='_id'
    col_corpus=COL_CORPUS='corpus'
    COL_FN='fn'
    EXT_HDR='.hdr'
    ENCODING_TXT = 'utf-8'
    ENCODING_XML = 'utf-8'
    TYPE='Corpus'
    TEXT_CLASS=BaseText
    TOKENIZER=tokenize
    MODERNIZE=MODERNIZE_SPELLING
    LANG='en'
    xml2txt = xml2txt_default
    REMOTE_SOURCES = REMOTE_SOURCES







    ####################################################################
    # Overloaded functions
    ####################################################################
    

    def __init__(self,
            id=None,
            _name=None,
            _path=None,
            _id_allows='_',
            _init=False,
            _quiet=False,
            **attrs):


        self.id=id
        self._metadf=None
        self._metadfd={}
        self._addrs=set()
        self._texts=None
        self._textd=defaultdict(lambda: None)
        self._dtmd={}
        self._mfwd={}
        self._init=set()
        self._source=None
        self.name=_name

        if log>1: log(f'{self.__class__.__name__}({get_imsg(id,**attrs)})')
        elif log>0: log(f'{self.__class__.__name__}({id})')

        # make sure we have a name and ID
        if self.id is None and self.ID: self.id=self.ID
        if self.name is None and self.NAME: self.name=self.NAME
        if self.id: self.id=get_idx(self.id,allow=_id_allows)
        if not self.id and type(id)==str and id: self.id=ensure_snake(id)
        if not self.id and self.ID: self.id=self.ID
        if not self.id and self.name: self.id=get_idx(self.name)
        if not self.id: self.id=TMP_CORPUS_ID

        if not self.name and self.NAME: self.name=self.NAME
        if not self.name and self.id: self.name=snake2camel(self.id)
        if not self.name: self.name=self.__class__.__name__
        if self.name: self.name=to_camel_case(self.name)

        ## set attrs
        attrs={**MANIFEST_DEFAULTS, **attrs}
        self._path=os.path.join(PATH_CORPUS,self.id) if _path is None else _path
        for k,v in attrs.items():
            if k.startswith('path_'): k='_'+k
            setattr(self,k,v)
        
        # init?
        self._quiet = _quiet
        if _init: self.init()
    
    def __getattr__(self, name):
        if name.startswith('path_'): return self.get_path(name)
        res = getattribute(self,name)
        return res
    
    def __len__(self): return self.num_texts
    def __getitem__(self, id): return self.text(id)
    def __setitem__(self, id, text): 
        if is_text_obj(text) or is_addr(text):
            return self.text(id, _source=text, _force=True)
    def __delitem__(self, key): self.remove_text(id)
    # def __iter__(self): return iter(self.iter_texts(progress=True))

    def __repr__(self): 
        cname=self.__class__.__name__
        if not cname.endswith('Corpus') and not cname.startswith('Corpus'): cname+='Corpus'
        return f'{cname}({self.id})'








    ####################################################################
    # PATHS
    ####################################################################


    def get_path(self,name):
        path = getattribute(self, name)
        if path is None: path = getattribute(self, '_'+name)
        if path is not None and not os.path.isabs(path):
            if '~' in path:
                path=path.split('~')[-1]
                path=os.path.join(os.path.expanduser('~'), path[1:])
            else:
                path=os.path.join(self.path, path)
        return path

    
    @property
    def path(self):
        res=getattribute(self,'_path')
        if res is None: res=os.path.expanduser(os.path.join(PATH_CORPUS,self.id))
        return res
    @property
    def path_data(self): return os.path.join(self.path,'data')
    @property
    def path_matches(self): return os.path.join(self.path_data,'rels')



    ####################################################################
    # ETC?
    ####################################################################


    @property
    def xml2txt_func(self): self.XML2TXT.__func__
    def xml2txt(self,*x,**y): return self.XML2TXT.__func__(*x,**y)

    @property
    def path_key(self):
        ofn=os.path.join(PATH_LLTK_KEYS,f'{self.id}.key')
        ensure_dir_exists(ofn,fn=True)
        return ofn

    

    @property
    def key(self):
        if not os.path.exists(self.path_key):
            if self.id in get_inducted_corpus_ids():
                self.acquire_key()
            else:
                self.generate_key()
        if not os.path.exists(self.path_key): return
        from cryptography.fernet import Fernet
        return Fernet(self.fetch_key())


    def fetch_key(self): 
        with open(self.path_key,'rb') as f: keyb_encr=f.read()
        keyb = self.userkey.decrypt(keyb_encr)
        return keyb

    def acquire_key(self,url=KEYSERVER_URL):
        infod = get_user_info()
        email = get_user_email()

        optname='always_accept_corpus_permissions'
        if infod.get(optname)!='y':
            yn = input(f'Do you, {email}, agree to use all data from the corpus "{self.name}" for lawful purposes of non-consumptive research? Type "Y" for yes, "N" for no, or "A" to consent to all such requests in the future.').strip().lower()
            if yn=='n': return
            if yn=='a': set_usr_info(**{optname:'y'})

        msg=f'{self.id}|{email}'
        msg_encr = get_pkey().encrypt(msg.encode())
        msg_encr64 = to_bs64(msg_encr)
        url = url + msg_encr64.decode()
        keystr = gethtml(url)
        keyb_encr = keystr.encode()
        with open(self.path_key,'wb') as of: of.write(keyb_encr)
        
        # f=get_passkey(email)
        # keyb=f.decrypt(keyb_encr)
        # with open(self.path_key,'wb') as of: of.write(keyb)
        # self._key=keyb
        # return keyb    

    def generate_key(self,force=False):
        if force or not os.path.exists(self.path_key):
            from cryptography.fernet import Fernet
            key = Fernet.generate_key()
            key_encr = self.userkey.encrypt(key)
            with open(self.path_key,'wb') as of: of.write(key_encr)

    def encrypt(self,obj): return self.key.encrypt(obj)
    def decrypt(self,obj): return self.key.decrypt(obj)


    ####################################################################
    # ADDRS
    ####################################################################
    # Corpus addr
    @property
    def addr(self): return IDSEP_START+self.id
    # get address of my text
    def get_addr(self,id): return IDSEP_START + self.id + IDSEP + id
    # magic lists
    @property
    def addrs(self): return [self.get_addr(id) for id in self.ids]
    @property
    def ids(self): return sorted(list(self._textd.keys()))






    ####################################################################
    # SYNCING to db
    ####################################################################


    def sync(self,progress=True,force=False,**kwargs):
        iterr = list(self.init_meta_csv())
        iterr = [(self.get_addr(id),d) for id,d in iterr]
        if progress: iterr = get_tqdm(iterr, desc=f'[{self.name}] Syncing texts to db')
        o=[]
        with log.silent:
            for addr,d in iterr:
                # if not force and addr in ids_done: continue
                meta = safebool(just_meta_no_id(d))
                t=Text(addr,**meta)
                t.cache(force=True)
                o.append(t)
        return TextList(o)

        

    def init_meta(self,sources=['csv'],merger=None,allow_hidden=False,*x,**y):
        
        def _filter(odx): 
            if allow_hidden: return odx
            return {k:v for k,v in odx.items() if k and k[0]!='_'}

        if sources == 'csv' or sources == ['csv']:
            for id,d in self.init_meta_csv(*x,**y):
                yield (id,_filter(d))
        else:
            if merger is None: merger = merge_dict
            if log>0: log(self)
            id2ld=defaultdict(list)
            for source in sources:
                if source=='csv':
                    # log('Init from csv')
                    for id,dx in self.init_meta_csv(*x,**y):
                        id2ld[id].append(dx)
                elif source=='json':
                    # log('Init from json')
                    for id,dx in self.init_meta_json(*x,**y):
                        id2ld[id].append(dx)
            
            # yield merged
            for id,ld in sorted(id2ld.items()):
                if type(ld)!=list or not ld: continue
                odx=merger(*ld) if len(ld)>1 else ld[0]    
                yield (id, _filter(odx))


    def init_meta_json(self,force=False,bad_cols=BAD_COLS,meta_fn='meta.json',**kwargs):
        # log(f'Initializing from json files: {self.addr}')
        if log>0: log(self)
        for root,dirs,fns in os.walk(self.path_texts):
            if meta_fn in set(fns):
                meta_root=os.path.abspath(root)
                meta_fnfn=os.path.join(root,meta_fn)
                idx = meta_root.replace(self.path_texts,'')
                idx = idx[1:] if idx.startswith('/') else idx
                yield idx, read_json(meta_fnfn)

    def init_meta_csv(self,*x,**y):
        if log>0: log(self)
        if not os.path.exists(self.path_metadata): self.install_metadata()
        if os.path.exists(self.path_metadata):
            df=read_df_anno(self.path_metadata,dtype=str)
            if type(df)==pd.DataFrame and len(df) and self.col_id in set(df.columns):
                df=df.set_index(self.col_id)
                o1=df.index
                o2=df.to_dict('records')
                yield from zip(o1,o2)







    ####################################################################
    # TEXTS
    ####################################################################


    def text(self,
            id: Union[str,BaseText,None] = None,
            _source: Union[str,BaseText,None] = None,
            _add: bool = True,
            _cache: bool = True,
            _force: bool = False,
            _new: bool = False,
            _init: bool = False,
            _remote: Union[bool,None] = None,
            **kwargs) -> BaseText:
        """
        The one function users need to interact with a corpus's texts. Use this function both to get an existing text or to create a new one. Returns a text of type `corpus.TEXT_CLASS`. If an `id` is not specified, one will be auto-generated.

        Parameters
        ----------
        id : Union[str,BaseText,None], optional
            If an `id` is specified, this will be used as the text's ID. If `id` is a text object, the incoming text's address (`text.addr`) will be used as the new text's ID as well as its source (added to the set, `text._sources`. Default: None.

        _source : Union[str,BaseText,None], optional
            An explicitly declared source text. Default: None.

        _add : bool, optional
            Add the text to the corpus? Default: True.
        
        _cache : bool, optional
            Cache the text in the database? Default: True.
        
        _force : bool, optional
            Whether to overwrite existing cache of the text. Default: False.

        _new : bool, optional
            Whether to force the creation of a new text (iterating id if necesssary). Default: False.

        _init : bool, optional
            Whether to initialize corpus if not done so yet. Default: False.



        Returns
        -------
        BaseText
            A text of a class given in `corpus.TEXT_CLASS`.

        Raises
        ------
        CorpusTextException
            If text creation fails.
        """        

        # log incoming
        # if log: log(f'<- {kwargs}')
        meta = just_metadata(kwargs)
        if log>1:  log(f'<- {get_imsg(id,self,_source,**meta)}')

        # Init corpus?
        if _init: self.init()

        # Defaults
        t = None

        if is_textish(id) and not _source: _source=id
        id = self.get_text_id(id, _source=_source, _new=_new, **meta)

        # get?
        if not _force and id is not None: t = self.get_text(id)
        
        # Create?
        if _force or t is None: t = self.init_text(id,_source=_source,_cache=_cache,_remote=_remote,**meta)
        elif meta and is_text_obj(t): t.update(meta,_cache=_cache)
        
        # Fail?
        if t is None: raise CorpusTextException('Could not get or create text')
        
        # Add to my own dictionary?
        # if _add: self.add_text(t)
        
        # add source?
        if _source: t.add_source(_source)

        # Return text
        if log>1: log(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t

    def text_from(self,text,**kwargs):
        for newtext in self.texts_from(text,**kwargs):
            return newtext

    def texts_from(self, id: Union[str,BaseText], add_source=True, remote=REMOTE_REMOTE_DEFAULT, cache=True,**kwargs):
        # Sublcass this in query-like corpus classes (Wikidata, Hathi)
        # so that the resulting text can be of a very different kind
        # (i.e. query on incoming text's title, sort through results, etc)
        # by default this will just use .text() and an incoming text's
        # address will be used as the ID
        #if log: log(f'<- remote = {remote} ?')
        oldtext=Text(id)
        newtext=self.text(oldtext,**kwargs).init_(remote=remote,cache=cache,**kwargs)
        newtext.match(oldtext)
        yield newtext


    def get_text(self,id:str,_use_db:bool=True) -> Union[BaseText,None]:
        """Attempt to get a pre-existing text.

        Parameters
        ----------
        id : str
            ID, not address.
        _use_db : bool, optional
            Look in database?, by default True

        Returns
        -------
        Union[BaseText,None]
            Either a text object if found, or None if not.
        """

        if log>1: log(f'<- id = {id}')
        
        t=self._textd.get(id)
        if log>1: log(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t
        


    def remove_text(self,id,db=True,matches=True):
        if id in self._textd:
            if log>0: log(f'removing {id} from {self.id}._textd')
            del self._textd[id]
        if db: self.db().delete(id)
        if matches: self.matcher.remove_node(self.get_addr(id))


    def get_text_id(self,
            id: Union[str,BaseText,None] = None,
            _source: Union[str,BaseText,None] = None,
            _new: bool = False,
            **kwargs):

        meta=just_metadata(kwargs)
        if log>1: log(f'<- {get_imsg(id,self,_source,**meta)}')
        
        if type(id)!=str or not id:
            if is_addr_str(id): id=id
            if is_text_obj(id): id=id.addr
            if is_addr_str(_source): id=_source
            if is_text_obj(_source): id=_source.addr        
        else:
            # otherwise?
            id = get_idx(
                id=id,
                # i=len(self._textd)+1,
                **meta
            )
        if not id: id=get_idx()
        if _new: id = self.iter_text_id(id)
        if log>1: log(f'-> {id}')
        return id


    def iter_text_id(self,id,_new=True,newsep='/v'):        
        while _new and id in self._textd:
            if log>0: log(f'<- {id}')
            idsuf=id.split(newsep)[-1]
            if idsuf.isdigit():
                isuf=int(idsuf)+1
                id=f'{newsep.join(id.split(newsep)[:-1])}{newsep}{isuf}'
            else:
                isuf=2
                id=f'{id}{newsep}{isuf}'
        if log: log(f'-> {id}')
        return id


    def init_text(self,id=None,_source=None,_cache=True,_remote=None,**kwargs):
        # if log: log('...')
        meta=just_meta_no_id(kwargs)
        if log>1: log(f'<- {get_imsg(id,self,_source,**meta)}')
        if id is None: id = self.get_text_id(id, _source=_source, **meta)
        # gen text in my image        
        t = self.TEXT_CLASS(id=id, _corpus=self, _source=_source, _remote=_remote, **meta)
        t._cacheworthy=True
        if _cache: t.cache()
        if log>1: log(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t


    def add_text(self,t,force=True,**kwargs):
        # assign by id
        if t is not None and is_text_obj(t):
            if force or self._textd.get(t.id) is None:
                self._textd[t.id]=t
                self._metadf=None # redo meta
    
    def add_texts_from(self,corpus_or_texts,**kwargs):
        c_t = corpus_or_texts
        texts = Corpus(c_t).texts(**kwargs) if type(c_t)!=list else c_t
        for t in texts: self.text(t,**kwargs)



    def query_db(self,fn='query_cache'):
        return DB(os.path.join(self.path,'data',fn), engine='sqlite')
    @property
    def qdb(self): return self.query_db()


    # def db(self,name,*x,**y): return self.query_db(fn=name)






    @property
    def paths(self):
        return self.__paths
    
    
    @property
    def t(self):
        ol=list(self.texts(progress=False))
        return random.choice(ol) if len(ol) else None




    # Get texts
    @property
    def textd(self):
        # if log: log('...')
        self.init()
        return self._textd
        
    def texts(self,*args,**kwargs):
        return self.iter_texts(*args,**kwargs)
    def itexts(self,*x,**y): return self.iter_texts(*x,**y)

    def iter_texts(self,texts=None,progress=False,shuffle=False,lim=None,verbose=False,**kwargs):
        if not texts: texts=list(self.textd.values())
        if not texts: return []
        if texts: o=list(map(Text, texts))
        if shuffle: random.shuffle(o)
        if lim: o=o[:lim]
        if progress:
            iterr=get_tqdm(o,desc=f'[{self.name}] iterating texts')
        else:
            iterr=o
        for t in iterr:
            if type(progress)==int and progress>1: iterr.set_description(f'[{self.name}] yielding: {t.id}')
            yield t
    
    def corpus_texts(self,*args,**kwargs): yield from self.texts(*args,**kwargs)
    
    # Convenience
    @property
    def num_texts(self): return len(self.textd)
    @property
    def text_ids(self): return list(self.textd.keys())




    ####################################################################
    # METADATA
    ####################################################################


    @property
    def au(self): return self.authors

    @property
    def authors(self,authorkey='author',titlekey='title',force=False,idkey=None,**kwargs):
        if idkey is None: idkey=self.col_id
        if force or self._authors is None:
            authord={}
            for t in self.texts(progress=False,**kwargs):
                au,ti=t.au,t.ti
                if au and ti:
                    if not au in authord: authord[au]=AuthorBunch(corpus=self,name=au)
                    aobj = authord[au]
                    setattr(aobj,ti,t)
            
            bigau=Bunch()
            for author,author_obj in authord.items():
                setattr(bigau,author,author_obj)
            self._authors = bigau
        return self._authors

        
    

    def iter_init(self,progress=True,_init=True,_cache=False,remote=False,lim=None,shuffle=False,**kwargs):
        #if log: log(f'<- remote = {remote}')
        remote=is_logged_on()

        iterr=[{**d, **{'id':i}} for i,d in self.init_meta()]
        if shuffle: random.shuffle(iterr)
        iterr=iterr[:lim]

        if iterr:
            if progress:
                iterr=get_tqdm(
                    iterr,
                    desc=f'[{self.name}] Loading corpus'
                )

            for dx in iterr:
                id=to_corpus_and_id(dx['id'])[1]
                meta=just_meta_no_id(dx)
                t = self.text(
                    id, 
                    _source=None,
                    _add = True,
                    _cache=False,
                    _force = False,
                    _new = False,
                    _init = False,
                    _remote = remote,
                    **meta)
                # if _init: t.init(cache=_cache,remote=remote,**kwargs)
                yield t

    def init_(self,remote=REMOTE_REMOTE_DEFAULT,cache=True,progress=2,**kwargs):
        with log.silent:
            for t in self.texts(progress=progress,**kwargs):
                t.init(remote=remote,cache=cache,**kwargs)
        

    # def init(self):
    #     if not self._data_all:
    #         log.info('loading csv')
    #         self._data_all=[
    #             {
    #                 **d, 
    #                 **{'_id':self.get_addr(id)}
    #             }
    #             for id,d in self.init_meta_csv()
    #         ]


    # def init(self,force=False,progress=False,remote=None,**kwargs):
    #     if log: log(self)
    #     remote=is_logged_on()
    #     if not self._texts or not remote in self._init:
    #         o = []
    #         if log: log('....')
    #         corp_ld = self.mdb.get(corpus=self.id)
    #         iterr = (Text(d) for d in corp_ld)
    #         if progress:
    #             iterr=get_tqdm(iterr,total=len(corp_ld),desc=f'[{self.id}] init')
    #         with log.silent:
    #             for t in iterr:
    #                 o.append(t.init())
    #         self._texts = o
    #         self._init |= {remote}
    #     return self._texts

    # def init(self,force=False,quiet=False,sync=True,progress=True,remote=False,cache=False,_init=True,lim=None,**kwargs):
    #     if not force and self._init: return self
    #     # if log: log('...')
    #     if log>0: log(self)
    #     remote=is_logged_on()
    #     #if log: log(f'<- remote = {remote}')

    #     texts=[]
    #     def go():
    #         numdone=0
    #         for t in self.iter_init(_init=_init,progress=progress,remote=remote,lim=lim,_cache=cache,**kwargs): numdone+=1
    #         return numdone
    #     def run():
    #         if not quiet: return go()
    #         with log.silent: return go()

    #     numdone=run()

    #     if numdone:
    #         if log: log(f'initialized {numdone} texts')
    #         self._init=True
    #     elif sync:
    #         if log: log(f'no texts initialized. corpus in db? trying to sync...')
    #         res = self.sync()
    #         if type(res)==list:
    #             texts = [Text(addr) for addr in res]
    #             if log and texts: log(f'{len(ids)} texts synced')
            
    #     return texts

    def init(self,force=False):
        if not force and self._init: return self
        for t in self.iter_init(): pass
        self._init=True
    
    def metadata(
            self,
            force=False,
            progress=True,
            lim=None,
            fillna='',
            from_cache=True,
            from_sources=True,
            cache=False,
            remote=False,
            sep=META_KEY_SEP,
            meta={},
            **kwargs):
        
        # self.init()
        remote=is_logged_on()
        #if log: log(f'<- remote = {remote}')
        key=(lim,fillna,from_cache,from_sources,remote)
        old_metadf=self._metadfd.get(key)
        if force or old_metadf is None:
            new_metadf=pd.DataFrame(
                t.metadata(
                    from_cache=from_cache,
                    from_sources=from_sources,
                    remote=remote,
                    cache=cache,
                    sep=sep,
                    meta=meta,
                    **kwargs
                )
                for ti,t in enumerate(self.texts(progress=progress))
                if t is not None
                and t.metadata is not None
                and not lim or ti<lim
            )
            if fillna is not None:
                new_metadf=new_metadf.fillna(fillna)
            if self.col_id in set(new_metadf.columns):
                new_metadf=new_metadf.set_index(self.col_id)
            self._metadfd[key]=new_metadf
            if self._metadf is None: self._metadf=new_metadf
            # self.save_metadata(ometa=mdf,force=force_save)
            # close dbs?
            close_dbs()

        return self._metadfd.get(key,pd.DataFrame())

    @property
    def meta(self): return self.metadata(force=False)
    @property
    def metadf(self): return self.meta
    @property
    def df(self): return self.metadf


    @property
    def addr2meta(self):
        if not hasattr(self,'_addr2meta'):
            self._addr2meta=a2m={}
            for t in self.texts():
                meta=t.meta
                a2m[t.addr]=meta
        return self._addr2meta

    @property
    def metad(self):
        if not hasattr(self,'_metad'):
            self._metad=dict(list(zip(self.text_ids,self.meta)))
        return self._metad





    ####################################################################
    # SOURCES
    ####################################################################

    def iter_sources(self,texts=[],include_t=True,**kwargs):
        for t in self.texts(texts=texts,**kwargs):
            tsrcs=t.get_sources(**kwargs)
            yield (t,tsrcs) if include_t else tsrcs
        
    def get_sources(self,**kwargs):
        return list(self.iter_sources(**kwargs))




    # #################
    # #### PROCESSING
    # #################


    def zip(self,savedir=PATH_CORPUS_ZIP,ask=False,parts=ZIP_PART_DEFAULTS):
        if not os.path.exists(savedir): os.makedirs(savedir)
        here=os.getcwd()
        ## ask which parts
        if not parts and ask:
            part2ok=defaultdict(None)
            for part in sorted(parts):
                path_part=getattr(self,f'path_{part}')
                if not path_part or not os.path.exists(path_part): continue
                part2ok[part]=input('>> [%s] Zip %s file(s)?: ' % (self.name, part)).strip().lower().startswith('y') if ask else True
        elif parts:
            part2ok=dict((part,True) for part in parts)
        else:
            return

        def _paths(path,pathpart=''):
            # paths in folder
            paths = []
            for root, dirs, files in os.walk(path):
                for file in files:
                    ofnfn=os.path.join(root, file)
                    paths.append(ofnfn)
            if pathpart=='raw': return paths
            # do they belong to this corpus?
            try:
                pppath=getattr(self,f'path_{pathpart}')
                pppath0 = os.path.dirname(pppath)
                acceptable_paths = {getattr(t,f'path_{pathpart}') for t in self.texts()}
                acceptable_paths = {p.replace(pppath0+os.path.sep,'') for p in acceptable_paths}
                # if log: log('orig paths',len(paths),list(paths)[:5])
                # if log: log('ok paths',len(acceptable_paths), list(acceptable_paths)[:5])
                paths = list(set(paths) & acceptable_paths)
            except AssertionError:
                pass
            return paths


        def zipdir(path, ziph, pathpart='', paths=None):
            # ziph is zipfile handle
            if not paths: paths=_paths(paths,pathpart)
            for ofnfn in paths:
                ziph.write(ofnfn)
                yield ofnfn

        def do_zip(path,fname,msg='Zipping files',default=False, pathpart=''):
            if not os.path.exists(path): return
            #if ask and input('>> {msg}? [{path}]\n'.format(msg=msg,path=path)).strip()!='y': return
            if not default: return

            if not fname.endswith('.zip'): fname+='.zip'
            opath=os.path.join(savedir,fname)

            path1,path2=os.path.split(path)

            with zipfile.ZipFile(opath,'w',zipfile.ZIP_DEFLATED) as zipf:
                os.chdir(path1)
                paths=list(_paths(path2,pathpart)) if os.path.isdir(path2) else [path2]
                #if log: log(type(paths),paths[:3])
                zipper = zipdir(path2, zipf, pathpart=pathpart, paths=paths)
                for ofnfn in self.get_tqdm(zipper, total=len(paths), desc=f'[{self.name}] Compressing {fname}'):
                    pass

        for part in part2ok:
            if not part2ok[part]: continue
            do_zip(getattr(self,f'path_{part}'), f'{self.id}_{part}.zip', f'Zip {part} files',part in parts,pathpart=part)
        os.chdir(here)
        # do_zip(self.path_txt, self.id+'_txt.zip','Zip txt files','txt' in parts)
        # do_zip(self.path_freqs, self.id+'_freqs.zip','Zip freqs files','freqs' in parts)
        # do_zip(self.path_metadata, self.id+'_metadata.zip','Zip metadata file','metadata' in parts)
        # do_zip(self.path_xml, self.id+'_xml.zip','Zip xml files','xml' in parts)
        # do_zip(self.path_data, self.id+'_data.zip','Zip data files (mfw/dtm)','xml' in parts)


    def uninstall(self):
        # Start from scratch
        pass


    def upload(self,ask=False,uploader='dbu upload',dest=DEST_LLTK_CORPORA,zipdir=None,overwrite=False,parts=ZIP_PART_DEFAULTS):
        #if not overwrite: uploader+=' -s'
        if not zipdir: zipdir=os.path.join(PATH_CORPUS,'lltk_corpora')
        here=os.getcwd()
        os.chdir(zipdir)
        #if log: log('?',zipdir,os.listdir('.'))


        cmds=[]
        for fn in os.listdir('.'):
            if not fn.endswith('.zip'): continue
            if not fn.startswith(self.id): continue
            if not parts:
                if ask:
                    if not input(f'>> [{self.name}] Upload {fn}? ').strip().lower().startswith('y'): continue
            else:
                part=fn.replace('.zip','').split('_')[-1]
                if part not in set(parts): continue

            cmd='{upload} {file} {dest}'.format(upload=uploader,file=fn,dest=dest)
            cmds.append(cmd)
        # cmdstr="\n".join(cmds)
        # if log: log(f'Executing:\n{cmdstr}')

        for cmd in self.get_tqdm(cmds,desc=f'[{self.name}] Uploading zip files'):
            os.system(cmd)

        os.chdir(here)

    def share(self,cmd_share='dbu share',dest=DEST_LLTK_CORPORA):
        ol=[]
        import subprocess
        ln='['+self.name+']'
        if log: log(ln)
        ol+=[ln]
        for part in ZIP_PART_DEFAULTS:
            fnzip = self.id+'_'+part+'.zip'
            cmd=cmd_share+' '+os.path.join(dest,fnzip)
            try:
                out=str(subprocess.check_output(cmd.split()))
            except (subprocess.CalledProcessError,ValueError,TypeError) as e:
                #if log: log('!!',e)
                continue
            link=out.strip().replace('\n','').split('http')[-1].split('?')[0]
            if link: link='http'+link+'?dl=1'

            url='url_'+part+' = '+link
            if log: log(url)
            ol+=[url]
        if log: log()
        #return '\n'.join(ol)

    def get_tqdm(self,*x,desc='',**y):
        if desc: desc=f'[{self.name}] {desc}'
        return get_tqdm(*x,desc=desc,**y)


    def mkdir_root(self):
        if not os.path.exists(self.path_root): os.makedirs(self.path_root)


    def urls(self):
        urls=[(x[4:], getattr(self,x)) for x in dir(self) if x.startswith('url_') and getattr(self,x)]
        return urls



    def compile(self, **attrs):
        ## THIS NEEDS TO BE OVERWRITTEN BY CHILD CLASS
        return

    def info(self):
        ol = []
        ol += [f'[{self.name}]']
        for x in ['id','desc','link','public']:
            xstr=x
            v=getattr(self,x)
            if v: ol+=[f'{xstr}: {v}']
        if log: log('\n'.join(ol))

    def install_metadata(
            self,
            ask=False,
            force=False,
            part='metadata',
            **kwargs):
        self.install(
            ask=ask,
            force=force,
            part=part,
            **kwargs
        )

    def install(self, part=None, ask=True, urls={}, force=False, flatten=False, parts=None, unzip=True, **attrs):
        if not parts: parts=DOWNLOAD_PART_DEFAULTS
        if type(parts)==str: parts=[p.strip().lower() for p in parts.split(',')]
        if not part and parts:
            for part in parts: self.install(ask=ask, urls=urls, part=part, parts=[], force=force, **attrs)
            return self
        if not part: return
        opath=getattr(self,f'path_{part}')
        tmpfnfn=self.path_zip(part)
        tmpfn=os.path.basename(tmpfnfn)
        if not os.path.exists(tmpfnfn):
            if not urls: urls=dict(self.urls())
            url=urls.get(part)
            if not url: return
            # self.mkdir_root()
            tmpfnfndir=os.path.dirname(tmpfnfn)
            if not os.path.exists(tmpfnfndir): os.makedirs(tmpfnfndir)
            tools.download(url,tmpfnfn,desc=f'[{self.name}] Downloading {tmpfn} to {mask_home_dir(tmpfnfn)}',force=force)
        if unzip:
            odir=self.path_raw if part=='raw' else (os.path.dirname(opath) if not flatten else opath)
            from zipfile import BadZipFile
            try:
                tools.extract(tmpfnfn,odir,desc=f'[{self.name}] Unzipping {tmpfn} to {mask_home_dir(odir)}',flatten=flatten)
            except BadZipFile:
                log.error(f'Bad zip file: {tmpfnfn}')
                rmfn(tmpfnfn)
                return self.install(ask=ask, urls=urls, force=force, part=part, flatten=flatten, parts=parts, unzip=unzip, **attrs)
            #if os.path.exists(self.path_raw) and os.listdir(self.path_raw)==['raw']:
            #	os.rename(os.path.join(self.path_raw,'raw'), self.path_raw)
        return self

    def path_zip(self,part):
        return os.path.join(PATH_CORPUS_ZIP,f'{self.id}_{part}.zip')

    # def get_path_text(self,text,part):
    #     if part.startswith('path_'): part=part[5:]
    #     if part == 'txt': return os.path.join(text.path,'text.txt')
    #     if part == 'xml': return os.path.join(text.path,'text.xml')
    #     if part in {'json','meta','meta_json'}: return os.path.join(text.path,'meta.json')
    #     if part == 'freqs': return os.path.join(text.path,'freqs.json')
    #     return None

    def get_path_text(self,text,*x,**y): return text.get_path(*x,**y)


    def compile_download(self,unzip=True):
        return self.install(part='raw',unzip=unzip)

    def preprocess(self,parts=PREPROC_CMDS,verbose=True,force=False,num_proc=DEFAULT_NUM_PROC,**attrs):
        if not parts: parts=PREPROC_CMDS
        if type(parts)==str: parts=[p.strip().lower() for p in parts.split(',')]
        for part in parts:
            fname='preprocess_'+part
            if log>0: log(pf(part,fname,getattr(self,fname)))
            if not hasattr(self,fname): continue
            func=getattr(self,fname)
            try:
                x=func(verbose=verbose,num_proc=int(num_proc),force=force, **attrs)
            except TypeError as e:
                self.log(f'!! ERROR: {e}')
                pass

    def preprocess_misc(self): pass

    def has_data(self,part):

        ppart=f'path_{part}'
        if not hasattr(self,ppart): return
        path=getattr(self,ppart)
        if not os.path.exists(path): return False
        if os.path.isdir(path) and not os.listdir(path): return False
        # if log: log(part,ppart,path,os.path.exists(path))

        return path



    def has_url(self,part):
        ppart=f'url_{part}'
        if not hasattr(self,ppart): return False
        if not getattr(self,ppart): return False
        return getattr(self,ppart)







    def export(self,folder,meta_fn=None,txt_folder=None,compress=False):
        """
        Exports everything to a folder.
        """
        if not os.path.exists(folder): os.makedirs(folder)
        if not meta_fn: meta_fn='corpus-metadata.%s.txt' % self.name
        if not txt_folder: txt_folder='texts'

        # save metadata
        meta_fnfn=os.path.join(folder,meta_fn)
        self.save_metadata(meta_fnfn)

        # save texts
        txt_path=os.path.join(folder,txt_folder)
        if not os.path.exists(txt_path): os.makedirs(txt_path)
        import shutil
        for t in self.texts():
            ifnfn=t.path_txt
            if not os.path.exists(ifnfn): continue
            ofnfn=os.path.join(txt_path, t.id+t.ext_txt)
            ofnfn_path=os.path.split(ofnfn)[0]
            if not os.path.exists(ofnfn_path): os.makedirs(ofnfn_path)
            shutil.copyfile(ifnfn,ofnfn)
            #break

    @property
    def path_mfw(self):
        path_mfw=os.path.join(self.path_data,'mfw')
        #if not os.path.exists(path_mfw): os.makedirs(path_mfw)
        return path_mfw
    @property
    def path_dtm(self):
        path_dtm=os.path.join(self.path_data,'dtm')
        #if not os.path.exists(path_dtm): os.makedirs(path_dtm)
        return path_dtm
    @property
    def path_home(self):
        return os.path.join(PATH_CORPUS,self.id)
    @property
    def path_texts(self):
        return os.path.join(self.path,DIR_TEXTS_NAME)










class SectionCorpus(BaseCorpus):
    def init(self): pass ##@TODO ???

    @property
    def source(self): return self._source
    @property
    def path(self):
        return os.path.join(self.source.path, self._id)
    def path_(self,_id=DIR_SECTION_NAME):
        return os.path.join(self.source.path, _id)
    @property
    def addr(self): return f'{IDSEP_START}{self.source.corpus.id}{IDSEP}{self.id}'
    def get_section_class(self,*x,**y): return self.source.get_section_class(*x,**y)

    def init_text(self,id=None,section_class=None,**meta):
        # make id
        if id is None: id=get_idx(i=len(self._textd), prefstr='S', numposs=1000)
        # id=os.path.join(self.id,id)
        # check not duplicate
        #assert id not in set(self._textd.keys())
        if id not in self._textd:
            # gen obj
            section_class=self.get_section_class(section_class)
            sec = section_class(id, _source=self.source, _section_corpus=self, **meta)
            self._textd[id]=sec
        else:
            sec=self._textd[id]
        return sec

    @property
    def txt(self): return self.get_txt()

    def get_txt(self,extra_txt_pref=[],force=False,**kwargs):
        if force or self._txt is None or self._txt_offsets is None:
            otxt=''
            offset=0
            offsets={}
            for t in self.texts():
                txt=t.txt
                if extra_txt_pref:
                    txt_pref='\n\n\n'.join(t.meta.get(x,'') for x in extra_txt_pref)
                    txt=txt_pref+'\n\n\n'+txt
                txt+='\n\n\n\n\n'
                offset2=offset + len(txt)
                otxt+=txt
                offsets[t.id]=(offset,offset2)
                offset=offset2
            self._txt_offsets=offsets
            self._txt=otxt
        return self._txt


CORPUS_CACHE={}

def Corpus(corpus=None,force=False,init=False,clear=False,**kwargs):
    global CORPUS_CACHE
    
    C = None
    
    if is_corpus_obj(corpus):
        C=corpus
        logg=False
    else:
        if log>1: log(f'<- id = {corpus}')
        logg=True
        
        if type(corpus)==str and corpus:
            if corpus in CORPUS_CACHE:
                C=CORPUS_CACHE[corpus]
            else:
                C=load_corpus(corpus,_init=init,**kwargs)

    if C is None: C=BaseCorpus(corpus,_init=init,**kwargs)
    
    if C is not None:
        CORPUS_CACHE[C.id]=CORPUS_CACHE[C.name]=C
        if clear:
            C.clear(**kwargs)
        elif init:
            C.init(**kwargs)

    if logg and log.verbose>1: log(f'-> {C}')
    return C




