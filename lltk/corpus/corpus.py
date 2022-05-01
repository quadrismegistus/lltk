from lltk.imports import *



class BaseCorpus(BaseObject):
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
    COL_ADDR='_addr'
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







    ####################################################################
    # Overloaded functions
    ####################################################################
    

    def __init__(self,
            id=None,
            _name=None,
            _path=None,
            _id_allows='_',
            _init=False,
            _quiet=True,
            **attrs):


        self.id=id
        self._metadf=None
        self._metadfd={}
        self._texts=None
        self._textd=defaultdict(lambda: None)
        self._dtmd={}
        self._mfwd={}
        self._init=False
        self._source=None
        self.name=_name

        if log.verbose>1: log(f'{self.__class__.__name__}({get_imsg(id,**attrs)})')
        elif log.verbose>0: log(f'{self.__class__.__name__}({id})')

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
    def __iter__(self): return iter(self.iter_texts(progress=True))
    # def __repr__(self): return f'[{self.__class__.__name__}]({self.id})'
    # def __repr__(self): 
        # return f'Corpus({self.id})'
    
    # def __repr__(self):
    #     if type(self)!=BaseCorpus:
    #         return f'[{self.__class__.__name__}]'
    #     else:
    #         return f'[{self.id}]'

    # def __repr__(self):
    #     if type(self)!=BaseCorpus:
    #         return f'{self.__class__.__name__}Corpus({self.id})'
    #     else:
    #         return f'Corpus({self.id})'

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
        if res is None: res=os.path.join(PATH_CORPUS,self.id)
        return res
    @property
    def path_data(self): return os.path.join(self.path,'data')
    @property
    def path_matches(self): return os.path.join(self.path_data,'rels')





    @property
    def xml2txt_func(self): self.XML2TXT.__func__
    def xml2txt(self,*x,**y): return self.XML2TXT.__func__(*x,**y)







    ####################################################################
    # DB
    ####################################################################

    def db(self):
        return DB(
            os.path.splitext(self.path_metadata)[0],
            force=False,
            engine=PATH_LLTK_DB_ENGINE
        )















    ####################################################################
    # ADDRS
    ####################################################################

    # Corpus addr
    @property
    def addr(self): return IDSEP_START+self.id
    

    # Text addrs
    def get_addr(self,id):
        return IDSEP_START + self.id + IDSEP + id
    
    @property
    def addrs(self): return self.addrs_all()

    def addrs_db(self,force=True):
        with self.db() as db:
            o={self.get_addr(id) for id in db.keys()}
        if log.verbose>1: log(f'-> {o}')
        return o

    def addrs_matchdb(self,force=True):
        o={
            addr
            for addr in self.matcher
            if addr.startswith(IDSEP_START+self.id+IDSEP)
        }
        if log.verbose>1: log(f'-> {o}')
        return o

    def addrs_textd(self,force=True):
        o={self.get_addr(id) for id in self.textd.keys()}
        if log.verbose>1: log(f'-> {o}')
        return o

    def addrs_all(self, db=True, matchdb=True, textd=True, force=True):
        o=set()
        if db: o|=set(self.addrs_db(force=force))
        if matchdb: o|=set(self.addrs_matchdb(force=force))
        if textd: o|=set(self.addrs_textd(force=force))
        return o


















    ####################################################################
    # Clearing DB
    ####################################################################
    
    def clear_db(self,keys=None):
        self.db().drop()
    
        # if keys is None: keys=self.addrs_db(force=True)
        # if len(keys):
        #     if log.verbose>0: log(f'[{self.id}] Deleting {len(keys)} db entries')
        #     with self.db() as db:
        #         for key in keys:
        #             del db[key]
    
    def clear_matches(self,keys=None):
        if keys is None: keys=self.addrs_matchdb()
        if len(keys):
            if log.verbose>0: log(f'[{self.id}] Deleting {len(keys)} matchdb entries')
            self.matcher.remove_nodes(keys)

    def clear(self,db=True,files=False,matches=True,**kwargs):
        if db: self.clear_db()
        if matches: self.clear_matches()
        if files: pass # @TODO
        self._metadf=None
        self._texts=None
        self._textd=defaultdict(lambda: None)
        self._dtmd={}
        self._mfwd={}
        self._init=False
        self._source=None

    def clear_files(self): 
        # @TODO
        pass









    ####################################################################
    # Matcher
    ####################################################################

    @property
    def matcher(self):
        if self._matcher is None:
            from lltk.model.matcher import Matcher
            self._matcher=Matcher(self)
        return self._matcher
    
    @property
    def matcher_global(self):
        if self._matcher_global is None:
            from lltk.model.matcher import Matcher
            self._matcher_global=Matcher()
        return self._matcher_global


    def match(self,corpus,**kwargs):
        return self.matcher.find_matches(self,corpus,**kwargs)













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
            _verbose: int = 1,
            **_params_or_meta) -> BaseText:
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
            Whether to initialize corpus if not done so yet. Default: True.

        _verbose : int, optional
            Verbosity level, by default 1


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
        params,meta = to_params_meta(_params_or_meta)
        if log.verbose>0:  log(f'<- {get_imsg(id,self,_source,**meta)}')

        # Init corpus?
        if _init: self.init()

        # Defaults
        tocache=False
        t = None

        # Get ID immediately
        id1 = id
        # log(f'id <- {id}')
        kwargs=merge_dict(meta,dict(corpus=self,source=_source))
        #id = get_id_str(id,**kwargs)
        ikwargs=merge_dict(
            dict(
                id=id1,
                i=len(self._textd),
                allow='_/.-:,=',
                prefstr='T',
                numzero=5,
                use_meta=True,
                force_meta=True,
            ),
            meta
        )

        id=get_idx(**ikwargs)
        if log.verbose>1: log(f'id = {id}')
        
        newsep='_'
        while _new and id in self._textd:
            if log.verbose>0: log(f'{id} already in {self.id}._textd and {_new} set')
            idsuf=id.split(newsep)[-1]
            if idsuf.isdigit():
                isuf=int(idsuf)+1
                id=f'{newsep.join(id.split(newsep)[:-1])}{newsep}{isuf}'
            else:
                isuf=2
                id=f'{id}{newsep}{isuf}'
            
            if log.verbose>0: log(f'new id set: {id}')


        # get?
        if not _force and id is not None:
            t = self.get_text(id)
        
        # Create?
        if _force or t is None:
            t = self.init_text(id,_source=_source,**meta)
            tocache = True
        
        elif meta and is_text_obj(t):
            if log.verbose>0: log(pf(f'Updating text metadata:',meta))
            t.update(meta,_cache=False)
            tocache = True
        
        # Fail?
        if t is None: raise CorpusTextException('Could not get or create text')
        
        # Add to my own dictionary?
        if _add: self.add_text(t)
        
        # Cache?
        if _cache and tocache: t.cache()
        
        # Return text
        if log.verbose>0: log(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t

    
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

        if log.verbose>1: log(f'<- id = {id}')
        t=self._textd.get(id)
        if log.verbose>1: log(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t
        
    def remove_text(self,id,db=True,matches=True):
        if id in self._textd:
            if log.verbose>0: log(f'removing {id} from {self.id}._textd')
            del self._textd[id]
        if db: self.db().delete(id)
        if matches: self.matcher.remove_node(self.get_addr(id))

    
    def init_text(self,id=None,_source=None,_cache=False,_use_db=True,**kwargs):
        # log('...')
        params,meta = to_params_meta(kwargs)
        if log.verbose>0:  log(f'<- {get_imsg(id,self,_source,**meta)}')

        if is_text_obj(_source): 
            if log.verbose>1: log(f'Source is text: {_source}')
            if id is None:
                id=_source.addr
                if log.verbose>1: log(f'Source is set, but I have no ID. Setting ID to source addr: {id}')

        elif is_text_obj(id):
            if log.verbose>1: log(f'"{id}" is already a text')
            _source = id
            id = _source.addr

        elif type(id)==str:
            ## get source?
            id_corpus,id_text = to_corpus_and_id(id)
            if id_corpus and id_corpus!=self.id:
                if log.verbose>0: log(f'hidden source -> {id}')
                if log.verbose>1: log(f'source = Corpus( {id_corpus} ).text( {id_text} ) ...')
                _source = Corpus(id_corpus).text(id_text)
                if log.verbose>0: log(f'hidden source <- {_source}')
        else:
            id = get_idx(i=len(self._textd), **kwargs)
            if log.verbose>1: log(f'id auto set to {id}')

        # gen text in my image        
        # log(kwargs)
        t = self.TEXT_CLASS(
            id=id,
            _corpus=self,
            _source=_source,
            **kwargs
        )
        if log.verbose>0: log(f'-> {t}' if is_text_obj(t) else "-> ?")
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
            iterr=get_tqdm(o,desc=f'[{self.name}] Iterating texts')
        else:
            iterr=o
        for t in iterr:
            # if progress: iterr.set_description(f'[{self.name}] Iterating texts: {t.id}')
            if progress: iterr.set_description(f'{t}')
            yield t
    
    def corpus_texts(self,*args,**kwargs): yield from self.texts(*args,**kwargs)
    
    # Convenience
    @property
    def num_texts(self): return len(self._textd)
    @property
    def text_ids(self): return list(self._textd.keys())












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

        
    def init_meta_json(self,force=False,bad_cols=BAD_COLS,meta_fn='meta.json',**kwargs):
        # log(f'Initializing from json files: {self}')
        if log.verbose>0: log(self)
        for root,dirs,fns in os.walk(self.path_texts):
            if meta_fn in set(fns):
                meta_root=os.path.abspath(root)
                meta_fnfn=os.path.join(root,meta_fn)
                idx = meta_root.replace(self.path_texts,'')
                idx = idx[1:] if idx.startswith('/') else idx
                yield idx, read_json(meta_fnfn)

    def init_meta_csv(self,*x,**y):
        if log.verbose>0: log(self)
        if not os.path.exists(self.path_metadata): self.install_metadata()
        if os.path.exists(self.path_metadata):
            df=read_df_anno(self.path_metadata,dtype=str)
            if type(df)==pd.DataFrame and len(df) and self.col_id in set(df.columns):
                df=df.set_index(self.col_id)
                o1=df.index
                o2=df.to_dict('records')
                yield from zip(o1,o2)


    def init_meta(self,sources=['csv'],merger=merge_dict,allow_hidden=False,*x,**y):
        if log.verbose>0: log(self)
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
            odx=merger(*ld)
            if not allow_hidden: odx={k:v for k,v in odx.items() if k and k[0]!='_'}
            yield id, odx
            

    def iter_init(self,progress=False,add=True,cache=True,force=False,**kwargs):
        if log.verbose>0: log(self)
        iterr=self.init_meta(**kwargs)
        if progress: iterr=get_tqdm(list(iterr), desc=f'[{self.name}] Iterating texts')
        for id,d in iterr:
            # init text object from meta
            od={k:v for k,v in d.items() if k not in {'id','add','cache'}}
            t = self.text(id=id,_add=add,_cache=False,_force=force,_init=False,**od)
            yield t
        
    def init(self,force=False,quiet=None,**kwargs):
        if not force and self._init: return
        self._init=True
        if log.verbose>0: log(self)
        # stop
        # log(self)
        def do_it():
            i=0
            for x in self.iter_init(**kwargs): i+=1
            return i
        
        if quiet is True or self._quiet is True:
            with log.hidden():
                numdone = do_it()
        else:
            numdone = do_it()

        if numdone and log.verbose>0: log(f'initialized {numdone} texts')



    def metadata(
            self,
            force=False,
            # force_save=True,
            progress=True,
            lim=None,
            fillna='',
            from_cache=True,
            from_sources=True,
            cache=False,
            remote=False,
            sep='__',
            meta={},
            **kwargs):
        
        # self.init()
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

    def save_metadata(self,ometa=None,*x,force=True,force_inner=False,ok_cols=set(),backup=True,**kwargs):
        if ometa is None: ometa=self.metadata(force=force_inner,**kwargs)
        if ometa is not None and len(ometa):
            if not ok_cols: 
                for t in self.texts(progress=False): ok_cols|=set(t._meta.keys())
                ometa = ometa[[col for col in ometa.columns if col in ok_cols]]
            if force or not os.path.exists(self.path_metadata):
                ofn = self.path_metadata
                if backup:
                    backup_save_df(ometa,ofn,**kwargs)
                save_df(ometa,ofn,**kwargs)

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




    def get_text_cache(self,id,**kwargs):
        return self.get_text_cache_json(id,**kwargs)
    def get_text_cache_json(self,id,meta_fn='meta.json'):
        fn=os.path.join(self.path_texts,id,meta_fn)
        return read_json(fn)
        







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

    # def preprocess_txt(self,force=False,num_proc=DEFAULT_NUM_PROC,verbose=True,**attrs): #force=False,slingshot=False,slingshot_n=1,slingshot_opts=''):
    #     if not self._texts: return
    #     paths = [(t.path_xml,t.path_txt) for t in self.texts() if t.path_xml and os.path.exists(t.path_xml)]
    #     if not paths:
    #         if log.verbose>0: self.log('No XML files to produce plain text files from')
    #         return
    #     objs = [
    #         (pxml,ptxt,self.XML2TXT.__func__)
    #         for pxml,ptxt in paths
    #         if force or not os.path.exists(ptxt)
    #     ]
    #     if not objs:
    #         if log.verbose>0: self.log('Plain text files already saved')
    #         return
    #     tools.pmap(
    #         do_preprocess_txt,
    #         objs,
    #         num_proc=num_proc,
    #         desc=f'[{self.name}] Saving plain text versions of XML files',
    #         **attrs
    #     )

    # def preprocess_freqs(self,force=False,kwargs={},verbose=True,**attrs): #force=False,slingshot=False,slingshot_n=1,slingshot_opts=''):
    #     objs = [
    #         (t.path_txt,t.path_freqs,self.TOKENIZER.__func__)
    #         for t in self.texts()
    #         if os.path.exists(t.path_txt) and (force or not os.path.exists(t.path_freqs))
    #     ]
    #     if not objs:
    #         if log.verbose>0:
    #             self.log('Word freqs already saved')
    #         return
    #     # print('parallel',parallel)
    #     tools.pmap(
    #         save_freqs_json,
    #         objs,
    #         kwargs=kwargs,
    #         desc=f'[{self.name}] Saving word freqs as jsons',
    #         **attrs
    #     )






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
                # print('orig paths',len(paths),list(paths)[:5])
                # print('ok paths',len(acceptable_paths), list(acceptable_paths)[:5])
                paths = list(set(paths) & acceptable_paths)
            except Exception:
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
                #print(type(paths),paths[:3])
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
        #print('?',zipdir,os.listdir('.'))


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
        # print(f'Executing:\n{cmdstr}')

        for cmd in self.get_tqdm(cmds,desc=f'[{self.name}] Uploading zip files'):
            os.system(cmd)

        os.chdir(here)

    def share(self,cmd_share='dbu share',dest=DEST_LLTK_CORPORA):
        ol=[]
        import subprocess
        ln='['+self.name+']'
        print(ln)
        ol+=[ln]
        for part in ZIP_PART_DEFAULTS:
            fnzip = self.id+'_'+part+'.zip'
            cmd=cmd_share+' '+os.path.join(dest,fnzip)
            try:
                out=str(subprocess.check_output(cmd.split()))
            except (subprocess.CalledProcessError,ValueError,TypeError) as e:
                #print('!!',e)
                continue
            link=out.strip().replace('\n','').split('http')[-1].split('?')[0]
            if link: link='http'+link+'?dl=1'

            url='url_'+part+' = '+link
            print(url)
            ol+=[url]
        print()
        #return '\n'.join(ol)

    def get_tqdm(self,*x,desc='',**y):
        if desc: desc=f'[{self.id}] {desc}'
        return get_tqdm(*x,desc=desc,**y)


    def mkdir_root(self):
        if not os.path.exists(self.path_root): os.makedirs(self.path_root)


    def urls(self):
        urls=[(x[4:], getattr(self,x)) for x in dir(self) if x.startswith('url_') and getattr(self,x)]
        return urls

    def compile_db(self,ld):
        if log.verbose>1: log('compiling database')
        with self.db() as db:
            for d in self.get_tqdm(ld,desc='Adding to database'):
                id = d.get(self.col_id)
                if id: db[self.col_id]=d
        if log.verbose>1: log('finished compiling database')
        
        

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
        print('\n'.join(ol))

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

    def install(self, ask=True, urls={}, force=False, part=None, flatten=False, parts=None, unzip=True, **attrs):
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
            if log.verbose>0: log(pf(part,fname,getattr(self,fname)))
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
        # print(part,ppart,path,os.path.exists(path))

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






    ### FREQS


    def dtm(self,words=[],texts=None,n=DEFAULT_MFW_N,tf=False,tfidf=False,meta=False,na=0.0,sort_cols_by='sum',**mfw_attrs):
        dtm=self.preprocess_dtm(texts=texts, words=words,n=n,**mfw_attrs)
        dtm=dtm.set_index(self.col_id) if self.col_id in set(dtm.columns) else dtm
        if texts is not None:
            dtm=dtm.loc[[w for w in to_textids(texts) if w in set(dtm.index)]]
        dtm = dtm.reindex(dtm.agg(sort_cols_by).sort_values(ascending=False).index, axis=1)

        if tf: dtm=to_tf(dtm)
        if tfidf: dtm=to_tfidf(dtm)
        if meta:
            if type(meta) in {list,set}:
                if not self.col_id in meta: meta=list(meta)+[self.col_id]
            mdf=self.metadf[meta] if type(meta) in {list,set} else self.metadf
            mdtm=mdf.merge(dtm,on=self.col_id,suffixes=('','_w'),how='right')
            micols = mdf.columns
            dtm=mdtm.set_index(list(micols))
        # odtm=dtm.fillna(na) 
        # odtm=odtm.reset_index().drop_duplicates()
        # indexby='id' if not meta else meta+['id']
        return dtm.sort_index()

    def mdw(self,groupby,words=[],dtm=None,texts=None,tfidf=True,keep_null_cols=False,remove_zeros=True,agg='median',num_proc=DEFAULT_NUM_PROC, **mfw_attrs):
        texts=self.metadf[self.metadf.id.isin(to_textids(texts))] if texts is not None else self.metadf
        if not keep_null_cols: 
            texts=texts.loc[[bool(x) for x in texts[groupby]]]

        if dtm is None:
            dtm=self.dtm(
                words=words,
                texts=texts,
                tfidf=tfidf,
                meta=[groupby] if type(groupby)==str else groupby,
                num_proc=num_proc,
                **mfw_attrs
            )

        mdw=to_mdw_mannwhitney(dtm, groupby, num_proc=num_proc)
        mdw['method']='mannwhitney'
        return mdw


    # @property
    # def path_mfw(self):
    # 	if not os.path.exists(self.path_data): os.makedirs(self.path_data)
    # 	return os.path.join(self.path_data, 'mfw.h5')
    # @property
    # def path_dtm(self):
    # 	if not os.path.exists(self.path_data): os.makedirs(self.path_data)
    # 	return os.path.join(self.path_data, 'dtm.h5')
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
    # @property
    # def path(self): return self.path_root


    #########
    # MFW
    #########

    # pos?



    def mfw_df(self,
            n=None,
            words=None,
            texts=None,
            yearbin=None,
            by_ntext=False,
            n_by_period=None,
            keep_periods=True,
            n_agg='median',
            min_count=None,
            col_group='period',
            excl_stopwords=False,
            excl_top=0,
            valtype='fpm',
            min_periods=None,
            by_fpm=False,
            only_pos=set(),
            force=False,
            keep_pos=None,
            **attrs):

        # gen if not there?
        if yearbin is None: yearbin=self.mfw_yearbin
        if n is None: n=self.mfw_n
        if type(yearbin)==str: yearbin=int(yearbin)
        df = self.preprocess_mfw(
            n=n,
            words=words,
            texts=texts,
            yearbin=yearbin,
            by_ntext=by_ntext,
            by_fpm=by_fpm,
            col_group=col_group,
            force=False,
            try_create_freqs=True,
            **attrs
        )
        if df is None:
            self.log('Could not load or generate MFW')
            return
        df=df.fillna(0)

        if words is not None: df=df[df.word.isin(words)]

        if excl_top:
            df=df[df['rank']>=excl_top]

        if excl_stopwords:
            stopwords=get_stopwords()
            df=df[~df.word.isin(stopwords)]

        if min_count: df = df[df['count']>=min_count]

        if min_periods and min_periods>1:
            num_periods_per_word = df[df['count']>0].groupby('word').size()
            if num_periods_per_word.max()>1:
                ok_words=set(num_periods_per_word[num_periods_per_word>=min_periods].index)
                df=df[df.word.isin(ok_words)]
        if only_pos:
            w2p=get_word2pos(lang=self.lang)
            def word_ok(w):
                wpos=w2p.get(w.lower())
                if not wpos: return False
                for posx in only_pos:
                    # print(posx,wpos,only_pos)
                    if posx==wpos: return True
                    if posx.endswith('*') and wpos.startswith(posx[:-1]): return True
                return False
            df=df[df.word.apply(word_ok)]
            # df['pos']=df.word.replace(w2p)
            # df['pos0']=df.pos.apply(lambda x: x[0] if x else '')

        ## now pivot
        dfi=df.reset_index()
        has_period = col_group in dfi.columns
        if n: # limit to overall
            if not n_by_period:
                if has_period:
                    dfpiv = dfi.pivot(col_group,'word',valtype).fillna(0)
                    # print(dfpiv.median().sort_values(ascending=False))
                    topNwords=list(dfpiv.median().sort_values(ascending=False).iloc[:int(n)].index) #iloc[excl_top:excl_top+n]
                else:
                    topNwords=dfi.sort_values(valtype,ascending=False).iloc[:int(n)].word
                df=df[df.word.isin(set(topNwords))]
            elif has_period:
                df = df.dropna().sort_values('rank').groupby(col_group).head(n)

        if not keep_periods or not has_period:
            # agg by word
            df=df.groupby('word').agg(n_agg)
            df['ranks_avg']=df['rank']
            df=df.sort_values(valtype,ascending=False)
            df['rank']=[i+1 for i in range(len(df))]

        # dfr=df.reset_index()
        # odf = odf[odf.columns[1:] if set(odf.columns) and odf.columns[0]=='index' else odf.columns]
        dfr=df#.reset_index() if not 'id' in df

        ## add back pos?
        if keep_pos is not False:
            w2pdf=get_word2pos_df(lang=self.lang)
            w2pdf['pos0']=w2pdf['pos'].apply(lambda x: x[0] if type(x)==str and x else '')
            # w2pdf['pos'],w2pdf['pos0']=w2pdf['pos0'],w2pdf['pos']
            dfr=dfr.merge(w2pdf,on='word')


        if col_group in dfr and set(dfr[col_group])=={EMPTY_GROUP}:
            dfr=dfr.drop(col_group,1)

        return dfr



    def mfw(self,*x,**y):
        y['keep_periods']=False
        df=self.mfw_df(*x,**y)
        try:
            return list(df.reset_index().word)
        except Exception:
            return []



    def to_texts(self,texts):
        if issubclass(texts.__class__, pd.DataFrame) and self.col_id in set(texts.reset_index().columns):
            return [self.textd[idx] for idx in texts.reset_index()[self.col_id]]
        # print(type(texts), texts)
        return [
            x if (Text in x.__class__.mro()) else self.textd[x]
            for x in texts
        ]
    
    def to_textdf(self,texts):
        if issubclass(texts.__class__, pd.DataFrame): return texts.reset_index()
        
        # is objects?
        meta=self.meta.reset_index()
        meta['corpus_id']=list(zip(meta.corpus,meta.id))
        ok_corpus_ids=[(t.corpus.name, t.id) for t in texts]
        return meta[meta.corpus_id.isin(ok_corpus_ids)]
    
    @property
    def textdf(self): return self.meta.reset_index()
    

    def preprocess_mfw(self,
            texts=None,
            words=None,
            yearbin=None,
            yearbin_posthoc=None, # applied at end
            year_min=None,
            year_max=None,
            by_ntext=False,
            by_fpm=False,
            num_proc=DEFAULT_NUM_PROC,
            col_group='period',
            estimate=True,
            force=False,
            verbose=False,
            try_create_freqs=True,
            progress=True,
            pos=set(),
            **attrs
        ):
        """
        From tokenize_texts()
        """
        if yearbin is None: yearbin=self.mfw_yearbin
        if type(yearbin)==str: yearbin=int(yearbin)
        textdf=self.textdf if texts is None else self.to_textdf(texts)
        if (year_min or year_max) and 'year' in set(textdf.columns):
            textdf=textdf.query(f'{year_min if year_min is not None else -1000000} <= year < {year_max if year_max is not None else -1000000}')
        key=self.mfwkey(yearbin,by_ntext,by_fpm,textdf.id)
        # keyfn=os.path.join(self.path_mfw,key+'.ft')
        keyfn=os.path.join(self.path_mfw,key+'.pkl')

        if key in self._mfwd and self._mfwd[key] is not None:
            return self._mfwd[key]

        kwargs={
            'by_ntext':by_ntext,
            'estimate':estimate,
            'by_fpm':by_fpm,
            'progress':progress,
            'desc':f'[{self.name}] Counting overall most frequent words (MFW)',
            'num_proc':num_proc if yearbin is False else 1
        }
        odf=None
        if not force and os.path.exists(keyfn):
            # if log.verbose>0: self.log(f'MFW is cached for key {key}')
            if log.verbose>0: self.log(f'Loading MFW from {ppath(keyfn)}')
            odf=read_df(keyfn)
            self._mfwd[key]=odf
            return odf


        if yearbin and not {'year','path_freqs'}-set(textdf.columns):
            textdf[col_group]=textdf.year.apply(lambda y: to_yearbin(y,yearbin))
            textdf[col_group+'_int']=textdf[col_group].apply(lambda y: int(y[:4]))
            
            if not len(textdf.path_freqs):
                if log.verbose>0: self.log('No freqs files found to generate MFW')
                if try_create_freqs:
                    self.preprocess_freqs()
                    return self.preprocess_mfw(
                        yearbin=yearbin,
                        year_min=year_min,
                        year_max=year_max,
                        by_ntext=by_ntext,
                        by_fpm=by_fpm,
                        num_proc=num_proc,
                        col_group=col_group,
                        # n=n,
                        progress=progress,
                        estimate=estimate,
                        force=force,
                        verbose=verbose,
                        try_create_freqs=False
                    )
                return pd.DataFrame()

            # run
            pathdf=textdf[[col_group,'path_freqs']]
            
            kwargs['progress']=False
            odf = pmap_groups(
                do_gen_mfw_grp,
                pathdf.groupby(col_group),
                num_proc=num_proc,
                kwargs=kwargs,
                desc=f'[{self.name}] Counting most frequent words across {yearbin}-year periods',
                progress=progress#yearbin is not False
            )
            if odf is None or not len(odf): return pd.DataFrame()
            odf=odf.reset_index().sort_values(['period','rank'])
        # no period
        elif 'path_freqs' in set(textdf.columns):
            pathdf=textdf[['path_freqs']]
            odf=do_gen_mfw_grp(pathdf,progress=progress,num_proc=num_proc)

        if odf is not None:
            if log.verbose>0: self.log(f'Saving MFW to {ppath(keyfn)}')
            save_df(odf, keyfn, verbose=False)

        self._mfwd[key]=odf
        return self._mfwd[key]



    ###################################
    # DTM
    ###################################

    def mfwkey(self,yearbin,by_ntext,by_fpm,text_ids):
        if type(yearbin)==str: yearbin=int(yearbin)
        tids=tuple(sorted([x for x in text_ids if x is not None]))
        return hashstr(str((yearbin, int(by_ntext), int(by_fpm), tids)))[:12]

    def wordkey(self,words):
        return hashstr('|'.join(sorted(list(words))))[:12]
    def preprocess_dtm(
            self,
            texts=None,
            words=[],
            n=DEFAULT_MFW_N,
            num_proc=DEFAULT_NUM_PROC,
            wordkey=None,
            sort_cols_by='sum',
            force=False,
            verbose=False,
            year_min=None,
            year_max=None,
            **attrs
        ):


        if not words: words=self.mfw(texts=texts,n=n,num_proc=num_proc,force=force,**attrs)
        wordset = set(words)
        # print(len(wordset))
        if not wordkey: wordkey=self.wordkey(words)
        if wordkey in self._dtmd: return self._dtmd[wordkey]
        # keyfn=os.path.join(self.path_dtm,wordkey+'.ft')
        keyfn=os.path.join(self.path_dtm,wordkey+'.pkl')
        if not force:
            if os.path.exists(keyfn):
                # if log.verbose>0: self.log(f'DTM already saved for key {key}')
                if log.verbose>0: self.log(f'Loading DTM from {ppath(keyfn)}')
                df=read_df(keyfn)
                self._dtmd[wordkey]=df
                if log.verbose>0: self.log(f'Returning DTM from {ppath(keyfn)}')
                return df

        # get
        texts=self.texts() if texts is None else self.to_texts(texts)
        texts=[t for t in texts if (not year_min or t.year>=year_min) and (not year_max or t.year<year_max)]



        objs = [
            (t.path_freqs,wordset,{self.col_id:t.id})
            for t in texts
            if os.path.exists(t.path_freqs) and len(wordset) and t.id
        ]

        if not objs:
            if log.verbose>0: self.log(f'No frequency files found to generate DTM. Run preprocess_freqs()?')
            return

        ld = pmap(
            get_dtm_freqs,
            objs,
            num_proc=num_proc,
            desc=f'[{self.name}] Assembling document-term matrix (DTM)',
            progress=attrs.get('progress',True)
        )

        # return
        dtm = pd.DataFrame(ld).set_index(self.col_id,drop=True).fillna(0)
        dtm = dtm.reindex(dtm.agg(sort_cols_by).sort_values(ascending=False).index, axis=1)

        # df.to_csv(self.path_dtm)
        # df.reset_index().to_feather(self.path_dtm)
        if log.verbose>0: self.log(f'Saving DTM to {ppath(keyfn)}')
        save_df(dtm.reset_index(), keyfn, verbose=False)
        self._dtmd[wordkey]=dtm
        return dtm
















    ### WORD2VEC
    @property
    def model(self):
        if not hasattr(self,'_model'):
            self._model=gensim.models.Word2Vec.load(self.fnfn_model)
        return self._model

    @property
    def fnfn_skipgrams(self):
        return os.path.join(self.path_skipgrams,'skipgrams.'+self.name+'.txt.gz')

    def word2vec(self,skipgram_n=10,name=None,skipgram_fn=None):
        if not name: name=self.name
        from lltk.model.word2vec import Word2Vec
        if skipgram_fn and not type(skipgram_fn) in [six.text_type,str]:
            skipgram_fn=self.fnfn_skipgrams

        return Word2Vec(corpus=self, skipgram_n=skipgram_n, name=name, skipgram_fn=skipgram_fn)

    def doc2vec(self,skipgram_n=5,name=None,skipgram_fn=None):
        if not name: name=self.name
        from lltk.model.word2vec import Doc2Vec
        if not skipgram_fn or not type(skipgram_fn) in [six.text_type,str]:
            skipgram_fn=os.path.join(self.path_skipgrams,'sentences.'+self.name+'.txt.gz')

        return Doc2Vec(corpus=self, skipgram_n=skipgram_n, name=name, skipgram_fn=skipgram_fn)

    def word2vec_by_period(self,bin_years_by=None,word_size=None,skipgram_n=10, year_min=None, year_max=None):
        """NEW word2vec_by_period using skipgram txt files
        DOES NOT YET IMPLEMENT word_size!!!
        """
        from lltk.model.word2vec import Word2Vec
        from lltk.model.word2vecs import Word2Vecs

        if not year_min: year_min=self.year_start
        if not year_max: year_max=self.year_end

        path_model = self.path_model
        model_fns = os.listdir(path_model)
        model_fns2=[]
        periods=[]

        for mfn in model_fns:
            if not (mfn.endswith('.txt') or mfn.endswith('.txt.gz')) or '.vocab.' in mfn: continue
            mfn_l = mfn.split('.')
            period_l = [mfn_x for mfn_x in mfn_l if mfn_x.split('-')[0].isdigit()]
            if not period_l: continue

            period = period_l[0]
            period_start,period_end=period.split('-') if '-' in period else (period_l[0],period_l[0])
            period_start=int(period_start)
            period_end=int(period_end)+1
            if period_start<year_min: continue
            if period_end>year_max: continue
            if bin_years_by and period_end-period_start!=bin_years_by: continue
            model_fns2+=[mfn]
            periods+=[period]

        #print '>> loading:', sorted(model_fns2)
        #return

        name_l=[self.name, 'by_period', str(bin_years_by)+'years']
        if word_size: name_l+=[str(word_size / 1000000)+'Mwords']
        w2vs_name = '.'.join(name_l)
        W2Vs=Word2Vecs(corpus=self, fns=model_fns2, periods=periods, skipgram_n=skipgram_n, name=w2vs_name)
        return W2Vs






def meta_load_metadata(C):
    odf=C.load_metadata()
    odf['corpus']=C.name
    return odf
    
#     # meta=C.meta
#     if not os.path.exists(C.path_metadata): return pd.DataFrame()
#     meta=read_df(C.path_metadata)
#     meta['corpus']=C.name
#     if not 'id' in meta.columns: meta=meta.reset_index()
#     return meta



class MetaCorpus0(BaseCorpus):
    def __init__(self,corpora,**attrs):
        super().__init__(**attrs)
        self.corpora=[]
        actual_cnames = set(corpus_names()) | set(corpus_ids())
        for cname in corpora:
            if not cname in actual_cnames: continue
            self.corpora+=[load_corpus(cname)]
            
    def to_texts(self,texts):
        if issubclass(texts.__class__, pd.DataFrame):
            textdf=texts.reset_index()
            tcols=set(textdf.columns)
            if self.col_id in tcols and self.col_corpus in tcols:
                return [
                    self.textd.get((c,i))
                    for c,i in zip(textdf[self.col_corpus], textdf[self.col_id])
                ]
        else:
            return [
                x if (Text in x.__class__.mro()) else self.textd[x]
                for x in texts
            ]

    

    def load_metadata(self,*args,**attrs):
        """
        Magic attribute loading metadata, and doing any last minute customizing
        """
        if self._metadf is None:
            self._metadf=pd.concat(
                pmap(
                    meta_load_metadata,
                    self.corpora,
                    num_proc=DEFAULT_NUM_PROC,
                    desc='Loading all subcorpora metadata'
                )
            ).reset_index().set_index(['corpus','id'])
        return self._metadf


    @property
    def textd(self):
        if self._textd is None or not len(self._textd):
            self._textd={}
            for C in self.get_tqdm(self.corpora,desc='Assembling dictionary of text objects'):
                for t in C.texts():
                    self._textd[(C.name, t.id)]=t
        return self._textd
            
    def meta_iter(self,progress=False):
        iterr=get_tqdm(self.corpora) if progress else self.corpora
        for C in iterr:
            iterr.set_description(f'[{C.name}] Iterating through metadata')
            for dx in C.meta_iter(progress=False):
                dx['corpus']=C.name
                yield dx
                
    def mfw_df(self,texts=None,keep_corpora=False,**attrs):
        o=[]
        for C in self.corpora:
            if type(texts)==pd.DataFrame and self.col_corpus in set(texts.columns):
                Ctexts=texts[texts.corpus.isin({C.id,C.name})]
            else:
                Ctexts=texts
            Cmfwdf=C.mfw_df(texts=Ctexts,**attrs)
            Cmfwdf['corpus']=C.name
            o+=[Cmfwdf]
        mfw_df=pd.concat(o)
        
        # filter
        if not keep_corpora:
            aggqualcols=['word','pos','pos0']
            if 'period' in set(mfw_df.columns): aggqualcols.insert(0,'period')
            mfw_df=mfw_df.groupby(aggqualcols).agg(dict(
                count=sum,
                fpm=np.mean,
                rank=np.median,
            )).reset_index()
        return mfw_df
            


    def texts(self):
        return [t for C in self.corpora for t in C.texts()]





class SectionCorpus(BaseCorpus):
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
        if log.verbose>0: log(f'<- id = {corpus}')
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

    if logg and log.verbose>0: log(f'-> {C}')
    return C




