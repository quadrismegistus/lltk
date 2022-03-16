from lltk.imports import *


def fixpath(path):
    if type(path)==str and path and not os.path.isabs(path):
        if '~' in path:
            path=path.split('~')[-1]
            path=os.path.join(os.path.expanduser('~'), path[1:])
        path=os.path.abspath(path)
    return path

def unrelfile(path):
    if type(path)==str and path and not os.path.isabs(path):
        path = path.replace('~', os.path.expanduser('~'))
        path=path.replace(os.path.sep + os.path.sep, os.path.sep)
    elif not path:
        path=''
    return path




class BaseCorpus(object):
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
    XML2TXT = default_xml2txt
    

    def __init__(self,
            id=None,
            _name=None,
            _id_allows='_',
            **attrs):

        self.id=id
        self._metadf=None
        self._texts=None
        self._textd=defaultdict(lambda: None)
        self._dtmd={}
        self._mfwd={}
        self._init=False
        self._source=None
        self.name=_name

        ## set attrs
        attrs={**MANIFEST_DEFAULTS, **attrs}

        for k,v in attrs.items():
            if k.startswith('path_'): k='_'+k
            setattr(self,k,v)

        # make sure we have a name
        if self.id: self.id=get_idx(self.id,allow=_id_allows)
        if not self.id and type(id)==str and id: self.id=ensure_snake(id)
        if not self.id and self.ID: self.id=self.ID
        if not self.id and self.name: self.id=get_idx(self.name)
        if not self.id: self.id=TMP_CORPUS_ID

        if not self.name and self.NAME: self.name=self.NAME
        if not self.name and self.id: self.name=snake2camel(self.id)
        if not self.name: self.name=self.__class__.__name__
        if self.name: self.name=to_camel_case(self.name)
    
    def __getattr__(self, name):
        if name.startswith('path_'): return self.get_path(name)

        res = get_from_attrs_if_not_none(self,name)
        if res is None and self._source is not None:
            res = get_from_attrs_if_not_none(self._source, name)
        return res

    def get_path(self,name):
        path = get_from_attrs_if_not_none(self, name)
        if path is None:
            path = get_from_attrs_if_not_none(self, '_'+name)
        if path is not None and not os.path.isabs(path):
            if '~' in path:
                path=path.split('~')[-1]
                path=os.path.join(os.path.expanduser('~'), path[1:])
            else:
                path=os.path.join(self.path, path)
        return path


    @property
    def path(self):
        res=get_from_attrs_if_not_none(self,'_path')
        if res is None: res=os.path.join(PATH_CORPUS,self.id)
        return res

    def __repr__(self):
        return f'[{self.__class__.__name__}]({self.id})'



    def __iter__(self):
        self._iter_i=-1
        self._texts=self.texts()
        return self
    def __next__(self):
        self._iter_i+=1
        if self._iter_i<len(self._texts):
            return self._texts[self._iter_i]
        else:
            raise StopIteration


    # get/make/add
    def text(self,id=None,init=True,add=True,**kwargs):
        id=get_idx(id)
        obj=self.get_text(id)
        if obj is None and init:
            obj=self.init_text(id=id,**kwargs)
        if obj is not None and add:
            self.add_text(obj)
        return obj
    

    def get_text(self,id=None,**kwargs):
        # try to return
        return self.textd[id]

    
    # def init_text(self,id=None,corpus=None,meta={},**kwargs):
    def init_text(self,id=None,**kwargs):
        # log.debug(f'{self}: Initializing text with id="{id}"')
        if id is None: id=get_idx()
        ## get source?
        id_corpus,id_text = to_corpus_and_id(id)
        
        source_text = None
        if id_corpus:
            if id_corpus==self.id:
                # can't be my own source
                id=id_text
            else:
                # log.debug(f'Need to get new corpus: {id_corpus}')
                # get the referenced text
                source_corpus = Corpus(id_corpus)
                # assign as source
                source_text = source_corpus.text(id_text)
        
        ## get text
        text = self.TEXT_CLASS(
            id,
            _corpus = self,
            **kwargs
        )
        text._source=source_text
        
        # log.debug(f'{self}: Returning text: {text}')
        return text
    
    def add_text(self,t,**kwargs):
        # ensure ours
        # if t.__class__!=self.TEXT_CLASS:
            #log.error(f'class of text {t.__class__} is not our own text class {self.TEXT_CLASS}!')
            #t=self.init_text(t,**kwargs)

        # assign by id
        self._textd[t.id]=t

    
    @property
    def paths(self):
        return self.__paths


    #####
    # Metadata
    ####
    @property
    def t(self):
        return random.choice(self.texts())

    @property
    def au(self): return self.authors

    @property
    def authors(self,authorkey='author',titlekey='title',idkey=COL_ID):
        if not hasattr(self,'_authors') or not self._authors:
            if self._metadf is None or self._textd is None: 
                self.load_metadata()
                return self.authors

            self._authors=au=Bunch()
            # for author,authordf in tqdm(list(self._metadf.reset_index().groupby(authorkey)),desc='Creating author/title shortcut attributes'):
            for author,authordf in self._metadf.reset_index().groupby(authorkey):
                if not author.strip(): continue
                akey=to_authorkey(author)
                aobj=AuthorBunch(corpus=self,name=author)
                setattr(self._authors,akey,aobj)
                for i,row in authordf.iterrows():
                    title=row[titlekey]
                    idx=row[idkey]
                    tkey=to_titlekey(title)
                    tobj=self._textd.get(idx)
                    setattr(aobj,tkey,tobj)
        return self._authors

    def load_metadata_file(self,force=False):
        return read_df(self.path_metadata)

    def init_meta(self,*x,**y):
        meta_files_or_dfs=[self.path_metadata_init,self.path_metadata,self._metadf]
        for mdf_or_fn in meta_files_or_dfs:
            mdf_or_fn_anno = get_anno_fn_if_exists(mdf_or_fn)
            yield from readgen(mdf_or_fn_anno,progress=False)

    # def init(self, meta_final=None, other_meta=[], expand_meta=True, force=False,**kwargs):
    def init(self,force=False,meta_final=[],**kwargs):
        if not force and self._init: return
        log.debug(f'Initializing from metadata: {self}')
        i=0
        for dx in self.init_meta(meta_final):
            if not self.col_id in dx: continue
            # init text from meta
            t = self.init_text(**dx)
            # add text to textd
            self.add_text(t)
            i+=1
        if i: self._init=True
        return




    def load_metadata(self,force=False,progress=True,lim=None,**kwargs):
        if force or self._metadf is None:
            log.debug('Generating metadata dataframe')
            self._metadf = pd.DataFrame(
                t.meta
                for t in tqdm(self.texts()[:lim],disable=not progress,desc='Iterating over texts')
            )
            try:
                self._metadf = self._metadf.set_index(self.col_id).fillna('')
                self.save_metadata(force=force)
            except KeyError:
                log.error(f'ID key not present in metadf: {self.col_id}')
                pass
            log.debug(f'Returning dataframe of {self._metadf.shape} dimensions')
        return self._metadf

    def save_metadata(self,*x,force=False,**y):
        if self._metadf is not None:
            if force or not os.path.exists(self.path_metadata):
                save_df(self._metadf, self.path_metadata)

    # def text(self,idx,meta=None):
    #     if meta is None:
    #         if self._textd is not None and idx in self._textd:
    #             return self._textd[idx]
    #     else:
    #         return self.TEXT_CLASS(idx,self,meta=dict(meta))

    @property
    def meta(self): return self.load_metadata()

    @property
    def textd(self):
        self.init()
        return self._textd
    
    @property
    def metadf(self):
        return self.meta.reset_index() if len(self.meta) else self.meta

    @property
    def metadata(self,*x,**y): return self.meta



    def meta_iter(self,progress=False,**y):
        for dx in readgen(self.path_metadata,progress=False,**y):
            if not 'id' in dx:
                # print(f'!! [{self.name}] Corpus does not have "id" column in {self.path_metadata}')
                continue
            dx['corpus']=self.name
            dx['path_freqs']=os.path.join(self.path_freqs, dx['id'] + self.EXT_FREQS)
            dx['path_txt']=os.path.join(self.path_txt, dx['id'] + self.EXT_TXT)
            dx['path_xml']=os.path.join(self.path_xml, dx['id'] + self.EXT_XML)
            if 'year' in dx and dx['year'] and type(dx['year']) not in {float, np.float}:
                dx['_year_orig']=dx['year']
                dx['year']=pd.to_numeric(dx['year'],errors='coerce')
            yield dx


    # Get texts
    def texts(self,text_ids=None):
        self.init()
        if text_ids is not None:
            text_ids=to_textids(text_ids)
            return [self._textd[tidx] for tidx in text_ids if tidx in self._textd]
        
        return list(self._textd.values())
    
    # Convenience
    @property
    def num_texts(self): return len(self.meta)
    # @property
    # def textd(self): return self._textd if self._textd is not None else {}
    @property
    def text_ids(self): return list(self.meta.id)


    # # Groups?
    # def new_grouping(self):
    # 	return CorpusGroups(self)


    #################
    #### PROCESSING
    #################

    def preprocess_txt(self,force=False,num_proc=DEFAULT_NUM_PROC,verbose=True,**attrs): #force=False,slingshot=False,slingshot_n=1,slingshot_opts=''):
        if not self._texts: return
        paths = [(t.path_xml,t.path_txt) for t in self.texts() if t.path_xml and os.path.exists(t.path_xml)]
        if not paths:
            if verbose: self.log('No XML files to produce plain text files from')
            return
        objs = [
            (pxml,ptxt,self.XML2TXT.__func__)
            for pxml,ptxt in paths
            if force or not os.path.exists(ptxt)
        ]
        if not objs:
            if verbose: self.log('Plain text files already saved')
            return
        tools.pmap(
            do_preprocess_txt,
            objs,
            num_proc=num_proc,
            desc=f'[{self.name}] Saving plain text versions of XML files',
            **attrs
        )

    def preprocess_freqs(self,force=False,kwargs={},verbose=True,**attrs): #force=False,slingshot=False,slingshot_n=1,slingshot_opts=''):
        objs = [
            (t.path_txt,t.path_freqs,self.TOKENIZER.__func__)
            for t in self.texts()
            if os.path.exists(t.path_txt) and (force or not os.path.exists(t.path_freqs))
        ]
        if not objs:
            if verbose:
                self.log('Word freqs already saved')
            return
        # print('parallel',parallel)
        tools.pmap(
            save_freqs_json,
            objs,
            kwargs=kwargs,
            desc=f'[{self.name}] Saving word freqs as jsons',
            **attrs
        )









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
                for ofnfn in tqdm(zipper, total=len(paths), desc=f'[{self.name}] Compressing {fname}'):
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

        for cmd in tqdm(cmds,desc=f'[{self.name}] Uploading zip files'):
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
        print('\n'.join(ol))

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
            tools.extract(tmpfnfn,odir,desc=f'[{self.name}] Unzipping {tmpfn} to {mask_home_dir(odir)}',flatten=flatten)
            #if os.path.exists(self.path_raw) and os.listdir(self.path_raw)==['raw']:
            #	os.rename(os.path.join(self.path_raw,'raw'), self.path_raw)
        return self

    def path_zip(self,part):
        return os.path.join(PATH_CORPUS_ZIP,f'{self.id}_{part}.zip')


    def compile_download(self,unzip=True):
        return self.install(part='raw',unzip=unzip)

    def preprocess(self,parts=PREPROC_CMDS,verbose=True,force=False,num_proc=DEFAULT_NUM_PROC,**attrs):
        if not parts: parts=PREPROC_CMDS
        if type(parts)==str: parts=[p.strip().lower() for p in parts.split(',')]
        for part in parts:
            fname='preprocess_'+part
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
            #if not hasattr(self,'_meta'): self.meta
            self._metad=dict(list(zip(self.text_ids,self.meta)))
        return self._metad

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
        return os.path.join(self.path_home,'texts')
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
            # if verbose: self.log(f'MFW is cached for key {key}')
            if verbose: self.log(f'Loading MFW from {ppath(keyfn)}')
            odf=read_df(keyfn)
            self._mfwd[key]=odf
            return odf


        if yearbin and not {'year','path_freqs'}-set(textdf.columns):
            textdf[col_group]=textdf.year.apply(lambda y: to_yearbin(y,yearbin))
            textdf[col_group+'_int']=textdf[col_group].apply(lambda y: int(y[:4]))
            
            if not len(textdf.path_freqs):
                if verbose: self.log('No freqs files found to generate MFW')
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
            if verbose: self.log(f'Saving MFW to {ppath(keyfn)}')
            save_df(odf, keyfn, verbose=False)

        self._mfwd[key]=odf
        return self._mfwd[key]

    def log(self,*x,**y):
        xstr=' '.join(str(xx) for xx in x)
        import logging
        logging.info(f'[{self.name}] {xstr}')
        # from loguru import logger
        # logger.remove()f
        # logger.add(sys.stderr, format="[LLTK] ({time:HH:mm:ss}) {message}", level="INFO")
        # logger.add(sys.stderr, format="{message} ({time:HH:mm:ss})", level="INFO")
        # logger.add(sys.stderr, format="{message}", filter="lltk", level="INFO")
        # logger.info(f'[{self.name}] {xstr}')
        # logger.remove()


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
                # if verbose: self.log(f'DTM already saved for key {key}')
                if verbose: self.log(f'Loading DTM from {ppath(keyfn)}')
                df=read_df(keyfn)
                self._dtmd[wordkey]=df
                if verbose: self.log(f'Returning DTM from {ppath(keyfn)}')
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
            if verbose: self.log(f'No frequency files found to generate DTM. Run preprocess_freqs()?')
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
        if verbose: self.log(f'Saving DTM to {ppath(keyfn)}')
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



class MetaCorpus(BaseCorpus):
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
            for C in tqdm(self.corpora,desc='Assembling dictionary of text objects'):
                for t in C.texts():
                    self._textd[(C.name, t.id)]=t
        return self._textd
            
    def meta_iter(self,progress=False):
        iterr=tqdm(self.corpora) if progress else self.corpora
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
    # def __init__(self,_source,_id=SECTION_NAME):
    #     self._source=_source
    #     self._id=_id
    #     for k,v in MANIFEST_DEFAULTS.items():
    #         setattr(self,k,v)
    #     self._metadf=None
    #     self._texts=None
    #     self._textd=defaultdict(lambda: None)

    # def __getattr__(self,name):
    #     res = get_from_attrs_if_not_none(self, name)
    #     if res is None:
    #         res = get_from_attrs_if_not_none(self._source, name)
    #     if res is not None and name.startswith('path_') and not os.path.isabs(res):
    #         res=os.path.join(self.source.path, res)
    #     return res

    
    @property
    def source(self): return self._source
    # @property
    # def id(self): return os.path.join(self.source.id, self._id)
    @property
    def path(self): return os.path.join(self.source.corpus.path_texts, self.id)
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








def Corpus(corpus_ref=None,**kwargs):
    # log.debug(f'Generating corpus object with id="{corpus_ref}"')

    if issubclass(corpus_ref.__class__, BaseCorpus):
        C=corpus_ref
    
    elif type(corpus_ref)==str and corpus_ref:
        # log.debug(f'Corpus() called on a string: "{corpus_ref}"')
        try:
            C=load(corpus_ref,**kwargs)
        except KeyError:
            C=BaseCorpus(corpus_ref,**kwargs)

    else:
        C=BaseCorpus(corpus_ref,**kwargs)
    
    # log.debug(f'Returning corpus: {C}')
    return C