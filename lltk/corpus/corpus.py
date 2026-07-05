import configparser
import os
import random
import re
import shutil

import numpy as np
import pandas as pd

from collections import Counter, defaultdict
from typing import Union

from logmap import logmap

from lltk.imports import (
    BAD_COLS,
    Bunch,
    DEFAULT_NUM_PROC,
    DEFAULT_PATH_TO_MANIFEST,
    DEST_LLTK_CORPORA,
    DIR_SECTION_NAME,
    DIR_TEXTS_NAME,
    DOWNLOAD_PART_DEFAULTS,
    IDSEP,
    IDSEP_START,
    MANIFEST_DEFAULTS,
    META_KEY_SEP,
    MODERNIZE_SPELLING,
    PATH_CORPUS,
    PATH_CORPUS_ZIP,
    PATH_LLTK_REPO,
    PATH_MANIFEST_GLOBAL,
    PREPROC_CMDS,
    TMP_CORPUS_ID,
    ZIP_PART_DEFAULTS,
    ensure_dir_exists,
    ensure_snake,
    get_tqdm,
    just_meta_no_id,
    just_metadata,
    log,
    mask_home_dir,
    read_json,
    rmfn,
    snake2camel,
    to_camel_case,
    tools,
)
from lltk.text import (
    BaseText,
    TextList,
    clean_text,
    get_idx,
    get_imsg,
    grab_tag_text,
    is_addr,
    is_addr_str,
    is_text_obj,
    is_textish,
    merge_dict,
    read_df_anno,
    to_corpus_and_id,
    tokenize,
    unhtml,
    xml2txt_default,
)
from .utils import (
    AuthorBunch,
    get_inducted_corpus_ids,
    is_corpus_obj,
    load_corpus,
)

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
    LINKS = {}
    LINK_TRANSFORMS = {}
    # Per-text prosodic output dir lives under {corpus.path}/prosodic/{text.id}/
    # (override via manifest `path_prosodic = ...` or instance `_path_prosodic`)
    _path_prosodic = 'prosodic'
    _path_tasks = 'tasks'







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
        self._addrs=set()
        self._texts=None
        self._textd=defaultdict(lambda: None)
        self._dtmd={}
        self._mfwd={}
        self._init=False
        self._source=None
        self._authors=None
        self._gdb=None
        self.name=_name

        # log.debug(f'{self.__class__.__name__}({get_imsg(id,**attrs)})')

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
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    
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
        if name.startswith('path_'): name_priv = '_'+name
        else: name_priv = '_path_'+name
        path = self.__dict__.get(name, self.__dict__.get(name_priv))
        if path is None:
            path = getattr(type(self), name, None)
        if path is None:
            path = getattr(type(self), name_priv, None)
        if path is not None and not os.path.isabs(path):
            if '~' in path:
                path=path.split('~')[-1]
                path=os.path.join(os.path.expanduser('~'), path[1:])
            else:
                path=os.path.join(self.path, path)
        return path

    
    @property
    def path(self):
        res=getattr(self,'_path',None)
        if res is None: res=os.path.join(PATH_CORPUS,self.id)
        return os.path.expanduser(res)
    
    @property
    def path_data(self): return os.path.join(self.path,'data')


    ####################################################################
    # ETC?
    ####################################################################


    @property
    def xml2txt_func(self): self.XML2TXT.__func__
    def xml2txt(self,*x,**y): return self.XML2TXT.__func__(*x,**y)

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








    @property
    def path_metadata_parquet(self):
        base = os.path.splitext(self.path_metadata)[0]
        return base + '.parquet'

    def _apply_manifest_genre(self, df):
        """Stamp genre / genre_raw from manifest keys, so a data-driven corpus
        can set a constant genre via the manifest (`genre = Fiction`) instead of
        a load_metadata override that exists only to do that. No-op unless the
        manifest defines the key (unset/empty = skip)."""
        if df is not None and len(df):
            g = getattr(self, 'genre', None)
            gr = getattr(self, 'genre_raw', None)
            if g: df['genre'] = g
            if gr: df['genre_raw'] = gr
        return df

    def load_metadata(self, force=False, **kwargs):
        if not force and self._metadf is not None:
            return self._metadf
        if not os.path.exists(self.path_metadata): self.install_metadata()
        if not os.path.exists(self.path_metadata): return pd.DataFrame()

        pq_path = self.path_metadata_parquet
        if not force and os.path.exists(pq_path):
            try:
                csv_mtime = os.path.getmtime(self.path_metadata)
                pq_mtime = os.path.getmtime(pq_path)
                if pq_mtime >= csv_mtime:
                    df = pd.read_parquet(pq_path)
                    if self.col_id in set(df.columns):
                        df = df.set_index(self.col_id)
                    df = self._apply_manifest_genre(df)
                    self._metadf = df
                    return df
            except (OSError, IOError, ValueError, KeyError):
                pass

        df = read_df_anno(self.path_metadata, dtype=str)
        if df is None or not len(df): return pd.DataFrame()
        if self.col_id in set(df.columns):
            df = df.set_index(self.col_id)
        from lltk.corpus.utils import clean_meta
        df = clean_meta(df)

        try:
            df.to_parquet(pq_path)
        except (OSError, IOError, PermissionError):
            pass

        df = self._apply_manifest_genre(df)
        self._metadf = df
        return df


    def merge_linked_metadata(self, meta):
        from lltk.corpus.utils import load
        if not self.LINKS or meta is None or not len(meta):
            return meta

        for corpus_id, (my_col, their_col) in self.LINKS.items():
            if my_col not in meta.columns:
                continue

            linked_corpus = load(corpus_id)
            if linked_corpus is None:
                continue
            linked_meta = linked_corpus.load_metadata()
            if linked_meta is None or not len(linked_meta):
                continue

            # Build join key, applying transform if needed
            transform = self.LINK_TRANSFORMS.get(my_col)
            if transform:
                join_col = f'_link_{corpus_id}'
                meta[join_col] = meta[my_col].apply(transform)
            else:
                join_col = my_col

            # Move their_col from index to column for merge
            if linked_meta.index.name == their_col or their_col not in linked_meta.columns:
                linked_meta = linked_meta.reset_index()

            # Prefix all linked columns
            linked_meta = linked_meta.rename(
                columns={c: f'{corpus_id}_{c}' for c in linked_meta.columns}
            )
            their_col_prefixed = f'{corpus_id}_{their_col}'

            # Preserve original index
            orig_index = meta.index
            meta = meta.reset_index()
            meta = meta.merge(
                linked_meta,
                left_on=join_col,
                right_on=their_col_prefixed,
                how='left',
                suffixes=('', f'_{corpus_id}_dup')
            )
            # Restore index
            if self.col_id in meta.columns:
                meta = meta.set_index(self.col_id)

            # Clean up temp join column
            if f'_link_{corpus_id}' in meta.columns:
                meta = meta.drop(columns=[f'_link_{corpus_id}'])

        return meta



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
            **kwargs) -> BaseText:
        """
        The one function users need to interact with a corpus's texts. Use this function both to get an existing text or to create a new one. Returns a text of type `corpus.TEXT_CLASS`. If an `id` is not specified, one will be auto-generated.

        Parameters
        ----------
        id : Union[str,BaseText,None], optional
            If an `id` is specified, this will be used as the text's ID. Default: None.

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
        # log(f'<- {kwargs}')
        meta = just_metadata(kwargs)
        # log.debug(f'<- {get_imsg(id,self,_source,**meta)}')

        # Init corpus?
        if _init: self.init()

        # Defaults
        t = None

        if is_textish(id) and not _source: _source=id
        id = self.get_text_id(id, _source=_source, _new=_new, **meta)

        # get?
        if not _force and id is not None: t = self.get_text(id)

        # Create?
        if _force or t is None: t = self.init_text(id,_source=_source,_cache=_cache,**meta)
        elif meta and is_text_obj(t): t.update(meta,_cache=_cache)

        # Fail?
        if t is None: raise CorpusTextException('Could not get or create text')

        # Return text
        # log.debug(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t

    # @log.fn
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

        # log.debug(f'<- id = {id}')
        
        t=self._textd.get(id)
        # log.debug(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t
        


    def remove_text(self,id,db=True,matches=True):
        if id in self._textd:
            log.info(f'removing {id} from {self.id}._textd')
            del self._textd[id]
        if db: self.db().delete(id)
        if matches: self.matcher.remove_node(self.get_addr(id))


    def get_text_id(self,
            id: Union[str,BaseText,None] = None,
            _source: Union[str,BaseText,None] = None,
            _new: bool = False,
            **kwargs):

        meta=just_metadata(kwargs)
        # log.debug(f'<- {get_imsg(id,self,_source,**meta)}')
        
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
        # log.debug(f'-> {id}')
        return id


    def iter_text_id(self,id,_new=True,newsep='/v'):        
        while _new and id in self._textd:
            log.info(f'<- {id}')
            idsuf=id.split(newsep)[-1]
            if idsuf.isdigit():
                isuf=int(idsuf)+1
                id=f'{newsep.join(id.split(newsep)[:-1])}{newsep}{isuf}'
            else:
                isuf=2
                id=f'{id}{newsep}{isuf}'
        log(f'-> {id}')
        return id


    def init_text(self,id=None,_source=None,_cache=True,**kwargs):
        # log('...')
        meta=just_meta_no_id(kwargs)
        # log.debug(f'<- {get_imsg(id,self,_source,**meta)}')
        if id is None: id = self.get_text_id(id, _source=_source, **meta)
        # gen text in my image
        t = self.TEXT_CLASS(id=id, _corpus=self, _source=_source, **meta)
        # log.debug(f'-> {t}' if is_text_obj(t) else "-> ?")
        return t


    def add_text(self,t,force=True,**kwargs):
        # assign by id
        if t is not None and is_text_obj(t):
            if force or self._textd.get(t.id) is None:
                self._textd[t.id]=t
    


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
        """Return a random text, sampling from the full corpus via MetaDB
        (or metadata.csv if the DB is unavailable). Avoids loading the
        whole corpus when you just want a quick sample.

        Sampling order:
          1. Main metadata DB (ClickHouse via shim, ~ms)
          2. metadata.csv id column (DB-free, ~1s for 300K rows)
          3. In-memory `_textd` (only useful if already iterated)
          4. Full corpus load (last resort)
        """
        # 1. Main metadata DB (ClickHouse via the metadb shim)
        row = None
        try:
            from lltk.db.metadb import metadb
            row = metadb.conn.execute(
                "SELECT id FROM texts WHERE corpus = ? ORDER BY random() LIMIT 1",
                [self.id],
            ).fetchone()
        except (ImportError, AttributeError, RuntimeError, OSError):
            pass
        if row and row[0]:
            return self.TEXT_CLASS(id=row[0], _corpus=self)
        # 2. CSV — works even if the DB is unavailable. Cached across calls
        # so we don't re-parse a 300K-row file every time.
        try:
            ids = self._cached_id_list()
            if ids:
                return self.TEXT_CLASS(id=random.choice(ids), _corpus=self)
        except (FileNotFoundError, OSError, KeyError, ValueError):
            pass
        # 3. Memory, if already populated by prior iteration
        loaded = [t for t in self._textd.values() if t is not None]
        if loaded and len(loaded) > 1:
            return random.choice(loaded)
        # 4. Full load (last resort)
        ol = list(self.texts(progress=False))
        return random.choice(ol) if ol else None

    def _cached_id_list(self):
        """Return a list of text ids from metadata.csv, cached per instance."""
        cached = self.__dict__.get('_cached_ids')
        if cached is not None:
            return cached
        path = self.path_metadata
        if not path or not os.path.exists(path):
            self._cached_ids = []
            return self._cached_ids
        import pandas as _pd
        try:
            ids = _pd.read_csv(path, usecols=['id'], dtype=str)['id'].dropna().tolist()
        except (FileNotFoundError, OSError, KeyError, ValueError):
            ids = []
        self._cached_ids = ids
        return ids




    @property
    def textd(self):
        if not self._init:
            for t in self.iter_init(): pass
            self._init = True
        return self._textd

    def texts(self,*args,**kwargs):
        return self.iter_texts(*args,**kwargs)

    def iter_texts(self, texts=None, progress=True, shuffle=False, lim=None, **kwargs):
        if texts:
            o = list(texts)
            if shuffle: random.shuffle(o)
            if lim: o = o[:lim]
            yield from o
        elif self._init:
            o = [t for t in self._textd.values() if t is not None]
            if shuffle: random.shuffle(o)
            if lim: o = o[:lim]
            yield from o
        else:
            yield from self.iter_init(progress=progress, shuffle=shuffle, lim=lim)
    
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

        
    
    def iter_init(self, progress=True, lim=None, shuffle=False, **kwargs):
        if self._init:
            o = [t for t in self._textd.values() if t is not None]
            if shuffle: random.shuffle(o)
            if lim: o = o[:lim]
            yield from o
            return

        df = self.load_metadata()
        if df is None or not len(df):
            self._init = True
            return

        ids = list(df.index)
        if shuffle: random.shuffle(ids)
        if lim: ids = ids[:lim]

        full_init = (not lim or lim >= len(df.index))
        desc = f'[{self.name}] Loading corpus'
        with logmap(desc) as lm:
            for id in lm.progress(ids) if progress else ids:
                id = to_corpus_and_id(id)[1]
                if id in self._textd and self._textd[id] is not None:
                    t = self._textd[id]
                else:
                    row = df.loc[id] if id in df.index else pd.Series(dtype=object)
                    row_meta = {k: v for k, v in row.items() if pd.notna(v) and str(v) not in ('', 'nan')}
                    t = self.TEXT_CLASS(id=id, _corpus=self, **row_meta)
                    t._meta_hydrated = True
                    self._textd[id] = t
                yield t
        if full_init:
            self._init = True

    def init(self, force=False, **kwargs):
        if not force and self._init: return self
        _ = self.textd
        return self

    def metadata(self, force=False, **kwargs):
        return self.load_metadata(force=force, **kwargs)

    @property
    def meta(self): return self.load_metadata()
    @property
    def metadf(self): return self.meta
    @property
    def df(self): return self.metadf


    @property
    def metad(self):
        if not hasattr(self,'_metad'):
            self._metad=dict(list(zip(self.text_ids,self.meta)))
        return self._metad








    # #################
    # #### PROCESSING
    # #################


    def zip(self,savedir=PATH_CORPUS_ZIP,ask=False,parts=ZIP_PART_DEFAULTS):
        import zipfile
        savedir = os.path.expanduser(savedir)
        if not os.path.exists(savedir): os.makedirs(savedir)
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

        def _collect_paths(path, pathpart=''):
            """Collect file paths relative to parent directory."""
            path = os.path.abspath(path)
            if not os.path.isdir(path):
                paths = [path]
                if pathpart == 'metadata':
                    import glob
                    parent = os.path.dirname(path)
                    for p in glob.glob(os.path.join(parent, 'metadata_enriched.*')):
                        if os.path.isfile(p):
                            paths.append(p)
                return paths
            paths = []
            for root, dirs, files in os.walk(path):
                for file in files:
                    paths.append(os.path.join(root, file))
            if pathpart == 'raw':
                return paths
            # filter to files belonging to this corpus
            try:
                pppath = getattr(self, f'path_{pathpart}')
                pppath0 = os.path.dirname(pppath)
                acceptable_paths = {getattr(t,f'path_{pathpart}') for t in self.texts()}
                paths = [p for p in paths if p in acceptable_paths]
            except (AssertionError, Exception):
                pass
            return paths

        def do_zip(path, fname, pathpart=''):
            path = os.path.abspath(path)
            if not os.path.exists(path): return
            if not fname.endswith('.zip'): fname+='.zip'
            opath = os.path.join(savedir, fname)

            abs_paths = _collect_paths(path, pathpart)
            if not abs_paths: return

            # determine base dir for arcnames (parent of the part path)
            basedir = os.path.dirname(path) if not os.path.isdir(path) else os.path.dirname(path)

            with zipfile.ZipFile(opath, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for abspath in self.get_tqdm(abs_paths, desc=f'Compressing {fname}'):
                    arcname = os.path.relpath(abspath, basedir)
                    zipf.write(abspath, arcname)

            log(f'Created {opath} ({os.path.getsize(opath)/1024:.0f} KB)')

        for part in part2ok:
            if not part2ok[part]: continue
            do_zip(getattr(self,f'path_{part}'), f'{self.id}_{part}.zip', pathpart=part)


    @staticmethod
    def _dropbox_cmd():
        """Find the dropbox_uploader.sh script."""
        import shutil
        # check PATH first
        dbu = shutil.which('dbu') or shutil.which('dropbox_uploader.sh')
        if dbu: return dbu
        # bundled copy
        bundled = os.path.join(PATH_LLTK_REPO, 'bin', 'dropbox_uploader.sh')
        if os.path.exists(bundled): return bundled
        return None

    def upload(self, parts=None, dest=DEST_LLTK_CORPORA):
        import subprocess
        if not parts: parts = DOWNLOAD_PART_DEFAULTS
        if isinstance(parts, str): parts = [p.strip() for p in parts.split(',')]
        dbu = self._dropbox_cmd()
        if not dbu:
            log.error('dropbox_uploader.sh not found. Install from https://github.com/andreafabrizi/Dropbox-Uploader')
            return
        zipdir = os.path.expanduser(PATH_CORPUS_ZIP)
        for part in parts:
            local = os.path.join(zipdir, f'{self.id}_{part}.zip')
            if not os.path.exists(local):
                log.warning(f'No zip for {part}: {local}')
                continue
            remote = f'{dest}/{self.id}_{part}.zip'
            log(f'Uploading {os.path.basename(local)} to {remote}...')
            subprocess.run([dbu, 'upload', local, remote], check=True)
            log(f'  Done.')

    def share(self, parts=None, dest=DEST_LLTK_CORPORA):
        """Get Dropbox share links. Returns dict of {part: url}."""
        import subprocess
        if not parts: parts = DOWNLOAD_PART_DEFAULTS
        if isinstance(parts, str): parts = [p.strip() for p in parts.split(',')]
        dbu = self._dropbox_cmd()
        if not dbu:
            log.error('dropbox_uploader.sh not found.')
            return {}
        urls = {}
        for part in parts:
            remote = f'{dest}/{self.id}_{part}.zip'
            try:
                out = subprocess.check_output([dbu, 'share', remote], text=True)
            except subprocess.CalledProcessError:
                continue
            # parse URL from output like: " > Share link: https://...?rlkey=...&dl=0"
            import re
            m = re.search(r'(https://\S+)', out)
            if not m: continue
            url = m.group(1).rstrip("'\"")
            # ensure dl=1 for direct download, preserve rlkey
            url = re.sub(r'[&?]dl=\d', '', url)
            url += ('&' if '?' in url else '?') + 'dl=1'
            urls[part] = url
            log(f'url_{part} = {url}')
        return urls

    def publish(self, public=None, private=None, parts=None):
        """Zip, upload to Dropbox, get share links, and update manifests.

        Public parts get URLs in the package manifest (checked into repo).
        Private parts get URLs only in the user manifest (~/.lltk_data/manifest.txt).
        Both update public= and private= fields.

        Usage:
            c.publish(public=['metadata'], private=['freqs', 'txt'])
            c.publish(public=['metadata', 'freqs'])
            c.publish(parts=['metadata'])  # all parts treated as public
        """
        if parts and not public and not private:
            public = parts
        if isinstance(public, str): public = [p.strip() for p in public.split(',')]
        if isinstance(private, str): private = [p.strip() for p in private.split(',')]
        public = public or []
        private = private or []
        all_parts = public + private
        if not all_parts:
            log.error('No parts specified.')
            return {}

        # 1. Zip
        log(f'[{self.name}] Zipping {all_parts}...')
        self.zip(parts=all_parts)

        # 2. Upload
        log(f'[{self.name}] Uploading to Dropbox...')
        self.upload(parts=all_parts)

        # 3. Share
        log(f'[{self.name}] Getting share links...')
        urls = self.share(parts=all_parts)
        if not urls:
            log.error('No share links generated.')
            return {}

        # 4. Update manifests
        public_urls = {p: urls[p] for p in public if p in urls}
        private_urls = {p: urls[p] for p in private if p in urls}

        # Package manifest: public URLs + public/private labels
        self._update_manifest(
            PATH_MANIFEST_GLOBAL,
            urls=public_urls,
            public=public,
            private=private,
        )
        # User manifest: all URLs (public + private)
        if private_urls:
            self._update_manifest(
                DEFAULT_PATH_TO_MANIFEST,
                urls=private_urls,
            )

        return urls

    def _update_manifest(self, manifest_path, urls=None, public=None, private=None):
        """Update a manifest file with URLs and public/private labels."""
        import configparser
        manifest_path = os.path.expanduser(manifest_path)
        config = configparser.ConfigParser()
        if os.path.exists(manifest_path):
            config.read(manifest_path)

        section = self.name
        if section not in config:
            config.add_section(section)
            # copy basic fields
            for key in ['id', 'name', 'desc']:
                val = getattr(self, key, None)
                if val: config.set(section, key, str(val))

        if urls:
            for part, url in urls.items():
                config.set(section, f'url_{part}', url)

        if public is not None:
            config.set(section, 'public', ','.join(public))
        if private is not None:
            config.set(section, 'private', ','.join(private))

        ensure_dir_exists(manifest_path)
        with open(manifest_path, 'w') as f:
            config.write(f)

        n_urls = len(urls) if urls else 0
        log(f'Updated {manifest_path} with {n_urls} URL(s) for [{section}]')

    def get_tqdm(self,*x,desc='',**y):
        if desc: desc=f'[{self.name}] {desc}'
        return get_tqdm(*x,desc=desc,**y)


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
        log('\n'.join(ol))

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

    def compile_download(self,unzip=True):
        return self.install(part='raw',unzip=unzip)

    def preprocess(self,parts=PREPROC_CMDS,verbose=True,force=False,num_proc=DEFAULT_NUM_PROC,**attrs):
        import lltk.model.preprocess  # ensure preprocess_txt/freqs are attached
        if not parts: parts=PREPROC_CMDS
        if type(parts)==str: parts=[p.strip().lower() for p in parts.split(',')]
        for part in parts:
            fname='preprocess_'+part
            if not hasattr(self,fname): continue
            func=getattr(self,fname)
            log.info(f'Running {fname}...')
            try:
                x=func(verbose=verbose,num_proc=int(num_proc),force=force, **attrs)
            except TypeError as e:
                log(f'!! ERROR in {fname}: {e}')
                pass

    def has_data(self,part):

        ppart=f'path_{part}'
        if not hasattr(self,ppart): return
        path=getattr(self,ppart)
        if not os.path.exists(path): return False
        if os.path.isdir(path) and not os.listdir(path): return False
        # log(part,ppart,path,os.path.exists(path))

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

    def mfw(self, n=10000, texts=None, force=False):
        cache_key = f'mfw_n{n}'
        if not force and cache_key in self._mfwd:
            return self._mfwd[cache_key]

        total = Counter()
        text_iter = texts if texts is not None else self.texts()
        for t in get_tqdm(text_iter, desc=f'[{self.name}] Computing MFW'):
            freqs = t.freqs()
            if freqs:
                total.update(freqs)

        result = [w for w, c in total.most_common(n)]
        self._mfwd[cache_key] = result
        return result

    def dtm(self, words=None, n=10000, texts=None, tf=False, tfidf=False, force=False):
        if words is None:
            words = self.mfw(n=n, texts=texts, force=force)

        rows = {}
        text_iter = texts if texts is not None else self.texts()
        for t in get_tqdm(text_iter, desc=f'[{self.name}] Building DTM'):
            freqs = t.freqs()
            if freqs:
                rows[t.id] = {w: freqs.get(w, 0) for w in words}

        dtm = pd.DataFrame.from_dict(rows, orient='index', columns=words).fillna(0)

        if tf:
            row_sums = dtm.sum(axis=1)
            dtm = dtm.div(row_sums, axis=0).fillna(0)
        if tfidf:
            from sklearn.feature_extraction.text import TfidfTransformer
            transformer = TfidfTransformer()
            tfidf_matrix = transformer.fit_transform(dtm)
            dtm = pd.DataFrame(tfidf_matrix.toarray(), index=dtm.index, columns=dtm.columns)

        return dtm

    @property
    def path_home(self):
        return os.path.join(PATH_CORPUS,self.id)
    @property
    def path_texts(self):
        return os.path.join(self.path,DIR_TEXTS_NAME)










class SectionCorpus(BaseCorpus):
    DIV_SECTION = None    # e.g. 'div3' — which div level holds sections
    DIV_GROUP = None      # e.g. 'div2' — optional grouping level (volumes/books)
    SECTION_PREFIX = 'S'  # prefix for generated section IDs

    def init(self, force=False, **kwargs):
        if not force and self._init: return
        self.parse_sections(force=force, **kwargs)

    def parse_sections(self, force=False, **kwargs):
        source = self.source
        if source is None: return
        xml = source.xml
        if not xml: return

        import bs4
        dom = bs4.BeautifulSoup(xml, 'lxml')

        div_tag = self.DIV_SECTION or self._find_div_tag(dom)
        if not div_tag: return

        group_tag = self.DIV_GROUP
        groups = dom(group_tag) if group_tag else [dom]
        if not groups: groups = [dom]

        section_i = 0
        for group_i, group_dom in enumerate(groups):
            divs = group_dom(div_tag)
            if not divs: divs = [group_dom]

            for div_dom in divs:
                section_i += 1
                section_id = f'{self.SECTION_PREFIX}{section_i:04}'
                # extract title before stripping bad tags (head is in BAD_TAGS)
                title = self._extract_title(div_dom)
                idref = grab_tag_text(div_dom, 'idref', limit=1)
                section_xml = clean_text(str(div_dom))
                meta = dict(
                    _xml=section_xml,
                    title=title,
                    id_orig=idref,
                    group_i=group_i + 1,
                    section_i=section_i,
                )
                self.init_text(id=section_id, **meta)

        if section_i: self._init = True

    def _find_div_tag(self, dom):
        # Pick the div level with the most elements — avoids selecting a rare
        # embedded sub-section (e.g. a single div4 "Provençal Tale" inside
        # Udolpho's 57 div3 chapters).  Ties broken toward deeper levels.
        best_tag, best_count = None, 0
        for tag in ['div0', 'div1', 'div2', 'div3', 'div4', 'div5']:
            n = len(dom(tag))
            if n >= best_count and n > 0:
                best_tag, best_count = tag, n
        return best_tag

    def _extract_title(self, div_dom):
        for tag_name in ['comhd5','comhd4','comhd3','comhd2','comhd1','head','caption']:
            tags = div_dom(tag_name, recursive=False) or div_dom(tag_name)
            if tags:
                raw = str(tags[0])
                # extract title text between </collection> and <attbytes> if present
                if '</collection>' in raw and '<attbytes>' in raw:
                    title = raw.split('</collection>')[-1].split('<attbytes>')[0].strip()
                else:
                    title = tags[0].get_text().strip()
                title = clean_text(unhtml(title))
                if title: return title
        # fallback: lxml strips <head> into bare text — check first text node
        from bs4 import NavigableString
        for child in div_dom.children:
            if isinstance(child, NavigableString):
                text = child.strip()
                if text: return clean_text(text)
            else:
                break  # stop at first tag
        return ''

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
        if id is None: id=get_idx(i=len(self._textd), prefstr='S', numposs=1000)
        if id not in self._textd:
            section_class=self.get_section_class(section_class)
            sec = section_class(id, _source=self.source, _section_corpus=self, **meta)
            self._textd[id]=sec
        else:
            sec=self._textd[id]
        return sec

    def texts(self, *args, **kwargs):
        if not self._init: self.init()
        return super().texts(*args, **kwargs)

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


class ParagraphSectionCorpus(SectionCorpus):
    SECTION_PREFIX = 'P'

    def parse_sections(self, force=False, **kwargs):
        source = self.source
        if source is None: return

        # try XML paragraphs first
        xml = source.xml
        if xml:
            import bs4
            dom = bs4.BeautifulSoup(xml, 'lxml')
            paras = dom('p')
            if paras:
                for i, p in enumerate(paras):
                    text = p.get_text().strip().replace('\n', ' ')
                    while '  ' in text: text = text.replace('  ', ' ')
                    if not text: continue
                    section_id = f'{self.SECTION_PREFIX}{i+1:04}'
                    words = text.split()
                    self.init_text(
                        id=section_id,
                        _txt=text,
                        word_start=0,
                        word_end=len(words),
                        num_words=len(words),
                    )
                if self._textd: self._init = True
                return

        # fall back to \n\n splitting on plain text
        txt = source.txt
        if not txt: return
        blocks = [b.strip() for b in txt.split('\n\n') if b.strip()]
        for i, block in enumerate(blocks):
            section_id = f'{self.SECTION_PREFIX}{i+1:04}'
            words = block.split()
            self.init_text(
                id=section_id,
                _txt=block,
                word_start=0,
                word_end=len(words),
                num_words=len(words),
            )
        if self._textd: self._init = True


class PassageSectionCorpus(SectionCorpus):
    SECTION_PREFIX = 'W'

    def __init__(self, n=500, **kwargs):
        self._passage_n = n
        super().__init__(**kwargs)

    def parse_sections(self, force=False, **kwargs):
        source = self.source
        if source is None: return
        txt = source.txt
        if not txt: return

        import nltk
        from lltk.text.text import _lang_to_punkt
        from lltk.tools.constants import chunk_sentences
        sents = nltk.sent_tokenize(txt, language=_lang_to_punkt(source.lang))

        for chunk_txt, word_start, word_end, num_words in chunk_sentences(sents, self._passage_n):
            section_id = f'{self.SECTION_PREFIX}{word_start:05}_{word_end:05}'
            self.init_text(
                id=section_id,
                _txt=chunk_txt,
                word_start=word_start,
                word_end=word_end,
                num_words=num_words,
            )

        if self._textd: self._init = True


CORPUS_CACHE={}

def Corpus(corpus=None,force=False,init=False,clear=False,**kwargs):
    global CORPUS_CACHE
    
    C = None
    
    if is_corpus_obj(corpus):
        C=corpus
        logg=False
    else:
        # log.debug(f'<- id = {corpus}')
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

    if logg: log.debug(f'-> {C}')
    return C




