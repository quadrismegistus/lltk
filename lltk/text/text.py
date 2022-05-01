from lltk.imports import *

## utility funcs


class BaseText(BaseObject):
    BAD_TAGS={'note','footnote','greek','latin'}
    # BODY_TAG=None
    XML2TXT=xml2txt_default
    TOKENIZER=tokenize
    SECTION_CLASS=None
    SECTION_CORPUS_CLASS=None
    SECTION_DIR_NAME=DIR_SECTION_NAME
    META={'genre':'', 'medium':''}

    def __eq__(self,other):
        addr1 = self.addr
        addr2 = other.addr if is_text_obj(other) else other
        return addr1==addr2
    
    def __hash__(self): return hash(self.addr)

    def __init__(self,
            id=None,
            _corpus=None,
            _section_corpus=None,
            _source=None,
            _sources=set(),
            _txt=None,
            _xml=None,
            _add_sources=True,
            _cache_g=True,
            **kwargs):
        
        from lltk import Corpus
        self.corpus=corpus=Corpus(_corpus)

        params,meta = to_params_meta(kwargs)
        if log>0:  log(f'<- {get_imsg(id,_corpus,_source,**meta)}')
        
        if id is None and _source is not None: id=_source.addr
        if id is None and len(_sources): id=list(_sources)[0].addr
        self.id=id=get_idx(**merge_dict( dict(id=id,i=len(self.corpus._textd)+1), meta ))

        if log>0: log(f'{self.__class__.__name__}({get_imsg(id,_corpus,_source,**meta)})')
        
        self._section_corpus=_section_corpus
        self._sections={}
        self._meta=self.ensure_id(meta)
        if _txt: self._txt=_txt
        if _xml: self._xml=_xml

        # matches?
        srcs = []
        for src in [_source] + list(_sources):
            if src is None: continue
            if src is self: continue
            if src == self.addr: continue
            src=Text(src)
            if src not in set(srcs):
                srcs.append(src)
                if _add_sources:
                    self.add_source(src,yn='y',cache=_cache_g)
        if srcs:
            if srcs and log.verbose>1: log(f'{self} has sources: {srcs}')
            self._sources = srcs
            self._source = srcs[0]
        else:
            self._sources = []
            self._source = None
        
    #def __repr__(self): return f'Text({self.addr})'
    def __repr__(self): 
        cname=self.__class__.__name__
        if not cname.endswith('Text') and not cname.startswith('Text'): cname='Text'+cname
        return f'{cname}({self.addr})'

    def __getitem__(self, key): return self.get(key)

    def __getattr__(self, name):
        if name.startswith('path_'): return self.get_path(name)

        res = getattribute(self, name)
        if res is not None: return res

        res = self.get(name)
        if res is not None: return res
        
        return None


    def get(self,
            key,
            ish=True,
            from_cache=True,
            from_sources=True,
            cache=False,
            remote=False,
            
            ):
        res = self._meta.get(key) if not ish else self.get_ish(self._meta,key)
        if from_sources and res is None and key[0]!='_':
            meta = self.metadata(
                from_cache=from_cache,
                from_source=from_sources,
                cache=cache,
                remote=remote
            )
            res = meta.get(key) if not ish else self.get_ish(meta,key)
        if type(res)==str: res=clean_text(res)
        return res

    def get_ish(self,meta,key,before_suf='|'):
        keys=[k for k in meta.keys() if k.startswith(key)]
        numkeys=len(keys)
        if numkeys==0: return None
        if numkeys==1: 
            keyname = keys[0]
        else:

            def get_rank(k,sep='__'):
                if not sep in k: return np.inf
                corp=k.split(sep)[-1]
                crnk=CORPUS_SOURCE_RANKS.get(corp,np.inf)
                return crnk

            keys.sort(key=lambda k: get_rank(k))
            keyname=keys[0]

            if log>0: log.warning(f'more than one metadata key begins with "{key}": {", ".join(keys)}. Using key: "{keyname}".')
        
        res = meta.get(keyname)

        if before_suf and type(res)==str and res:
            res = res.split('|')[0]
        
        return res


    
    def __setitem__(self, key, value): return self.update({key:value})
    def __delitem__(self, key):
        if key in self._meta: del self._meta[key]
    def __iter__(self): return iter(self.meta.items())
    def __len__(self): return self.num_words

    def update(self,*metal,_cache=True,**metad):
        imeta = [self._meta] + list(metal) + [metad]
        meta = merge_dict(*imeta)
        self.set_meta(meta,cache=_cache)
    
    def set_meta(self,meta,cache=True):
        newmeta=self.ensure_id(meta)
        ddiff=diffdict(self._meta, newmeta)
        if ddiff:
            if log>0: log(pf('Metadata updated:',ddiff))
            self._meta = newmeta
            if cache: self.cache()
    
    def db(self): return self.corpus.db()
    
    def cache(self, json=True,db=False,*x, **y):
        meta={k:v for k,v in self._meta.items() if k and not '__' in k and v}
        if log>0: log(self)
        if db: self.cache_db(meta=meta)
        if json: self.cache_json(meta=meta)

    def init_cache(self,json=True,db=False,*x,**y):
        if json: self._meta=merge_dict(self._meta,self.init_cache_json())
        if db: self._meta=merge_dict(self._meta,self.init_cache_db())
        return self._meta




        
    def cache_db(self,meta=None):
        tid,new=self.id,(self._meta if not meta else meta)
        with self.db() as db:
            old=db[tid]
            if is_cacheworthy(new,old):
                db[tid]=new
                if log>0: log(f'cached text meta in db under key "{self.id}"')
    
    def cache_json(self,meta=None):
        # tid,new=self.id,(self._meta if not meta else meta)
        #old=self.init_cache_json()
        #if is_cacheworthy(new,old):
        #    write_json(new,self.path_meta_json)
        write_json(
            self._meta if not meta else meta,
            self.path_meta_json
        )   
        if log>0: log(f'cached text meta in json: {self.path_meta_json}')


    def init_cache_db(self,*x,**y):
        return self.db().get(self.id,{})
    
    # def init_cache(self,*x,**y):
    #     cached = self.load_cache(*x,**y)
    #     # self.log(cached)
    #     if cached:
    #         newmeta = merge_dict(
    #             cached,
    #             self._meta,
    #         )
    #         ddiff = diffdict(self._meta,newmeta)
    #         if ddiff:
    #             if log>0: log(pf(f'changes loaded from cache:',ddiff))
    #             self._meta = self.ensure_id(newmeta)
    #     return self._meta


    def init_cache_json(self,*x,**y):
        return read_json(self.path_meta_json)
    
    

    def get_path_old(t,part='texts',**kwargs):
        if not t.corpus: return ''
        partattr='path_'+part
        extattr='ext_'+part
        res=getattr(t.corpus, partattr)
        if res:
            o=os.path.join(res,t.id)
            resext=getattr(t.corpus, extattr)
            if resext: o+=resext
            return o


    def get_path(self,part,**kwargs):
        if part.startswith('path_'): part=part[5:]
        path = self.get_path_new(part,**kwargs)
        if path and os.path.exists(path): return path
        path = self.get_path_old(part,**kwargs)
        if path and os.path.exists(path): return path
        return ''

    def get_path_new(self,part,**kwargs):
        if part == 'txt': return os.path.join(self.path,'text.txt')
        if part == 'xml': return os.path.join(self.path,'text.xml')
        if part in {'json','meta','meta_json'}: return os.path.join(self.path,'meta.json')
        if part == 'freqs': return os.path.join(self.path,'freqs.json')
        return None

    @property
    def xml2txt_func(self): return self.XML2TXT.__func__




    
    @property
    def path(self): return os.path.join(self.corpus.path_texts,self.id)
    
    @property
    def path_meta_json(self): return os.path.join(self.path,'meta.json')
    

    # load text?
    
    @property
    def addr(self):
        return f'{IDSEP_START}{self.corpus.id}{IDSEP}{self.id}'
    @property
    def txt(self): return self.get_txt()

    @property
    def xml(self):
        if self._xml: return self._xml
        path_xml = self.get_path_xml()
        if not os.path.exists(path_xml): return ''
        with open(path_xml) as f: return clean_text(f.read())
    
    
    def get_path_xml(self):
        if not os.path.exists(self.path_xml):
            tsrc = self.source
            if tsrc is not None and os.path.exists(tsrc.path_xml):
                return tsrc.path_xml
        return self.path_xml

    def get_path_text(self,part='txt'):
        if part=='txt':
            return os.path.join(self.path, 'text.txt')
        elif part=='xml':
            return os.path.join(self.path, 'text.xml')
        return ''

    # xml
    @property
    def dom(self):
        if self._dom is not None: return self._dom
        import bs4
        xml=self.xml
        if xml:
            dom=bs4.BeautifulSoup(xml,'lxml')
            for tag in self.BAD_TAGS:
                for x in dom(tag):
                    x.extract()
        else:
            dom=bs4.BeautifulSoup()

        if self.BODY_TAG is not None:
            dom = dom.find(self.BODY_TAG)
        
        return dom

    
        
    
    
    @property
    def meta(self): return self.metadata()

    @property
    def _meta_(self): return {k:v for k,v in self._meta.items() if not '__' in k}


    def id_is_valid(self,*x,**y): return True
    def meta_is_valid(self,*x,**y): return True
    def is_valid(self,meta=None,**kwargs):
        """
        @TODO: Subclasses need to implement this
        """
        return True

    @property
    def matcher(self): return self.corpus.matcher
    @property
    def matcher_global(self): return self.corpus.matcher_global


    @property
    def matches(self): return self.get_matches()
    def get_matches(self,as_text=True,**kwargs):
        matches = (set(self.matcher[self.addr]) | set(self.matcher_global[self.addr])) - {self.addr}
        o=[
            Text(x) if as_text else x
            for x in matches
        ]
        return [x for x in o if (not as_text or x.id_is_valid())]
    
    # Text
    
    def add_source(self,source,viceversa=True,yn='',**kwargs):
        match=self.match(source,yn=yn,**kwargs)
        if log>0: log(f'found match: {match}')
        if viceversa:
            if log>0: log('adding source vice versa')
            source.add_source(self,viceversa=False,yn=yn,**kwargs)
        return match
    
    def match(self,other,yn='',**kwargs):
        return (
            self.matcher.match(self,other,yn=yn,**kwargs),
            self.matcher_global.match(self,other,yn=yn,**kwargs)
        )

    @property
    def idx(self): return self.id.split('/')[-1]
    
    def get_sources(self,remote=False,cache=True,**kwargs):
        if not self.id_is_valid(): return []
        sources=self.get_local_sources(**kwargs)
        if remote:
            self.get_remote_sources(sources,**kwargs)
            sources=self.get_local_sources(**kwargs)
        if cache: self.matcher.cache()
        return [x for x in sources if x.id_is_valid()]
        # return set(sources)
        # return sorted(list(sources),key=lambda t: t.addr)

    def get_remote_sources(self,*args,**kwargs):
        # OVERWRITE?
        return set()

    # def get_remote_sources(self,sources=None,wikidata=True,**kwargs):
    #     sofar=set()
    #     if wikidata:
    #         wiki=self.wikidata(sources,**kwargs)
    #         if wiki is not None:
    #             sofar|={wiki}
    #     return sofar - {self}


    def get_local_sources(self,sofar=None,recursive=False,verbose=1,**kwargs):
        if log>1: self.log(f'get_sources(sofar = {sofar}, recursive = {recursive}, **kwargs')
        sofar = set() if not sofar else set([x for x in sofar])

        matches = set(self.get_matches(**kwargs)) - {self}
        if matches and verbose>1: self.log('Matches:',matches)

        _sources = set(self._sources) - {self}
        if log>1: self.log('My _sources:',_sources)

        # missing match?
        for src in (_sources - matches): self.match(src,cache=False)
        
        newsrcs = {Text(x) for x in (((_sources | matches) - set(sofar)) - {self})}
        if newsrcs and verbose>1: self.log('New sources:',newsrcs)

        for src in newsrcs:
            if src not in sofar:
                if log>0: self.log(f'found source: {src}')
                sofar|={src}
        
        return sofar - {self}

    
    @property
    def sources(self): return self.get_sources()

    @property
    def source(self):
        srcs=[x for x in self._sources]
        if srcs: return Text(srcs[0])
        srcs=self.sources
        if srcs: return list(srcs)[0]

    def query(self,*x,**y): return {}

    def metadata(
            self,
            meta={},
            from_cache=True,
            from_sources=True,
            cache=False,
            remote=False,
            sep='__',
            **kwargs):
        # get meta
        # return self._meta
        # self.corpus.init()					
        # query?
        ometa={}
        if from_cache: 
            self.init_cache()
            if log>1: log(f'loaded from cache: {self._meta}')
        
        ometa=merge_dict(TEXT_META_DEFAULT, self.META, ometa, self._meta, meta)
        ometa={k:v for k,v in ometa.items() if k not in {'_source'}}
        # self.log('1',ometa)

        if remote:
            if self.id_is_valid() and not self.meta_is_valid(ometa):
                query_meta = self.query(**kwargs)
                if query_meta:
                    ometa=merge_dict(ometa, query_meta)
        # self.log('2',ometa)
        # sources?
        ometa = {k:v for k,v in ometa.items() if k.count(sep)<=1}
        if from_sources:
            sources_present = {x.split(sep)[-1] for x in ometa if sep in x}
            if log>1: log(f'sources_present = {sources_present}')
            for src in self.get_sources(remote=remote,cache=False,**kwargs):
                if src in sources_present: continue
                if src.corpus.id == self.corpus.id: continue
                if src.corpus.id == TMP_CORPUS_ID: continue
                
                if log>0: log(f'adding metadata from source: {src}')
                # if not wikidata and src.corpus.id=='wikidata': continue
                sd = src.metadata(
                    from_sources=False,
                    # from_cache=False,
                    from_cache=from_cache,
                    remote=remote,
                    cache=cache,
                    **kwargs
                )
                sd2={k+sep+src.corpus.id:v for k,v in sd.items() if k!=self.corpus.col_id and not sep in k}
                ometa = merge_dict(ometa, sd2)
        # self.log('3',ometa)
        ometa = {k:v for k,v in ometa.items() if k.count(sep)<=1}
        # ometa = to_numeric_dict(ometa)
        ometa = self.ensure_id_addr(ometa)
        self._meta = ometa
        if cache:
            self.cache()
        return ometa
    
    @property
    def col_addr(self): return self.corpus.col_addr
    @property
    def col_id(self): return self.corpus.col_id

    def ensure_id_addr(self,*x,**y): return self.ensure_id(*x,**y)

    def ensure_id(self,meta=None,col_id=COL_ID,col_corpus='_corpus'):
        if meta is None: meta=self._meta
        return {
            **{col_id:self.id},
            **dict(
                sorted({
                    k:v
                    for k,v in meta.items()
                    if k!=col_id
                }.items())
            )
        }

    def init(self,force=False,**kwargs):
        if force or not self._init:
            if log>1: log(f'Init metadata: {self}')
            self.metadata(**kwargs)
            # self.get_sources()
            self._init=True
        return self


    def text_plain(self, force_xml=None):
        """
        This function returns the plain text file. You may want to modify this.
        """
        # Return plain text version if it exists
        if self.path_txt and os.path.exists(self.path_txt) and not force_xml:
            with open(self.path_txt,encoding='utf-8',errors='ignore') as f:
                return f.read()
        # Otherwise, load from XML?
        if os.path.exists(self.path_xml): return self.XML2TXT.__func__(self.path_xml)
        return ''

    def get_txt(self,force=False,prefer_sections=True,section_type=None,force_xml=False):
        if force or not self._txt:
            if not prefer_sections:
                self._txt=self.text_plain(force_xml=force_xml)
                self._txt_offsets={}
            else:
                secs=self.sections(section_type)
                if secs is not None and secs.txt:
                    self._txt=secs.txt
                    self._txt_offsets=secs._txt_offsets
                else:
                    self._txt=self.text_plain(force_xml=force_xml)
                    self._txt_offsets={}
        return clean_text(self._txt) if self._txt else ''

    
    # freqs
    def save_freqs_json(self,ofolder=None,force=False):
        if not self.id: return {}
        if not os.path.exists(self.path_txt): return {}
        if not force and os.path.exists(self.path_freqs): return
        return save_freqs_json((self.path_txt,self.path_freqs,self.corpus.TOKENIZER.__func__))

    def freqs(self,lower=True,modernize_spelling=None):
        if not hasattr(self,'_freqs') or not self._freqs:
            # print('loading from file')
            if not os.path.exists(self.path_freqs): self.save_freqs_json()
            if not os.path.exists(self.path_freqs): return {}
            with open(self.path_freqs) as f: freqs=Counter(json.load(f))
            self._freqs=freqs
        return filter_freqs(self._freqs,modernize=modernize_spelling,lower=lower)

    @property
    def length(self): return sum(self.freqs().values())

    def tokens(self,lower=True):
        return self.TOKENIZER.__func__(self.txt.lower() if lower else self.txt)
    @property
    def words(self,lower=False):
        tokens=[noPunc(w) for w in self.tokens(lower=lower)]
        return [w for w in tokens if w]
    def sents(self):
        import nltk
        return nltk.sent_tokenize(self.txt)
    @property
    def counts(self,*x,**y): return self.freqs(*x,**y)
    def len():
        return self.num_words()
    @property
    def tfs(self,*x,**y): 
        counts=self.freqs(*x,**y)
        total=self.length
        return dict((w,v/total) for w,v in counts.items())
    @property
    def fpm(self,*x,**y):
        return dict((w,v*1000000) for w,v in self.tfs(*x,**y).items())
    def num_words(self,keys=['num_words','length']):
        for k in keys:
            if k in self.meta:
                return int(self.meta[k])
        return sum(self.counts().values())
    @property
    def words_recognized(self):
        wordlist=get_wordlist(lang=self.lang)
        return [w for w in self.words if w in wordlist or w.lower() in wordlist]
    @property
    def ocr_accuracy(self):
        return float(len(self.words_recognized)) / len(self.words) if len(self.words) else np.nan
    
    def yearbin(self,ybin,as_str=False,zfill=4):
        try:
            binval=self.year//ybin*ybin
            return binval if not as_str else f'{str(binval).zfill(zfill)}-{str(binval+ybin).zfill(zfill)}'
        except Exception:
            return np.nan
    @property
    def halfdecade(self): return self.yearbin(5)
    @property
    def decade(self): return self.yearbin(10)
    @property
    def quartercentury(self): return self.yearbin(25)
    @property
    def halfcentury(self): return self.yearbin(50)
    @property
    def century(self): return self.yearbin(100)
    @property
    def halfdecade_str(self): return self.yearbin(5,as_str=True)
    @property
    def decade_str(self): return self.yearbin(10,as_str=True)
    @property
    def quartercentury_str(self): return self.yearbin(25,as_str=True)
    @property
    def halfcentury_str(self): return self.yearbin(50,as_str=True)
    @property
    def century_str(self): return self.yearbin(100,as_str=True)
    @property
    def title(self): return self.get('title',ish=True)
    @property
    def author(self): return self.get('author',ish=True)
    @property
    def au(self): return to_authorkey(self.author)
    @property
    def ti(self): return ensure_snake(self.shorttitle,lower=False)
    
    @property
    def shorttitle(self,
            puncs=':;.([,!?',
            ok={'Mrs','Mr','Dr'},
            title_end_phrases={
                'edited by','written by',
                'a novel','a tale','a romance','a history','a story',
                'a domestic tale',
                'by the author','by a lady','being some','by Miss','by Mr',
                'an historical','the autobiography',
                'being',
                ' by ',
                ' or'
            },
            replacements={
                ' s ':"'s ",
            },
            replacements_o={"'S ":"'s "}
            ):

        ti=self.title
        ti=ti.strip().replace('—','--').replace('–','-')
        ti=ti.title()
        for x,y in replacements.items(): ti=ti.replace(x.title(),y)
        if any(x in ti for x in puncs):
            for x in puncs:
                o2=ti.split(x)[0].strip()
                if o2 in ok: continue
                ti=o2
        else:
            l=list(title_end_phrases)
            l.sort(key = lambda x: -len(x))
            for x in l:
                # log(x+' ?')
                ti=ti.split(x.title())[0].strip()
        o=ti.strip()
        for x,y in replacements_o.items(): o=o.replace(x,y)
        return o
    
    @property
    def qstr(self):
        return clean_text(f'{self.shorttitle} {self.au}')
    @property
    def qstr_plus(self):
        from urllib.parse import quote_plus
        return quote_plus(self.qstr)
    
    @property
    def shortauthor(self):
        au=clean_text(self.author)
        if not au: return ''
        if not ',' in au: return au
        
        parts=[x.strip() for x in au.split(',') if x.strip() and x.strip()[0].isalpha()]
        if len(parts)==0: return au
        if len(parts)==1: return parts[0]
        oparts=[parts[1]] + [parts[0]]

        # parentheses
        def grabparen(x):
            if '(' in x and ')' in x: return x.split('(',1)[-1].split(')',1)[0].strip()
            return x
        oparts=[grabparen(x) for x in oparts]
        ostr=' '.join(oparts)
        return ostr



    @property
    def prose_or_verse(t):
        for g in [t.medium, t.genre, t.major_genre, t.canon_genre]:	
            if g in {'Prose','Non-Fiction','Fiction','Biography','Oratory'}:
                return 'Prose'
            elif g in {'Verse','Poetry'}:
                return 'Verse'
        if t.corpus.name in {'Chadwyck'}:
            return 'Prose'
        elif t.corpus.name in {'ChadwyckPoetry'}:
            return 'Verse'
        # else:
        # 	txt_verse, txt_prose = t.txt_verse, t.txt_prose
        # 	if txt_verse or txt_prose:
        # 		return 'Verse' if len(txt_verse)>len(txt_prose) else 'Prose'
        return ''
    @property
    def is_prose(self): return self.prose_or_verse=='Prose'
    @property
    def is_verse(self): return self.prose_or_verse=='Verse'

    @property
    def txt_prose(self):
        paras=self.paras_xml
        if not paras and self.is_prose: paras=self.paras
        return '\n\n'.join(paras if paras else [])
    @property
    def txt_verse(self):
        lines=self.lines_xml
        if not lines and self.is_verse: return self.txt
        return '\n'.join(lines if lines else [])

    @property
    def lines_xml(self):
        dom=self.dom
        for x in dom('p'): x.extract()
        return [clean_text(e.text).strip() for e in dom('l')]
    
    @property
    def paras_xml(self):
        dom=self.dom
        for x in dom('l'): x.extract()
        paras=[e.text.strip() for e in dom('p')]
        return [para for para in paras if para]
    @property
    def paras_txt(self):
        paras=[para.strip() for para in self.txt.split('\n\n')]
        return [para for para in paras if para]
    @property
    def paras(self):
        return self.paras_txt
        # paras = self.paras_xml
        # if not paras: paras = self.paras_txt
        # return paras

    @property
    def nltk(self):
        import nltk
        return nltk.Text(self.tokens())
    @property
    def blob(self):
        from textblob import TextBlob
        return TextBlob(self.txt)
    def stanza_paras(self,lang=None,num_proc=1):
        if lang is None: lang=self.lang
        txt=self.txt
        if not txt: return
        yield from pmap_iter(
                do_parse_stanza,
                # self.paras,
                [(para,lang) for para in self.paras],
                desc='Parsing paragraphs with Stanza',
                num_proc=num_proc)
    @property
    def stanza(self,lang=None):
        if lang is None: lang=self.lang
        #return do_parse_stanza(self.txt)
        return list(self.stanza_paras(lang=lang))
    @property
    def spacy(self,lang=None,num_proc=1):
        if lang is None: lang=self.lang
        objs=[(para,lang) for para in self.paras]
        if not objs: return []
        return pmap(
            do_parse_spacy,
            objs,
            desc='Parsing paragraphs with spaCy',
            num_proc=num_proc
        )
    @property
    def minhash(self):
        from datasketch import MinHash
        m = MinHash()
        for word in self.tokens:
            m.update(word.encode('utf-8'))
        return m



    def get_section_class(self,section_class=None):
        if section_class is not None: return section_class
        if self.SECTION_CLASS is not None: return self.SECTION_CLASS
        return TextSection

    def get_section_corpus_class(self,section_corpus_class=None):
        if section_corpus_class is not None: return section_corpus_class
        if self.SECTION_CORPUS_CLASS is not None: return self.SECTION_CORPUS_CLASS
        from lltk.corpus.corpus import SectionCorpus
        return SectionCorpus


    


    # @property
    @property
    def letters(self): return self.sections(_id='letters')
    @property
    def chapters(self): return self.sections(_id='chapters')

    def sections(self,_id=None,section_class=None,section_corpus_class=None,force=False):
        if _id is None: _id=self.SECTION_DIR_NAME
        if force or _id not in self._sections:
            SectionCorpusClass = self.get_section_corpus_class(section_corpus_class)
            self._sections[_id]=SectionCorpusClass(
                # id=os.path.join(self.id, _id),
                id=_id,
                _source=self,
                _id_allows='_/',
                _id=_id
            )
        return self._sections.get(_id)

    @property
    def text_root(self):
        if not issubclass(self.__class__,BaseText): return
        if issubclass(self.__class__,TextSection): return self.source
        return self

    def characters(self,id='default',systems={'booknlp'},**kwargs):
        if type(self._characters)!=dict: self._characters={}
        if not id in self._characters:
            from lltk.model.characters import CharacterSystem
            CS=self._characters[id]=CharacterSystem(self.text_root)
            for sysname in systems:
                system=getattr(self,sysname)
                CS.add_system(system)
        return self._characters[id]

    def get_character_id(self,char_tok_or_id,**kwargs):
        return self.characters().get_character_id(char_tok_or_id,**kwargs)

    @property
    def charsys(self): return self.characters()
    def interactions(self,**kwargs): return self.charsys.interactions(**kwargs)

    @property
    def booknlp(self):
        if self._booknlp is None: self._booknlp={}
        if not self.addr in self._booknlp:
            from lltk.model.booknlp import ModelBookNLP
            self._booknlp[self.addr]=ModelBookNLP(self)
        return self._booknlp[self.addr]
    








class TextSection(BaseText):
    _type='sections'

    @property
    def corpus(self): return self._section_corpus

    # @property
    # def corpus(self): return self._section_corpus
    
    # @property
    # def path(self): return os.path.join(self.source.path,self.id)
    # @property
    # def addr(self): return os.path.join(self.source.addr,self.id)
    # @property
    # def txt(self): return self._txt if self._txt else ''
    # @property
    # def xml(self): return self._xml if self._xml else ''
    @property
    def path_txt(self): return self.get_path_text('txt')
    @property
    def path_xml(self): return self.get_path_text('xml')
    
    
# def get_addr(text_ref,corpus_ref=None,**kwargs):
#     if is_text_obj(text_ref): text_ref = text_ref.addr
#     if is_corpus_obj(corpus_ref): corpus_ref = corpus_ref.id    
#     addr = text_ref
#     if corpus_ref: 
#         corpus_ref_addr = f'_{corpus_ref}/'
#         if not addr.startswith(corpus_ref_addr):
#             addr = corpus_ref_addr + addr
#     return addr








TEXT_CACHE=defaultdict(type(None))
def Text(
        text=None,
        _corpus=None,
        _source=None,
        _force=False,
        _new=False,
        _add=True,
        _init=False,
        _cache=False,
        _use_db=USE_DB,
        # _col_id=COL_ID,
        **_params_or_meta):
    
    global TEXT_CACHE
    if is_text_obj(text) and not _corpus: return text
    params,meta = to_params_meta(_params_or_meta)
    if log>0: log(f'<- {get_imsg(text,_corpus,_source,**meta)}')
    if _new: _force=True
    
    taddr = get_addr_str(
        text=text,
        corpus=_corpus,
        source=_source,
        **_params_or_meta
    )
    if not taddr: raise CorpusTextException(f"cannot get address for {(text,_corpus)}")
    if log>0: log(f'<- addr = {taddr}')

    # set kwargs
    tcorp,tid = to_corpus_and_id(taddr)
    if 'id' in _params_or_meta: del _params_or_meta['id']

    if not _force and is_text_obj(TEXT_CACHE.get(taddr)) and TEXT_CACHE[taddr].is_valid():
        if log>1: log('found in `TEXT_CACHE`')
        t = TEXT_CACHE[taddr]
        if t and meta: t.update(meta)

    elif tcorp and tid:
        if log>1: log(f'Corpus( {tcorp} ).text( {tid} ) ->')
        from lltk.corpus.corpus import Corpus
        t = Corpus(tcorp).text(
            id=tid,
            _source=_source,
            _add=_add,
            _init=_init,
            _cache=_cache,
            _force=_force,
            _new=_new,
            **_params_or_meta
        )
    
    # caching
    if is_text_obj(t): TEXT_CACHE[t.addr] = t
    if log>0: log(f'-> {t}' if is_text_obj(t) else "?")
    
    return t





    