import os

import numpy as np
import pandas as pd

from collections import Counter, defaultdict

from lltk.imports import (
    BAD_TAGS,
    BaseObject,
    COL_ID,
    DIR_SECTION_NAME,
    IDSEP,
    IDSEP_START,
    META_KEY_SEP,
    PATH_CORPUS,
    SetList,
    TEXT_META_DEFAULT,
    TMP_CORPUS_ID,
    YEARKEYS,
    ensure_snake,
    get_wordlist,
    is_hashable,
    just_meta_no_id,
    log,
    noPunc,
    safebool,
    to_numeric_dict,
    zeropunc,
)
from .utils import (
    clean_text,
    filter_freqs,
    get_addr_str,
    is_addr_str,
    is_corpus_obj,
    is_text_obj,
    merge_dict,
    remove_bad_tags,
    save_freqs_json,
    to_corpus_and_id,
    tokenize,
    xml2txt_default,
)


def _open_file(path, **kwargs):
    """Open a file, using gzip if path ends with .gz."""
    import gzip
    if path.endswith('.gz'):
        return gzip.open(path, 'rt', encoding='utf-8', errors='ignore')
    return open(path, encoding='utf-8', errors='ignore', **kwargs)


_PUNKT_LANG = {
    'en': 'english', 'fr': 'french', 'de': 'german', 'es': 'spanish',
    'it': 'italian', 'pt': 'portuguese', 'nl': 'dutch', 'da': 'danish',
    'sv': 'swedish', 'no': 'norwegian', 'fi': 'finnish', 'pl': 'polish',
    'cs': 'czech', 'el': 'greek', 'ru': 'russian', 'tr': 'turkish',
    'et': 'estonian', 'sl': 'slovene',
}

def _lang_to_punkt(lang):
    """Map ISO 639-1 lang code to NLTK punkt language name."""
    if not lang:
        return 'english'
    return _PUNKT_LANG.get(lang, 'english')


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
            _txt=None,
            _xml=None,
            _cache=True,
            **kwargs):
        
        meta = just_meta_no_id(kwargs)
        from lltk import Corpus
        self.corpus=Corpus(_corpus)
        self._section_corpus=_section_corpus
        self._sections={}
        self._rels={}
        self._meta={}
        self._meta_hydrated=False
        self._txt=_txt
        self._xml=_xml
        self._node=None
        self._dom=None
        self._freqs=None
        self._minhash=None
        self._characters=None
        self._booknlp=None
        self._txt_offsets=None
        self._source=_source
        if id is None:
            id = self.corpus.get_text_id(id, _source=_source, **meta)
            log(f'blank id set to {id}')
        self.id=id
        self.corpus.add_text(self)
        self._meta=self.ensure_id(merge_dict(
            TEXT_META_DEFAULT,
            self.META,
            self._meta,
            meta,
        ))



        
    def __repr__(self):  return self.node
    

    @property
    def addr(self): return f'{IDSEP_START}{self.corpus.id}{IDSEP}{self.id}'

    @property
    def node(self, force=True):
        if force or not self._node:
            addr = self.addr
            au = self.au
            ti = self.ti
            if au and ti:
                ol = [f'{au}, {ti.replace("_"," ").title()[:50].strip()}']
                yr = self.year
                if safebool(yr): ol.append(f' ({int(yr)})')
                ol.append(f' [{addr}]')
                self._node = ''.join(ol)
            elif addr:
                self._node = f'({addr})'
        return self._node

            
    
    @property
    def idx(self): return self.id.split('/')[-1]
    
    @property
    def col_addr(self): return self.corpus.col_addr
    @property
    def col_id(self): return self.corpus.col_id

    def ensure_id_addr(self,*x,**y): return self.ensure_id(*x,**y)

    def ensure_id(self,
            meta=None,
            col_id=COL_ID,
            col_corpus='_corpus',
            col_addr='_id',
            sep=META_KEY_SEP,
            allow_sep=True):
        if meta is None: meta=self._meta
        items = [
            (k,v)
            for k,v in sorted(meta.items())
            if k and v and k.count(sep)<=(1 if allow_sep else 0)
            and k not in {col_id,col_corpus}
        ]
        return {
            col_addr:self.addr,
            col_corpus:self.corpus.id,
            col_id:self.id,

            **{k:v for k,v in items if k.startswith(col_id+META_KEY_SEP)},
            **{k:v for k,v in items if not k.startswith(col_id+META_KEY_SEP)},
        }

    @property
    def rels(self):
        return self._rels if self._rels else {}

    ####################################################################
    # GETTING ATTRIBUTES
    ####################################################################

    def __getitem__(self, key): return self.get(key)

    def __getattr__(self, name):
        if name.startswith('_'): raise AttributeError(name)
        if name.startswith('path_'): return self.get_path(name)

        res = self.get(name)
        if res is not None: return res

        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        
    def __setitem__(self, key, value): return self.update({key:value})
    def __delitem__(self, key):
        if key in self._meta: del self._meta[key]
    def __iter__(self): return iter(self.meta.items())
    def __len__(self): return self.n_words

    def _corpus_meta_row(self):
        """Look up this text's row. Tries MetaDB first, falls back to corpus DataFrame."""
        # Fast path: DuckDB indexed lookup (returns dict with meta JSON unpacked)
        try:
            from lltk.db.metadb import metadb
            row = metadb.get(self.corpus.id, self.id)
            if row:
                return row
        except Exception as e:
            pass
        # Fallback: corpus load_metadata() — can be slow for large corpora with enrichment
        # Only use if DB lookup failed (no DB, text not in DB, etc.)
        try:
            cached = self.corpus._metadf
            if cached is not None and self.id in cached.index:
                row = cached.loc[self.id]
                return {k: v for k, v in row.items() if pd.notna(v)}
            # Not cached — load (will cache for subsequent texts)
            df = self.corpus.load_metadata()
            if df is not None and self.id in df.index:
                row = df.loc[self.id]
                return {k: v for k, v in row.items() if pd.notna(v)}
        except Exception:
            pass
        return {}

    def _hydrate_meta(self):
        """Lazily populate _meta from corpus DataFrame on first access."""
        if self._meta_hydrated:
            return
        self._meta_hydrated = True
        crow = self._corpus_meta_row()
        if crow:
            # corpus row is base layer; existing _meta (local overrides) wins
            self._meta = self.ensure_id(merge_dict(
                TEXT_META_DEFAULT,
                self.META,
                crow,
                self._meta,
            ))

    def get(self, key, default=None, ish=False, ish_all=None, **kwargs):
        self._hydrate_meta()
        key = str(key)
        if key.startswith('_'): ish = False
        if key.endswith('_l'): return self.meta_l(key[:-2], ish=True, **kwargs)
        if key.endswith('_1'): return self.meta_1(key[:-2], ish=True, **kwargs)
        if key.endswith('_'): return self.meta_(key[:-1], ish=True, **kwargs)

        meta = self._meta
        if not ish:
            return meta.get(key, default)

        vals = []
        hvals = set()
        for k in meta:
            if k.startswith(key):
                l = meta[k]
                if type(l) != list: l = [l]
                for v in l:
                    if v and (not is_hashable(v) or v not in hvals):
                        if is_hashable(v): hvals |= {v}
                        vals.append(v)
        if vals:
            o = (vals if ish_all else vals[0])
            return o if o is not None else default
        return default
        


    @property
    def xml2txt_func(self): return self.XML2TXT.__func__
    
    

    ####################################################################
    # PATHS 
    ####################################################################


    

    def get_path(self, part, **kwargs):
        if part.startswith('path_'): part = part[5:]
        if not self.corpus: return ''
        res = getattr(self.corpus, 'path_' + part, None)
        if res:
            ext = getattr(self.corpus, 'ext_' + part, None)
            return os.path.join(res, self.id) + (ext or '')
        return ''

    @property
    def path(self):
        if self.corpus.path_texts and self.id:
            return os.path.join(self.corpus.path_texts,self.id)
        else:
            return ''
    @property
    def path_rel(self): return self.relpath()
    @property
    def rel_path(self): return self.relpath(reverse=True)

    def relpath(self,path=None,text=None,reverse=False):
        a = path if path else self.path
        b = text.path if text else PATH_CORPUS
        if reverse: a,b=b,a
        return os.path.relpath(a,b)

    @property
    def path_meta_json(self): return os.path.join(self.path,'meta.json')

    def prosodic(self, cached=True, **kwargs):
        """Return a prosodic TextModel for this text.

        If `cached` is True (default) and a pre-parsed copy exists under
        `{corpus.path_prosodic}/{text.id}/`, load it — this is fast and
        includes all parsed/scanned state from `lltk prosodic-parse`.

        Otherwise (or if `cached=False`), build a fresh TextModel from
        `self.txt`. The fresh model has syllable/line structure but no
        metrical parse — call `.parse()` on it to scan meter on the fly.

        Returns None if the text has no `.txt` and no cached parse.

        kwargs are forwarded to `prosodic.Text(...)` for fresh construction
        (e.g. `lang='en'`, `syntax=False`).
        """
        from lltk.tools.prosodic_tools import _load_prosodic
        prosodic = _load_prosodic()
        if cached:
            d = self.path_prosodic
            if d and os.path.exists(os.path.join(d, 'meta.json')):
                return prosodic.TextModel.load(d)
        txt = self.txt
        if not txt:
            return None
        return prosodic.Text(txt, **kwargs)
    
    def task_dir(self, task_name: str) -> str:
        """Return the directory for a rich-JSON task output.

        Layout: {corpus.path}/tasks/{task_name}/{text.id}/
        Files inside are named {model_slug}.json.
        """
        return os.path.join(self.corpus.path_tasks, task_name, self.id)

    def task(self, task_name: str, source: str = None):
        """Read a rich-JSON task result. Returns dict or None.

        With no source, returns the most recently modified result.
        """
        d = self.task_dir(task_name)
        if not os.path.isdir(d):
            return None
        if source:
            p = os.path.join(d, f'{source}.json')
            if not os.path.isfile(p):
                return None
            import json
            with open(p) as f:
                return json.load(f)
        files = [f for f in os.listdir(d) if f.endswith('.json')]
        if not files:
            return None
        best = max(files, key=lambda f: os.path.getmtime(os.path.join(d, f)))
        import json
        with open(os.path.join(d, best)) as f:
            return json.load(f)

    def task_sources(self, task_name: str) -> list:
        """List available model sources for a task (e.g. ['qwen36-27b', 'gemini-25-pro'])."""
        d = self.task_dir(task_name)
        if not os.path.isdir(d):
            return []
        return sorted(f[:-5] for f in os.listdir(d) if f.endswith('.json'))




    def update(self, meta={}, **metad):
        if meta or metad:
            self._meta = {**self._meta, **meta, **metad}
            self._node = None


    
    ####################################################################
    # Metadata 
    ####################################################################
    
    @property
    def qdb(self): return self.corpus.qdb
    
    def query(self,*x,**y): return {}

    def metadata(self, meta={}, to_numeric=True, sep=META_KEY_SEP, **kwargs):
        self._hydrate_meta()
        imeta = merge_dict(TEXT_META_DEFAULT, self.META, self._meta, meta)
        ometa = self.ensure_id(imeta, allow_sep=False)
        self._meta = {k: v for k, v in ometa.items() if sep not in k}
        if to_numeric:
            ometa = to_numeric_dict(ometa)
        return ometa
    


    @property
    def meta(self): return self.metadata()
    @property
    def _meta_(self): return {k:v for k,v in self._meta.items() if not META_KEY_SEP in k}

    def meta_(self, key='', ish=True, **kwargs):
        self._hydrate_meta()
        meta = self._meta
        o = []
        for k, v in meta.items():
            if (ish and k.startswith(key)) or (not ish and k == key):
                o.append((self, k, v))
        return o

    def meta_l(self, *args, **kwargs):
        return SetList([v for t, k, v in self.meta_(*args, **kwargs)])

    def meta_1(self, *args, **kwargs):
        l = self.meta_l(*args, **kwargs)
        return l[0] if l else None

    def id_is_valid(self,*x,**y):
        if self.id in {None,'','None'}: return False 
        return True
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
    # These properties delegate to get(), which is the single hydration gateway.
    @property
    def title(self): return str(self.get('title') or '')
    @property
    def author(self): return str(self.get('author') or '')
    @property
    def genre(self): return str(self.get('genre') or '')
    @property
    def genre_raw(self): return str(self.get('genre_raw') or '')
    @property
    def genre_enriched_source(self): return str(self.get('genre_enriched_source') or '')
    @property
    def title_norm(self): return str(self.get('title_norm') or '')
    @property
    def author_norm(self): return str(self.get('author_norm') or '')
    @property
    def n_words(self):
        v = self.get('n_words')
        return int(v) if v and str(v) != 'nan' else 0
    @property
    def is_translated(self): return bool(self.get('is_translated'))
    @property
    def original_lang(self): return self.get('original_lang') or ''
    @property
    def lang_detected(self): return self.get('lang_detected') or ''
    @property
    def au(self):
        from lltk.corpus.utils import to_authorkey
        return to_authorkey(self.author)
    @property
    def ti(self): return ensure_snake(self.shorttitle,lower=False)

    @property
    def years(self,keys=YEARKEYS):
        years = SetList()
        for trykey in keys:
            for val in self[trykey+'_l']:
                v = zeropunc(str(val))[:4]
                if v.isdigit():
                    vnum = pd.to_numeric(v,errors='coerce')
                    if safebool(vnum):
                        years.append(vnum)
        if not years: return []
        years.sort()
        return years
        # return self._years

    @property
    def year(self):
        v = self.get('year')
        if v is not None:
            v = pd.to_numeric(v, errors='coerce')
            if pd.notna(v):
                return v
        years = self.years
        if len(years)==0: return np.nan
        if len(years)==1: return years[0]
        if len(years)==2: return years[0]
        if len(years)==3: return years[1]
        if len(years)==4: return years[1]
        imedian = len(years) // 2
        return years[imedian]


    
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
        if not ti: return ''
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
    def matches(self):
        return set(self._rels.keys())

    @property
    def match_group(self):
        """Return DataFrame of all texts in this text's match group from CH."""
        try:
            from lltk.db.metadb import metadb
            return metadb.get_group(self.addr)
        except Exception:
            return None

    @property
    def match_group_texts(self):
        """Return text objects for all texts in this text's match group.
        Falls back to [self] if no match group exists."""
        mg = self.match_group
        if mg is None:
            return [self]
        from lltk.corpus.corpus import Corpus
        texts = []
        for _, row in mg.iterrows():
            _id = row['_id']
            parts = _id.lstrip('_').split('/', 1)
            if len(parts) != 2:
                continue
            try:
                t = Corpus(parts[0]).text(parts[1])
                texts.append(t)
            except Exception:
                continue
        return texts or [self]
    


    
    
    _linked_cache = {}

    def linked(self, target_corpus_id=None, **kwargs):
        if target_corpus_id is not None:
            from lltk.corpus.utils import load
            if not hasattr(self.corpus, 'LINKS') or target_corpus_id not in self.corpus.LINKS:
                return []
            my_col, their_col = self.corpus.LINKS[target_corpus_id]

            # Get this text's link value
            my_val = self.get(my_col)
            if my_val is None:
                my_meta = self.corpus.load_metadata()
                if my_meta is not None and len(my_meta) and self.id in my_meta.index:
                    my_val = my_meta.at[self.id, my_col] if my_col in my_meta.columns else None
            if my_val is None:
                return []

            transform = getattr(self.corpus, 'LINK_TRANSFORMS', {}).get(my_col)
            if transform:
                my_val = transform(my_val)

            # Build/get cached lookup: link_value → [text_id, ...]
            cache_key = (self.corpus.id, target_corpus_id)
            if cache_key not in BaseText._linked_cache:
                target_corpus = load(target_corpus_id)
                if target_corpus is None:
                    return []
                target_meta = target_corpus.load_metadata()
                if target_meta is None or not len(target_meta):
                    return []
                # Build reverse index
                from collections import defaultdict
                lookup = defaultdict(list)
                if their_col == target_meta.index.name:
                    for text_id in target_meta.index:
                        lookup[text_id].append(text_id)
                elif their_col in target_meta.columns:
                    for text_id, val in zip(target_meta.index, target_meta[their_col]):
                        if val and str(val).strip():
                            lookup[val].append(text_id)
                else:
                    return []
                BaseText._linked_cache[cache_key] = (target_corpus, target_meta, dict(lookup))

            target_corpus, target_meta, lookup = BaseText._linked_cache[cache_key]
            matched_ids = lookup.get(my_val, [])
            results = []
            for text_id in matched_ids:
                meta = {}
                if text_id in target_meta.index:
                    meta = target_meta.loc[text_id].to_dict()
                results.append(target_corpus.text(text_id, **meta))
            return results
        return set(self.links(**kwargs).keys())
    def links(self,**kwargs):
        return self.gdb.get_links(self.addr)
    def edges(self,**kwargs):
        return self.gdb.get_edges(self.addr)
    

    @property
    def source(self):
        if self._source is not None: return Text(self._source)



    ####################################################################
    # Txt/xml 
    ####################################################################


    # load text?
    
    @property
    def txt(self):
        if not self._txt:
            self._txt = self.text_plain()
        return clean_text(self._txt) if self._txt else ''

    @property
    def xml(self):
        if self._xml: return self._xml
        path_xml = self.path_xml
        if not path_xml or not os.path.exists(path_xml):
            src = getattr(self, '_source', None)
            if src is not None and hasattr(src, 'path_xml') and os.path.exists(src.path_xml):
                path_xml = src.path_xml
            else:
                return ''
        with _open_file(path_xml) as f: return clean_text(f.read())
    
    
    
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



    def text_plain(self, force_xml=None):
        """
        This function returns the plain text file. You may want to modify this.
        """
        # Return plain text version if it exists
        if self.path_txt and os.path.exists(self.path_txt) and not force_xml:
            with _open_file(self.path_txt) as f:
                return f.read()
        # Otherwise, load from XML?
        if os.path.exists(self.path_xml): return self.XML2TXT.__func__(self.path_xml)
        return ''


    
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
            import orjson
            with open(self.path_freqs, 'rb') as f: freqs=Counter(orjson.loads(f.read()))
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
    @property
    def lang(self):
        """Best-effort language as ISO 639-1 two-letter code."""
        from lltk.db.metadb import normalize_lang
        meta = self._meta or {}
        for key in ('lang', 'language', 'language_1', 'estc_lang', 'language1'):
            val = meta.get(key)
            if val and str(val).strip() and str(val) != 'nan':
                result = normalize_lang(str(val).strip())
                if result:
                    return result
        return None

    def sents(self, lang=None):
        import nltk
        return nltk.sent_tokenize(self.txt, language=_lang_to_punkt(lang or self.lang))
    @property
    def counts(self,*x,**y): return self.freqs(*x,**y)
    def len(self):
        return self.n_words
    @property
    def tfs(self,*x,**y): 
        counts=self.freqs(*x,**y)
        total=self.length
        return dict((w,v/total) for w,v in counts.items())
    @property
    def fpm(self,*x,**y):
        return dict((w,v*1000000) for w,v in self.tfs(*x,**y).items())
    

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

    def minhash(self,cache=True,force=False):
        from datasketch import MinHash,LeanMinHash
        from base64 import b64decode,b64encode

        qkey=self.addr
        db = self.cachedb('minhash',engine='sqlite')
        buf64 = db.get(qkey) if not force and cache else None
        if buf64 is not None:
            self._minhash = LeanMinHash.deserialize(b64decode(buf64))
        else:
            if not os.path.exists(self.path_txt): return
            words = self.words
            if words:
                m = MinHash(num_perm=128*2)
                for word in words: m.update(word.encode('utf-8'))
                self._minhash = lm = LeanMinHash(m)

                if cache:
                    buf = bytearray(lm.bytesize())
                    lm.serialize(buf)
                    buf64=b64encode(buf)
                    db.set(qkey,buf64)
        return self._minhash


    def get_section_class(self,section_class=None):
        if section_class is not None: return section_class
        if self.SECTION_CLASS is not None: return self.SECTION_CLASS
        return TextSection

    def get_section_corpus_class(self,section_corpus_class=None):
        if section_corpus_class is not None: return section_corpus_class
        if self.SECTION_CORPUS_CLASS is not None: return self.SECTION_CORPUS_CLASS
        from lltk.corpus.corpus import SectionCorpus
        return SectionCorpus


    


    @property
    def letters(self): return self.sections(_id='letters')
    @property
    def chapters(self): return self.sections(_id='chapters')

    @property
    def paragraphs(self):
        from lltk.corpus.corpus import ParagraphSectionCorpus
        return self.sections(_id='paragraphs', section_corpus_class=ParagraphSectionCorpus)

    def passages(self, n=500, force=False):
        from lltk.corpus.corpus import PassageSectionCorpus
        key = f'passages_n{n}'
        if force or key not in self._sections:
            self._sections[key] = PassageSectionCorpus(
                n=n,
                id=key,
                _source=self,
                _id_allows='_/',
                _id=key
            )
        return self._sections[key]

    def sections(self,_id=None,section_class=None,section_corpus_class=None,force=False):
        if _id is None: _id=self.SECTION_DIR_NAME
        if force or _id not in self._sections:
            SCC = section_corpus_class or self.get_section_corpus_class()
            self._sections[_id]=SCC(
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



    @property
    def booknlp(self):
        if self._booknlp is None: self._booknlp={}
        if not self.addr in self._booknlp:
            from lltk.model.booknlp import ModelBookNLP
            self._booknlp[self.addr]=ModelBookNLP(self)
        return self._booknlp[self.addr]



    ###






class TextSection(BaseText):
    _type='sections'
    PARA_TAG = 'p'
    VERSE_TAG = 'l'

    def __init__(self, id=None, _section_corpus=None, _source=None, **kwargs):
        kwargs['_corpus'] = _source.corpus if _source else None
        kwargs['_source'] = _source
        kwargs['_section_corpus'] = _section_corpus
        super().__init__(id=id, **kwargs)
        # override corpus set by BaseText.__init__
        self.corpus = _section_corpus
    @property
    def path_txt(self): return os.path.join(self.path, 'text.txt')
    @property
    def path_xml(self): return os.path.join(self.path, 'text.xml')

    @property
    def txt(self):
        # try in-memory text first
        if self._txt: return self._txt
        # try cached file
        if self.path_txt and os.path.exists(self.path_txt):
            with _open_file(self.path_txt) as f: return f.read()
        # try extracting from XML
        return self.txt_from_xml()

    def txt_from_xml(self):
        xml = self.xml
        if not xml: return ''
        import bs4
        dom = bs4.BeautifulSoup(xml, 'lxml')
        source = self._source or self
        bad_tags = getattr(source, 'BAD_TAGS', BAD_TAGS)
        dom = remove_bad_tags(dom, bad_tags)

        # try paragraphs
        paras = []
        for p in dom(self.PARA_TAG):
            text = p.get_text().strip().replace('\n', ' ')
            while '  ' in text: text = text.replace('  ', ' ')
            if text: paras.append(text)

        if not paras:
            # try verse lines
            for l in dom(self.VERSE_TAG):
                text = l.get_text().strip()
                if text: paras.append(text)

        if not paras:
            # last resort
            text = dom.get_text(separator='\n').strip()
            return clean_text(text)

        return clean_text('\n\n'.join(paras))

    def freqs(self, lower=True, modernize_spelling=None):
        if not self._freqs:
            txt = self.txt
            if not txt: return Counter()
            tokenizer = self.TOKENIZER.__func__
            tokens = tokenizer(txt.lower() if lower else txt)
            self._freqs = Counter(tokens)
        return filter_freqs(self._freqs, modernize=modernize_spelling, lower=lower)


TEXT_CACHE = {}


def Text(id=None, _corpus=None, _source=None, **kwargs):
    """Factory: resolve an address string to a cached text object.

    Common usage: ``Text('_corpus/id')`` -- parse the address, look up the
    corpus, and return a (possibly cached) text object.

    Parameters
    ----------
    id : str or BaseText
        An address string like ``'_corpus/id'``, or an already-resolved text.
    _corpus : str, optional
        Explicit corpus name (used when *id* is a bare ID without prefix).
    _source : str or BaseText, optional
        Source text for linking.
    **kwargs
        Extra keyword arguments forwarded to ``Corpus.text()``.
    """

    # Already a text object -- return as-is.
    if is_text_obj(id):
        return id

    # Resolve the address string.
    if is_addr_str(id):
        taddr = id
    elif isinstance(id, str) and _corpus:
        taddr = f"{IDSEP_START}{_corpus}{IDSEP}{id}"
    else:
        taddr = get_addr_str(text=id, corpus=_corpus, source=_source)

    if not taddr or not is_addr_str(taddr):
        return NullText()

    # Cache hit?
    cached = TEXT_CACHE.get(taddr)
    if cached is not None and is_text_obj(cached):
        return cached

    # Parse into corpus + id, then delegate to Corpus.text().
    tcorp, tid = to_corpus_and_id(taddr)
    if not tcorp or not tid:
        return NullText()

    from lltk.corpus.corpus import Corpus
    t = Corpus(tcorp).text(id=tid, _source=_source, **kwargs)

    # Cache and return.
    if is_text_obj(t) and t.id_is_valid():
        TEXT_CACHE[t.addr] = t
        return t

    return NullText()


class NullText(BaseText):
    def id_is_valid(self, *x, **y): return False



