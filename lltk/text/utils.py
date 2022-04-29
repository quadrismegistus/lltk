from lltk.imports import *

def get_prop_ish(meta,key,before_suf='|'):
    keys=[k for k in meta.keys() if k.startswith(key)]
    numkeys=len(keys)
    if numkeys==0: return None
    if numkeys==1: 
        keyname = keys[0]
    else:
        log.warning(f'more than one metadata key begins with "{key}": {keys}.')
        keyname=keys[0]
        log.warning(f'using key: {keyname}')
    
    res = meta.get(keyname)

    if before_suf and type(res)==str and res:
        res = res.split('|')[0]
    
    return res




def dbget(addr):
    c,i = to_corpus_and_id(addr)

    from lltk.corpus.corpus import Corpus
    return Corpus(c).db().get(i,{})


def yield_addrs(meta,addr_prefix='_addr_', id_prefix='_id_',ok_corps=set(),*args,**kwargs):
    od={}
    for k,v in meta.items():
        rel,addr,corpid = '','',''
        if k.startswith(addr_prefix):
            rel=k
            addr = v
            corpid=k[len(addr_prefix):]
        elif k.startswith(id_prefix):
            corpid=k[len(id_prefix):]
            rel=addr_prefix+corpid
            addr=f'{IDSEP_START}{corpid}{IDSEP}{v}'
        if rel and addr and corpid and (not ok_corps or corpid in set(ok_corps)) and not rel in od and corpid != meta.get('_corpus'):
            od[rel]=addr
    yield from od.items()

def yield_ids(meta,*args,**kwargs):
    for addr_name, addr in yield_addrs(meta,*args,**kwargs):
        idcorp,idx = to_corpus_and_id(addr)
        id_name = f'id__{idcorp}'
        yield id_name,idx


# def get_addr_str(addr=None,corpus=None,**kwargs):
# 	if is_text_obj(addr): addr=addr.addr
# 	if not addr: addr=''
# 	if corpus and addr:
# 		if is_corpus_obj(corpus): corpus=corpus.id
# 		addr=f'_{corpus}/{addr}'
# 	return addr


META_KEYS_USED_IN_AUTO_IDX = {
    'author',
    'title',
    'edition',
    'year',
    'publisher',
    'vol',
}

# def get_idx_from_meta(meta,sep_kv='=',sep='/',hidden='_'):
#     o=[]
#     for k,v in sorted(meta.items()):
#         if k and k[0]!=hidden:
#             o.append(f'{k}{sep_kv}{v}')
#     ostr=sep.join(o)
#     return get_idx(ostr)

def get_idx_from_meta(
        meta,
        keys=META_KEYS_USED_IN_AUTO_IDX,
        sep_kv='=',
        sep='/',
        hidden='_'):
    o=[]
    for k in keys:
        v = get_prop_ish(meta,k)
        if v is not None:
            o.append(f'{k}{sep_kv}{v}')
    ostr=sep.join(o)
    return get_idx(ostr) if o else None

def get_idx_from_int(i=None,numzero=5,prefstr='T'):
    if not i:
        numposs=int(f'1{"0"*5}')
        i=random.randint(1,numposs-1)
    return f'{prefstr}{i:0{numzero}}'


def get_idx(
        id=None,
        i=None,
        allow='_/.-:,=',
        prefstr='T',
        numzero=5,
        use_meta=True,
        force_meta=True,
        **meta):
    
    if is_text_obj(id): return id.id
    id1=id
    # already given?
    if safebool(id):
        if type(id)==str:
            id = ensure_snake(
                str(id),
                allow=allow,
                lower=False
            )
            if log.verbose>2:
                if log.verbose>0: log(f'id set via `id` str: {id1} -> {id}')
        
        elif type(id) in {int,float}:
            id = get_idx_from_int(int(id))
            if log.verbose>1: log(f'id set via `id` int: {id1} -> {id}')
        
        else:
            raise Exception(f'What kind of ID is this? {type(id1)}')

    else:
        if meta and (force_meta or (use_meta and not i)):
            id = get_idx_from_meta(meta)
            if log.verbose>1: log(f'id set via `meta`: {id1} -> {id}')
        elif i:
            id = get_idx_from_int(i,numzero=numzero,prefstr=prefstr)
            if log.verbose>1: log(f'id set via `i` int: {id1} -> {id}')

    
    if not id:
        id = get_idx_from_int(numzero=numzero,prefstr=prefstr) # last resort
        if log.verbose>1: log(f'id set via random int: {id1} -> {id}')
    
    return id
    
def get_addr(*x,**y): return get_addr_str(*x,**y)
# def get_id(*x,**y): return get_id_str(*x,**y)
def is_addr(*x,**y): return id_is_addr(*x,**y)

def get_addr_str(text=None,corpus=None,source=None,**kwargs):
    if log.verbose>3: log(f'<- {get_imsg(text,corpus,source,**kwargs)}')

    # rescue via source?
    if text is None:
        if source is not None: return get_addr_str(source,corpus,None,**kwargs)
        return get_addr_str(
            get_idx(
                i=len(corpus._textd)+1 if corpus else None,
                **kwargs),
                corpus,
            **kwargs
        )
    
    # corpus set? if not, work to get it so
    if not corpus:
        if is_text_obj(text): return text.addr
        if type(text)==str:
            cx,ix = to_corpus_and_id(text)
            if cx and ix: return text
            if ix: return get_addr_str(ix,TMP_CORPUS_ID,**kwargs)
        return get_addr_str(text,TMP_CORPUS_ID,**kwargs)

    # now can assume we have both corpus and text
    corpus = corpus.id if is_corpus_obj(corpus) else str(corpus)
    idx=get_idx(text)
    cpref=IDSEP_START + corpus + IDSEP
    o=cpref + idx if not idx.startswith(cpref) else idx
    if log.verbose>3: log(f'-> {o}')
    return o

def get_imsg(text=None,corpus=None,source=None,**kwargs):
    o=[]
    if text: o.append(f'id = {text}')
    if corpus: o.append(f'corpus = {corpus}')
    if source: o.append(f'source = {source}')
    if kwargs:
        #oo=', '.join([f'{k} = {v}' for k,v in kwargs.items()])
        oo=pf(kwargs)
        o.append(f'kwargs = dict({oo})')
    return ', '.join(o) if o else ''

def get_id_str(text=None,corpus=None,source=None,**kwargs):
    if log.verbose>3: log(f'<- {get_imsg(text,corpus,source,**kwargs)}')
    addr=get_addr_str(text,corpus,source,**kwargs)
    o=to_corpus_and_id(addr)[-1]
    if log.verbose>3: log(f'-> {o}')
    return o

def id_is_addr(idx):
    return type(idx)==str and idx and idx.startswith(IDSEP_START) and IDSEP in idx

def to_corpus_and_id(idx):
    if id_is_addr(idx):
        return tuple(idx[len(IDSEP_START):].split(IDSEP,1))
    return ('',idx)

def unhtml(raw_html):
    import re
    # as per recommendation from @freylis, compile once only
    CLEANR = re.compile('<.*?>') 
    cleantext = re.sub(CLEANR, '', raw_html)
    return cleantext


def grab_tag_text(dom,tagname,limit=None,sep_tag=' || ',sep_ln=' | '):
    tagnames = [tagname] if type(tagname) not in {tuple,list} else tagname
    tags = [
        tag
        for tagname in tagnames
        for tag in dom(tagname)
    ]
    
    tags_txt = [
        unhtml(str(tag)).strip().replace('\n',sep_ln)
        for tag in tags
    ]

    otxt = sep_tag.join(tags_txt).strip()

    return otxt


def read_df_annos(fn,anno_exts=ANNO_EXTS,id_key='id',**kwargs):
    fnbase,fnext = os.path.splitext(fn)
    exts = anno_exts + [fnext]
    fns=[(fnbase+anno_ext,anno_ext) for anno_ext in anno_exts] + [(fn,fnext)]
    fns=[x for x in fns if os.path.exists(x[0])]
    o=[read_df(fn).fillna('').assign(ext=ext, ext_i=exts.index(ext)) for fn,ext in fns]
    o=[x for x in o if type(x)==pd.DataFrame]
    odf=pd.concat(o) if o else pd.DataFrame()
    if len(odf) and id_key in set(odf.columns):
        ol=[]
        for id,iddf in odf.groupby(id_key):
            iddf=iddf.sort_values('ext_i',ascending=False)
            idd = merge_dict(*iddf.to_dict('records'))
            ol.append(idd)
        odf=pd.DataFrame(ol)
    return odf

def read_df_anno(fn,anno_exts=ANNO_EXTS,**kwargs) :
    fnbase,fnext = os.path.splitext(fn)
    for anno_ext in anno_exts:
        if fnext != anno_ext:
            anno_fn = fnbase + anno_ext
            if os.path.exists(anno_fn):
                return read_df(anno_fn,**kwargs)
    if os.path.exists(fn): return read_df(fn,**kwargs)
    return None


# def load_with_anno(fn,anno_exts=['xlsx','xls','csv'],suffix='anno',**kwargs):

def load_with_anno(fn,anno_exts=ANNO_EXTS,suffix='anno',**kwargs):
    fnbase,fnext = os.path.splitext(fn)
    for anno_ext in anno_exts:
        if fnext != anno_ext:
            anno_fn = fnbase + anno_ext
            if os.path.exists(anno_fn):
                return read_df(anno_fn)
    return pd.DataFrame()

def load_with_anno_or_orig(fn,**kwargs):
    df_anno = load_with_anno(fn,**kwargs)
    if len(df_anno): return df_anno
    o=read_df(fn)
    if o is not None and len(o): return o
    return pd.DataFrame()

def get_anno_fn_if_exists(
        fn,
        anno_exts=['.anno.xlsx','.anno.xls','.anno.csv','.xlsx','.xls'],
        return_fn_otherwise=True,
        **kwargs):
    if type(fn)!=str: return fn
    fnbase,fnext = os.path.splitext(fn)
    for anno_ext in anno_exts:
        if fnext != anno_ext:
            anno_fn = fnbase + anno_ext
            if os.path.exists(anno_fn):
                return anno_fn
    return fn if return_fn_otherwise else ''




def merge_read_dfs_iter(fns_or_dfs, opt_exts=[]):
    odd=defaultdict(dict)
    for fn_or_df in fns_or_dfs:
        yield from readgen(fn_or_df,progress=False)

        if type(fn_or_df)==str:
            for opt_ext in opt_exts:
                newfn = os.path.splitext(fn_or_df)[0]+opt_ext
                if os.path.exists(newfn):
                    # log(f'Overwriting dataframe with data from: {newfn}')
                    yield from readgen(newfn,progress=False)
            

def merge_read_dfs_dict(fns_or_dfs, opt_exts=[], on='id', fillna=None):
    odd=defaultdict(dict)
    if type(fns_or_dfs) not in {list,tuple}: fns_or_dfs=[fns_or_dfs]
    iterr=merge_read_dfs_iter(fns_or_dfs, opt_exts=opt_exts)
    for dx in iterr:
        if on not in dx: continue
        onx = dx[on]
        for k,v in dx.items():
            if v is not None and (type(v)!=str or len(v)):
                if fillna is not None and v is np.nan: v=fillna
                odd[onx][k]=v
    return odd

def merge_read_dfs(fns_or_dfs, opt_exts=[], on='id'):
    odd = merge_read_dfs_dict(fns_or_dfs, opt_exts=opt_exts, on=on)
    index = list(odd.keys())
    rows = [odd[i] for i in index]
    odf=pd.DataFrame(rows)
    
    if on in set(odf.columns):
        odf=odf[~odf[on].isna()]
        odf=odf[~odf[on].isnull()]
        odf=odf.set_index(on)
    return odf


def merge_read_dfs_anno(fns_or_dfs, on='id'):
    return merge_read_dfs(fns_or_dfs, opt_exts=ANNO_EXTS, on=on)




def do_parse_stanza(obj):
    txt,lang=obj
    nlp=get_stanza_nlp(lang=lang)
    return nlp(txt)

nlpd={}
def get_stanza_nlp(lang='en'):
    global nlpd
    if not lang in nlpd:
        import stanza
        try:
            nlpd[lang] = stanza.Pipeline(lang)
        except FileNotFoundError:
            stanza.download(lang)
            nlpd[lang] = stanza.Pipeline(lang)
    nlp=nlpd[lang]
    return nlp
def get_spacy_nlp(lang='en'):
    global nlpd
    langsp='spacy_'+lang
    model=f"{lang}_core_web_sm"
    if not langsp in nlpd: 
        import spacy
        try:
            nlp = spacy.load(model)
        except OSError:
            os.system('python -m spacy download {model}')
            try:
                nlp = spacy.load(model)
            except OSError:
                return
        nlpd[langsp]=nlp
    else:
        nlp=nlpd[langsp]
    return nlp


def do_parse_spacy(obj):
    txt,lang=obj
    nlp=get_spacy_nlp(lang=lang)
    return nlp(txt)
    


## Spelling
V2S = None
def variant2standard():
    global V2S
    if not V2S:
        V2S = dict((d['variant'],d['standard']) for d in tools.tsv2ld(SPELLING_VARIANT_PATH,header=['variant','standard','']))
    return V2S

def standard2variant():
    v2s=variant2standard()
    d={}
    for v,s in list(v2s.items()):
        if not s in d: d[s]=[]
        d[s]+=[v]
    return d



def phrase2variants(phrase):
    s2v=standard2variant()
    words = phrase.split()
    word_opts = [[s]+s2v[s] for s in words]
    word_combos = list(tools.product(*word_opts))
    phrase_combos = [' '.join(x) for x in word_combos]
    return phrase_combos
###




def load_english():
    return get_wordlist(lang='en')







### Functions to be mapped


def get_dtm_freqs(obj):
    import ujson as json
    path,words,dmeta = obj
    with open(path,encoding='utf-8',errors='ignore') as f:
        counts=json.load(f)
    total=sum(counts.values())
    dx={
        **dict((w,c) for w,c in counts.items() if w in words),
        # **{'_total':total},
        **dmeta
    }
    return dx


def do_preprocess_txt(obj):
    ifnfn, ofnfn, func = obj
    otxt = func(ifnfn)
    odir=os.path.dirname(ofnfn)
    if not os.path.exists(odir):
        try:
            os.makedirs(odir)
        except Exception:
            pass
    
    with open(ofnfn,'w',encoding='utf-8',errors='ignore') as f:
        f.write(otxt)
        # print('>> saved:',ofnfn)


def do_metadata_text(i,text,num_words=False,ocr_accuracy=False):
    global ENGLISH
    md=text.meta
    print('>> starting:',i, text.id, len(md),'...')
    if num_words or ocr_accuracy:
        print('>> getting freqs:',i,text.id,'...')
        freqs=text.freqs()
        print('>> computing values:',i,text.id,'...')
        if num_words:
            md['num_words']=sum(freqs.values())
        if ocr_accuracy:
            num_words_recognized = sum([v for k,v in list(freqs.items()) if k[0] in ENGLISH])
            print(md['num_words'], num_words_recognized)
            md['ocr_accuracy'] = num_words_recognized / float(md['num_words']) if float(md['num_words']) else 0.0
    print('>> done:',i, text.id, len(md))
    return [md]



def skipgram_do_text(text,i=0,n=10):
    from lltk import tools
    print(i, text.id, '...')
    from nltk import word_tokenize
    words=word_tokenize(text.text_plain)
    words=[w for w in words if True in [x.isalpha() for x in w]]
    word_slices = tools.slice(words,slice_length=n,runts=False)
    return word_slices

def skipgram_do_text2(text_i,n=10,lowercase=True):
    text,i=text_i
    import random
    print(i, text.id, '...')
    from lltk import tools
    words=text.text_plain.strip().split()
    words=[tools.noPunc(w.lower()) if lowercase else tools.noPunc(w) for w in words if True in [x.isalpha() for x in w]]
    #sld=[]
    for slice_i,slice in enumerate(tools.slice(words,slice_length=n,runts=False)):
        sdx={'id':text.id, 'random':random.random(), 'skipgram':slice, 'i':slice_i}
        yield sdx
        #sld+=[sdx]
    #return sld

def skipgram_save_text(text_i_mongotup,n=10,lowercase=True,batch_size=1000):
    text,i,mongotuple = text_i_mongotup
    from pymongo import MongoClient
    c=MongoClient()
    db1name,db2name=mongotuple
    db0=getattr(c,db1name)
    db=getattr(db0,db2name)

    sld=[]
    for sdx in skipgram_do_text2((text,i),n=n,lowercase=lowercase):
        sld+=[sdx]
        if len(sld)>=batch_size:
            db.insert(sld)
            sld=[]
    if len(sld): db.insert(sld)
    c.close()
    return True




def save_tokenize_text(text,ofolder=None,force=False):
    import os
    if not ofolder: ofolder=os.path.join(text.corpus.path, 'freqs', text.corpus.name)
    ofnfn=os.path.join(ofolder,text.id+'.json')
    opath = os.path.split(ofnfn)[0]
    if not os.path.exists(opath): os.makedirs(opath)
    if not force and os.path.exists(ofnfn) and os.stat(ofnfn).st_size:
        print('>> already tokenized:',text.id)
        return
    else:
        print('>> tokenizing:',text.id,ofnfn)

    from collections import Counter
    import json,codecs
    toks=tokenize_text(BaseText)
    tokd=dict(Counter(toks))
    with codecs.open(ofnfn,'w',encoding='utf-8') as of:
        json.dump(tokd,of)
    #assert 1 == 2

def is_hashable(v):
    """Determine whether `v` can be hashed."""
    try:
        hash(v)
        return True
    except Exception:
        return False

def is_iterable(v):
    from collections.abc import Hashable,Iterable
    return isinstance(v,Iterable)

def is_hashable(v):
    """Determine whether `v` can be hashed."""
    try:
        hash(v)
    except TypeError:
        return False
    return True

def safebool(x,bad_vals={np.nan}):
    import pandas as pd
    if is_hashable(x) and x in bad_vals: return False
    if is_iterable(x): return bool(len(x))
    if pd.isnull(x) is True: return False
    return bool(x)

def merge_dict(*l,bad_keys_final=set()):
    od={}
    for d in l:
        if not issubclass(type(d), dict): continue
        for k,v in d.items():
            if safebool(k) and safebool(v):
                # log(f'k,v = {k},{v}')
                od[k]=v
    return {k:v for k,v in od.items() if k not in bad_keys_final}

def merge_dict_list(*l):
    od={}
    for d in l:
        if not issubclass(type(d), dict): continue
        for k,v in d.items():
            
            if type(v) in {list,tuple,set}:
                v='; '.join(str(vx) for vx in v)
            else:
                v=str(v)
            
            if v and k:
                if k in od and od[k]:
                    if type(od[k])!=list: od[k] = [od[k]]
                    od[k].append(v)
                else:
                    od[k]=v
    
    return od

def merge_dict_set(*l):
    od=defaultdict(set)
    for d in l:
        if not issubclass(type(d), dict): continue
        for k,v in d.items():
            if type(v) in {list,tuple,set}:
                vset=set(v)
            else:
                vset=set(str(v))
            od[k]|=vset
    
    for k,v in od.items():
        vl=list(v)
        od[k]=vl[0] if len(vl)==1 else vl

    return od



def MaybeListDict(key_val_iter):
    od=defaultdict(set)
    for key,val in key_val_iter:
        valstr=str(val)
        lval=set(val) if type(val) in {list,tuple} else set(valstr.split('; '))
        # log([key,val,valstr,lval])
        od[key]|=lval
    return od



def is_text_obj(obj):
    from lltk.text.text import BaseText
    if issubclass(type(obj), BaseText): return True
    if hasattr(obj,'__dict__') and (obj.__dict__.get('_meta') or obj.__dict__.get(BROKENSTATE,{}).get('_meta')): return True
    return False

def is_broken_obj(obj):
    return hasattr(obj,'__dict__') and BROKENSTATE in obj.__dict__
    

def is_corpus_obj(obj): 
    from lltk.corpus.corpus import BaseCorpus
    return issubclass(type(obj), BaseCorpus)


def to_textids(l,col_id='id'):
    import pandas as pd
    from lltk import Text

    if all([is_text_obj(t) for t in l]): return [t.id for t in l]

    if issubclass(l.__class__, pd.DataFrame) and col_id in set(l.reset_index().columns):
        return list(l.reset_index()[col_id])
    
    return [
        x.id if (Text in x.__class__.mro()) else x
        for x in l
    ]

def clean_text(txt):
    import ftfy
    txt=ftfy.fix_text(txt)
    replacements={
        '&eacute':'é',
        '&hyphen;':'-',
        '&sblank;':'--',
        '&mdash;':' -- ',
        '&ndash;':' - ',
        '&longs;':'s',
        '&wblank':' -- ',
        u'\u2223':'',
        u'\u2014':' -- ',
        # '|':'',
        '&ldquo;':u'“',
        '&rdquo;':u'”',
        '&lsquo;':u'‘’',
        '&rsquo;':u'’',
        '&indent;':'     ',
        '&amp;':'&',
        '&euml;':'ë',
        '&uuml;':'ü',
        '&auml;':'ä',
    }
    for k,v in list(replacements.items()):
        if k in txt:
            txt=txt.replace(k,v)
        elif k.startswith('&') and k.endswith(';') and k[:-1] in txt:
            txt=txt.replace(k[:-1],v)
    return txt

def remove_bad_tags(dom, bad_tags):
    for tag in bad_tags:
        for x in dom(tag):
            x.decompose()
    return dom


def to_lastname(name):
    name=name.strip()
    if not name: return 'Unknown'
    if ',' in name:
        namel=[x.strip() for x in name.split(',') if x.strip()]
        name=namel[0] if namel else name
    else:
        namel=[x.strip() for x in name.split() if x.strip()]
        name=namel[-1] if namel else name
    return name






# def xml2txt_prose(path_xml, para_tag='p', bad_tags=BAD_TAGS, body_tag='doc'):
#     import bs4

#     if not os.path.exists(path_xml): return ''
#     with open(path_xml) as f: xml=f.read()
#     xml = clean_text(xml)
#     dom = bs4.BeautifulSoup(xml,'lxml')
#     body = dom.find(body_tag)    
#     if body is None: body = dom
#     for tag in bad_tags:
#         for x in body(tag):
#             x.extract()
    
#     paras = [para.text.strip() for para in body(para_tag)]
#     if paras:
#         paras=[' '.join(para.strip().split()) for para in paras]
#         paras=[para for para in paras if para]
#         txt='\n\n'.join(paras)
#     else:
#         txt = body.text.strip()
#     return txt








def xml2txt_default(xml, *x, OK={'p','l'}, BAD=None, body_tags={'text','doc'}, **args):
    #print '>> text_plain from stored XML file...'
    import bs4
    if log.verbose>1: log(f'xml = {xml}')
    if '\n' not in xml and os.path.exists(xml):
        if log.verbose>1: log(f'is filename = {xml}')
        with open(xml) as f: xml=f.read()

    # clean
    xml = clean_text(xml)
    if log.verbose>1: log([xml[:1000]])

    ## get dom
    dom = bs4.BeautifulSoup(xml,'lxml') if type(xml)==str else xml
    ## remove bad tags
    if BAD is None: BAD = BAD_TAGS
    if log.verbose>1: log([str(dom)[:1000]])
    for tag in BAD: [x.extract() for x in dom.findAll(tag)]
    ## get text
    txt=[]

    docl=[]
    for btag in body_tags:
        btagl = list(dom(btag))
        if btagl:
            docl+=btagl
            break
    docl=[dom]
    for doc in docl:
        for tag in doc.find_all():
            if tag.name not in OK: continue
            if tag.name=='p':
                txt+='\n' + tag.text.replace('\n',' ').replace('  ',' ').strip()+'\n'
            elif tag.name=='l':
                txt+=tag.text.strip()+'\n'
    o=''.join(txt).strip()
    if log.verbose>1: log([o[:1000]])
    return o

xml2txt_prose = xml2txt_default


def tokenize_agnostic(txt):
    return re.findall(r"[\w']+|[.,!?; -—–\n]", txt)
    
def tokenize_fast(line,lower=False):
    line = line.lower() if lower else line
    import re
    # tokenize using reg ex (fast)
    tokens = re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line)
    # remove punctuation on either end
    from string import punctuation
    tokens = [tok.strip(punctuation) for tok in tokens]
    # make sure each thing in list isn't empty
    tokens = [tok for tok in tokens if tok]
    return tokens

# tokenize
def tokenize_nltk(txt,lower=False):
    # lowercase
    txt_l = txt.lower() if lower else txt
    # use nltk
    tokens = nltk.word_tokenize(txt_l)
    # weed out punctuation
    tokens = [
        tok
        for tok in tokens
        if tok[0].isalpha()
    ]
    # return
    return tokens

def tokenize(txt,*x,**y):
    return tokenize_fast(txt,*x,**y)




def filter_freqs(freqs,modernize=False,lower=True):
    from collections import Counter
    cd=Counter()
    if modernize: mod=get_spelling_modernizer()
    for w,c in sorted(freqs.items(),key=lambda x: -x[1]):
        if lower: w=w.lower()
        if modernize: w=mod.get(w,w)
        cd[w]+=c
    return cd





def save_freqs_json(obj, lower=True):
    from collections import Counter
    import ujson as json

    ifnfn,ofnfn,tokenizer=obj
    if not os.path.exists(ifnfn): return
    # if os.path.exists(ofnfn): return
    if tokenizer is None: tokenizer=tokenize

    opath = os.path.dirname(ofnfn)
    try:
        if not os.path.exists(opath): os.makedirs(opath)
    except FileExistsError:
        pass

    # read txt
    with open(ifnfn,encoding='utf-8',errors='replace') as f: txt=f.read()
    
    # tokenize
    if lower: txt=txt.lower()
    toks=tokenizer(txt)
    #print(len(toks),ofnfn)
    
    # count
    tokd=dict(Counter(toks))
    
    # save
    with open(ofnfn,'w') as of: json.dump(tokd,of)
    
    # return?
    # return tokd



def stamp_d(src,sd):
    stamppref=f'{src.corpus.id}::'
    stampsuf=f'__{src.corpus.id}'

    return {
        # (stamppref+sdk):sdv
        (sdk+stampsuf):sdv
        for sdk,sdv in sd.items()
    }
def unstamp_d(src,sd):
    stampsuf=f'__{src.corpus.id}'
    stamppref=f'{src.corpus.id}::'
    return {
        sdk:sdv
        for sdk,sdv in sd.items()
        if '__' not in sdk
        # if not sdk.startswith(stamppref)
        # if not sdk.endswith(stampsuf)
    }