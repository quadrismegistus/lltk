import os
import sys
import re
import shutil
import warnings
import multiprocessing as mp
import time
import csv
import numpy as np
from collections import UserList, defaultdict
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from io import StringIO
from lltk.imports import (
    HOME, ROOT, LLTK_ROOT, PATH_HERE,
    PATH_DEFAULT_LLTK_HOME, PATH_DEFAULT_CONF,
    META_KEY_SEP, DEFAULT_NUM_PROC, mp_cpu_count,
)


def _load_config():
    """Load configuration from config files. Returns dict."""
    import configparser
    cfg = {'PATH_TO_CORPORA': os.path.join(PATH_DEFAULT_LLTK_HOME, 'corpora')}
    for p in [PATH_DEFAULT_CONF,
              os.path.join(ROOT, 'config_local.txt'),
              os.path.join(HOME, 'lltk_config.txt')]:
        if os.path.exists(p):
            c = configparser.ConfigParser()
            c.read(p)
            for section in c.sections():
                for k, v in c[section].items():
                    cfg[k.upper()] = v
            if 'Default' in c:
                for k, v in c['Default'].items():
                    cfg[k.upper()] = v
    # Also check pointer file
    pointer = os.path.join(HOME, '.lltk_config')
    if os.path.exists(pointer):
        with open(pointer) as f:
            user_conf = f.read().strip()
        if os.path.exists(user_conf):
            c = configparser.ConfigParser()
            c.read(user_conf)
            for section in c.sections():
                for k, v in c[section].items():
                    cfg[k.upper()] = v
    return cfg

config = _load_config()


class _PmapCaller:
    """Picklable callable for pmap — avoids unpicklable closures."""
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
    def __call__(self, obj):
        return self.func(obj, *self.args, **self.kwargs)



def pmap(func, objs, args=(), kwargs=None, num_proc=1, use_threads=False,
         progress=True, desc='', **_ignored):
    """
    Parallel map with progress bar. Replaces yapmap.pmap.

    Args:
        func: function to apply to each object
        objs: iterable of objects
        args: extra positional args passed to func
        kwargs: extra keyword args passed to func
        num_proc: number of workers (1 = sequential)
        use_threads: use threads instead of processes (good for I/O-bound work)
        progress: show tqdm progress bar
        desc: progress bar description
    """
    if kwargs is None:
        kwargs = {}
    objs = list(objs)
    if not objs:
        return []

    caller = _PmapCaller(func, args, kwargs)

    if num_proc <= 1:
        # Sequential
        iterr = get_tqdm(objs, desc=desc) if progress else objs
        return [caller(obj) for obj in iterr]

    # Parallel
    Executor = ThreadPoolExecutor if use_threads else ProcessPoolExecutor
    results = []
    with Executor(max_workers=num_proc) as pool:
        futures = pool.map(caller, objs)
        if progress:
            from tqdm import tqdm
            futures = tqdm(futures, total=len(objs), desc=desc)
        results = list(futures)
    return results



def pmap_iter(func, objs, args=(), kwargs=None, num_proc=1, use_threads=False,
              progress=True, desc='', **_ignored):
    """Iterator version of pmap."""
    return iter(pmap(func, objs, args=args, kwargs=kwargs, num_proc=num_proc,
                     use_threads=use_threads, progress=progress, desc=desc))



def is_hashable_rly(v):
    try:
        hash(v)
        return True
    except Exception:
        return False

def is_hashable(v):
    from collections.abc import Hashable
    return isinstance(v,Hashable) and is_hashable_rly(v)


def is_dictish(v):
    from collections.abc import MutableMapping
    return isinstance(v, MutableMapping)


def is_iterable(v):
    from collections.abc import Iterable
    return isinstance(v,Iterable)


class SetList(UserList):
    def __init__(self, initlist=None):
        self.data = []
        self.data_set = set()
        if initlist is not None:
            for x in initlist:
                self.append(x)
    def __repr__(self): return self.data.__repr__()
    
    def append(self, item):
        if is_hashable(item):
            if item not in self.data_set:
                self.data_set|={item}
                self.data.append(item)
        elif type(item) in {list,set} or isinstance(item, UserList):
            for x in item:
                self.append(x)
        else:
            self.data.append(item)
            
    def __iadd__(self, other):
        if isinstance(other, UserList) or isinstance(other, type(self.data)):
            for x in other:
                self.append(x)
        else:
            self.append(other)
        return self
    
    def extend(self, other):
        for x in other: self.append(x)

    def remove(self, item):
        if is_hashable(item):
            if item in self.data_set:
                self.data_set = self.data_set - {item}
        try:
            self.data.remove(item)
        except ValueError:
            pass
                
        



class OrderedSetDict(MutableMapping):
    """A dictionary that applies an arbitrary key-altering
    function before accessing the keys"""

    def __init__(self, *args, flatten=False, **kwargs):
        self.store = defaultdict(list)
        self.store_set = defaultdict(set)
        self.update(dict(*args, **kwargs))  # use the free update to set keys
        self.flatten = flatten

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        vals = [v for v in value] if type(value) in {list,set} else [value]
        for v in vals:
            if is_hashable(v):
                if not v in self.store_set[key]:
                    self.store_set[key]|={v}
                    self.store[key]+=[v]
            elif type(v)==dict:
                if self.flatten:
                    for vk,vv in v.items():
                        key2 = f'{key}_{vk}'
                        print([key2,vv])
                        if is_hashable(vv):
                            if vv not in self.store_set[key2]:
                                self.store[key2]+=[vv]
                                self.store_set[key2]|={vv}
                        else:
                            self.store[key2]+=[vv]
                else:
                    self.store[key]+=[v]
            else:
                self.store[key]+=[v]


    def __delitem__(self, key):
        del self.store[key]
        del self.store_set[key]

    def __iter__(self):
        return iter(self.store)
    
    def __len__(self):
        return len(self.store)

    def to_dict(self):
        return {
            k:(val[0] if len(val)==1 else val)
            for k,val in self.store.items()
        }



def safebool(x,bad_vals={np.nan}):
    if is_dictish(x):
        return {
            k:v
            for k,v in x.items()
            if safebool(k) and safebool(v)
        }

    import pandas as pd
    try:
        if is_hashable(x) and x in bad_vals: return False
    except AssertionError as e:
        log.error(e)
    
    try:
        if is_iterable(x): return bool(len(x))
    except AssertionError as e:
        log.error(e)
    
    try:
        if pd.isnull(x) is True: return False
    except AssertionError as e:
        log.error(e)

    try:
        return bool(x)
    except AssertionError as e:
        log.error(e)
        return None



def just_metadata(d,prefix_params='_',ok_keys=None):
    from lltk.imports import COL_ADDR,COL_ID,COL_CORPUS
    if not ok_keys: ok_keys={COL_ADDR,COL_ID,COL_CORPUS}
    return {k:v for k,v in d.items() if k and (k in ok_keys or k[0] not in set(prefix_params))}


def just_meta_no_id(d,**y):
    from lltk.imports import COL_ADDR,COL_ID,COL_CORPUS
    bad_keys={COL_ADDR,COL_ID,COL_CORPUS}
    return {k:v for k,v in just_metadata(d).items() if k not in bad_keys and META_KEY_SEP not in k}


def to_numeric_dict(d):
    import pandas as pd
    
    odx={}
    for k,v in d.items():
        ov=v
        if type(v)==str and v and v[0].isdigit():
            try:
                ov=pd.to_numeric(v)
                try:
                    ovint=int(ov)
                    if ovint == ov:
                        ov = ovint
                except ValueError:
                    pass
            except ValueError:
                pass
        odx[k]=ov
    return odx



def camel_case_split(str):
    words = [[str[0]]]

    for c in str[1:]:
        if words[-1][-1].islower() and c.isupper():
            words.append(list(c))
        else:
            words[-1].append(c)

    return [''.join(word) for word in words]




def rmfn(fn):
    if os.path.exists(fn):
        try:
            os.unlink(fn)
        except AssertionError as e:
            pass




def read_json(path):
    if os.path.exists(path):
        try:
            with open(path, 'rb') as f:
                return orjson.loads(f.read())
        except Exception:
            import json
            with open(path) as f:
                return json.load(f)
    return {}


def fillna(x,y=''):
    try:
        return y if np.isnan(x) else x
    except TypeError:
        return x


def snake2camel(x,sep='_'):
    return ''.join(
        xx.title()
        for xx in x.split(sep)
    )


def to_camel_case(x):
    return ''.join((y[0].upper()+y[1:] for y in x.split()))


def ensure_snake(xstr,lower=True,allow={'_'}):
    if lower: xstr=xstr.lower()
    xstr=xstr.strip().replace(' ','_')
    o='_'.join(
        zeropunc(x,allow=allow)
        for x in xstr.split('_')
    )
    while META_KEY_SEP in o: o=o.replace(META_KEY_SEP,'_')
    return o



def which(pgm):
    path=os.getenv('PATH')
    for p in path.split(os.path.pathsep):
        p=os.path.join(p,pgm)
        if os.path.exists(p) and os.access(p,os.X_OK):
            return p


try:
    from IPython.core.magic import register_cell_magic
    @register_cell_magic
    def write_and_run(line, cell):
        argz = line.split()
        file = argz[-1]
        mode = 'w'
        if len(argz) == 2 and argz[0] == '-a':
            mode = 'a'
        with open(file, mode) as f:
            f.write(cell)
        get_ipython().run_cell(cell)
except (NameError,ModuleNotFoundError):
    pass



def human_format(num):
    magnitude = 0
    if num<1000: return str(num)
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    # add more suffixes if you need them
    return '%.0f%s' % (num, ['', 'K', 'M', 'B', 'T', 'P'][magnitude])


def get_tqdm(*args,**kwargs):
    if in_jupyter():
        from tqdm.notebook import tqdm as tqdmx
    else:
        from tqdm import tqdm as tqdmx
    return tqdmx(*args,**kwargs)



def get_wordlist(lang='en'):
    global WORDLISTS
    if lang in WORDLISTS: return WORDLISTS[lang]
    if lang=='en':
        from lltk import PATH_TO_ENGLISH_WORDLIST
        path = config.get('PATH_TO_ENGLISH_WORDLIST',PATH_TO_ENGLISH_WORDLIST)
        if not path: raise Exception('!! PATH_TO_ENGLISH_WORDLIST not set in config.txt')
        if not os.path.isabs(path): path=os.path.join(PATH_LLTK_HOME,path)
        if not os.path.exists(path): download_default_data(path)
        if os.path.exists(path):
            with xopen(path) as f:
                WORDLISTS[lang]=set(f.read().strip().split('\n'))
    return WORDLISTS[lang]


def get_spelling_modernizer(lang='en'):
    global SPELLINGD
    if lang in SPELLINGD: return SPELLINGD[lang]
    if lang=='en':
        from lltk import PATH_TO_ENGLISH_SPELLING_MODERNIZER
        path = config.get('PATH_TO_ENGLISH_SPELLING_MODERNIZER',PATH_TO_ENGLISH_SPELLING_MODERNIZER)
        if not path: raise Exception('!! PATH_TO_ENGLISH_SPELLING_MODERNIZER not set in config.txt')
        if not os.path.isabs(path): path=os.path.join(PATH_LLTK_HOME,path)
        if not os.path.exists(path): download_default_data(path)
        if os.path.exists(path):
            #print('>> getting spelling modernizer from %s...' % SPELLING_MODERNIZER_PATH)
            d={}
            #with codecs.open(SPELLING_MODERNIZER_PATH,encoding='utf-8') as f:
            with xopen(path) as f:
                for ln in f:
                    ln=ln.strip()
                    if not ln: continue
                    try:
                        old,new=ln.split('\t')
                    except ValueError:
                        continue
                    d[old]=new
            SPELLINGD[lang]=d
    return SPELLINGD[lang]


def get_word2pos(lang='en'):
    global WORD2POS
    # from lltk import PATH_LLTK_CODE_HOME
    if lang in WORD2POS: return WORD2POS[lang]
    if lang=='en':
        from lltk import PATH_TO_ENGLISH_WORD2POS
        path = config.get('PATH_TO_ENGLISH_WORD2POS',PATH_TO_ENGLISH_WORD2POS)
        if not path: raise Exception('!! PATH_TO_ENGLISH_WORD2POS not set in config.txt')
        if not os.path.isabs(path): path=os.path.join(PATH_LLTK_HOME,path)
        if not os.path.exists(path): download_default_data(path)
        if os.path.exists(path):
            with xopen(path) as f:
                # print(path,f)
                WORD2POS[lang]=json.load(f)
    return WORD2POS[lang]


def get_ocr_corrections(lang='en'):
    global OCRCORREX
    if lang in OCRCORREX: return OCRCORREX[lang]
    if lang=='en':
        d={}
        from lltk import PATH_TO_ENGLISH_OCR_CORRECTION_RULES
        path = config.get('PATH_TO_ENGLISH_OCR_CORRECTION_RULES',PATH_TO_ENGLISH_OCR_CORRECTION_RULES)
        if not os.path.isabs(path): path=os.path.join(PATH_LLTK_HOME, path)
        if not os.path.exists(path): download_default_data(path)
        if os.path.exists(path):
            with xopen(path) as f:
                for ln in f:
                    ln=ln.strip()
                    if not ln: continue
                    try:
                        old,new,count=ln.split('\t')
                    except ValueError:
                        continue
                    d[old]=new
        OCRCORREX[lang]=d
    return OCRCORREX[lang]


def save_df(df,ofn,move_prev=False,index=None,key='',log=print,verbose=False,**kwargs):
    import pandas as pd
    if os.path.exists(ofn) and move_prev: iter_move(ofn)
    ext = os.path.splitext(ofn.replace('.gz',''))[-1][1:]
    if index is None: index=type(df.index) != pd.RangeIndex
    
    ofndir=os.path.dirname(ofn)
    if ofndir and not os.path.exists(ofndir): os.makedirs(ofndir)

    try:
        if ext=='csv':
            df.to_csv(ofn,index=index)
        elif ext in {'xls','xlsx'}:
            df.to_excel(ofn)
        elif ext in {'txt','tsv'}:
            df.to_csv(ofn,index=index,sep='\t')
        elif ext=='ft':
            # if index: df=df.reset_index()
            df.to_feather(ofn)
        elif ext=='pkl':
            df.to_pickle(ofn)
        elif ext=='h5':
            df.to_hdf(ofn, key=key)
        # else:
            # raise Exception(f'[save_df()] What kind of df is this: {ofn}')
    except AssertionError as e:
        # try again as csv?
        ofn=os.path.splitext(ofn)[0]+'.csv'
        df.to_csv(ofn)
    if verbose and log: log(f'Saved: {ofn}')



def read_df(ifn,key='',fmt='',on_bad_lines='skip',**attrs):
    if not os.path.exists(ifn): return
    import pandas as pd
    if issubclass(ifn.__class__,pd.DataFrame): return ifn

    ext = os.path.splitext(ifn.replace('.gz',''))[-1][1:]

    try:

        if fmt=='csv' or ext=='csv':
            return pd.read_csv(ifn,on_bad_lines=on_bad_lines,**attrs)
        elif fmt=='tsv' or ext=='tsv':
            return pd.read_csv(ifn,sep='\t',on_bad_lines=on_bad_lines,**attrs)
        elif ext in {'xls','xlsx'}:
            return pd.read_excel(ifn,**attrs)
        elif ext in {'txt','tsv'}:
            return pd.read_csv(ifn,sep='\t',**attrs)
        elif ext=='ft':
            return pd.read_feather(ifn,**attrs)
        elif ext=='pkl':
            return pd.read_pickle(ifn,**attrs)
        elif ext=='h5':
            return pd.read_hdf(ifn, key=key,**attrs)
        else:
            raise Exception(f'[save_df()] What kind of df is this: {ifn}')
    except AssertionError as e:
        from lltk import log
        if log>0: log(f'Error: {e}')
        pass
    
    return pd.DataFrame()


def backup_fn(fn,suffix='bak',copy=True,move=True,**kwargs):
    """
    `move` is reset to False if copy == True
    """
    if copy: move=False
    if os.path.exists(fn):
        name, ext = os.path.splitext(fn)
        ofn = f'{name}.bak{ext}'
        if copy: shutil.copy(fn,ofn)
        if move: shutil.move(fn,ofn)


def iter_move(fn,force=False,prefix='',keep=3):
    if os.path.exists(fn):
        iter_fn=iter_filename(fn,force=force,prefix=prefix)
        iter_dir=os.path.dirname(iter_fn)
        if not os.path.exists(iter_dir): os.makedirs(iter_dir)
        shutil.move(fn,iter_fn)
        # print(f'>> moved: {fn} --> {iter_fn}')


def iter_filename(fnfn,force=False,prefix=''):
    if os.path.exists(fnfn) or force:
        fndir,fn=os.path.split(fnfn)
        filename,ext = os.path.splitext(fn)
        fnum=1 if not force else 0
        maybe_fn=os.path.join(fndir, prefix + filename + ext)
        while fnum and os.path.exists(maybe_fn):
            fnum+=1
            maybe_fn=os.path.join(fndir, prefix + filename + str(fnum) + ext)
        fnfn = maybe_fn
    return fnfn



def tokenize(txt,*x,**y):
    # from nltk import word_tokenize
    # return word_tokenize(txt)
    from lltk.text.utils import tokenize as f
    return f(txt)

_SPLITTER_ = r"([-.,/:!?\";)(])"



class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout




def ensure_dir_exists(path,fn=None):
    if not path: return ''
    try:
        if fn is None and os.path.splitext(path)!=path: fn=True
        if fn: path=os.path.dirname(path)
        if not os.path.exists(path): os.makedirs(path)
    except AssertionError:
        pass




MDETOK=None


###


def read_ld(fn,keymap={},toprint=True):
    if fn.endswith('.xls') or fn.endswith('.xlsx'):
        return xls2ld(fn,keymap=keymap)
    #elif fn.endswith('.csv'):
    #	sep=','
    #	return list(readgen_csv(fn,as_dict=True,toprint=toprint,tsep=','))
    #return list(readgen(fn,as_dict=True,toprint=toprint))
    return list(readgen_csv(fn))



def printm(x):
    from IPython.display import display,Markdown
    display(Markdown(x))



def writegen(fnfn,generator,header=None,args=[],kwargs={},find_all_keys=False,total=None,progress=False,delimiter=','):
    from tqdm import tqdm
    import csv,gzip

    if not header:
        iterator=generator(*args,**kwargs)
        if not find_all_keys:
            first=next(iterator)
            header=sorted(first.keys())
        else:
            print('>> finding keys:')
            keys=set()
            for dx in iterator:
                keys|=set(dx.keys())
            header=sorted(list(keys))
            print('>> found:',len(header),'keys')

    iterator=generator(*args,**kwargs)
    if progress or total: iterator=get_tqdm(iterator,total=total)

    with (open(fnfn, 'w') if not fnfn.endswith('.gz') else gzip.open(fnfn,'wt')) as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=header,extrasaction='ignore',delimiter=delimiter)
        writer.writeheader()
        for i,dx in enumerate(iterator):
            #for k,v in dx.items():
            #	dx[k] = str(v).replace('\r\n',' ').replace('\r',' ').replace('\n',' ').replace('\t',' ')
            writer.writerow(dx)
    print('>> saved:',fnfn)
    

# def writegen(fnfn,generator,header=None,args=[],kwargs={},find_all_keys=False,total=None):
# 	from tqdm import tqdm
# 	import codecs,csv
# 	if 'jsonl' in fnfn.split('.'): return writegen_jsonl(fnfn,generator,args=args,kwargs=kwargs)

# 	iterator=generator(*args,**kwargs)
# 	if total: iterator=get_tqdm(iterator,total=total)
# 	if not header:
# 		if not find_all_keys:
# 			first=next(iterator)
# 			header=sorted(first.keys())
# 		else:
# 			print('>> finding keys:')
# 			keys=set()
# 			for dx in iterator:
# 				keys|=set(dx.keys())
# 			header=sorted(list(keys))
# 			print('>> found:',len(header),'keys')

# 	iterator=generator(*args,**kwargs)
# 	with open(fnfn, 'w') as csvfile:
# 		writer = csv.DictWriter(csvfile,fieldnames=header,extrasaction='ignore',delimiter='\t')
# 		writer.writeheader()
# 		for i,dx in enumerate(iterator):
# 			for k,v in dx.items():
# 				#if type(v) in [str]:
# 				#	dx[k]=v.encode('utf-8')
# 				dx[k] = str(v).replace('\r\n',' ').replace('\r',' ').replace('\n',' ').replace('\t',' ')
# 			writer.writerow(dx)
# 	print('>> saved:',fnfn)


def readgen_csv(fnfn,sep=None,encoding='utf-8',errors='ignore',header=[],progress=True,num_lines=0,desc='Reading CSV file'):
    from smart_open import open
    from csv import reader
    from tqdm import tqdm
    if not sep: sep=',' if fnfn.endswith('csv') or fnfn.endswith('.csv.gz') else '\t'
    if progress and not num_lines:
        with open(fnfn,encoding=encoding,errors=errors) as f:
            for _ in f: num_lines+=1
    
    with open(fnfn,encoding=encoding,errors=errors) as f:
        # csv_reader = reader(f)
        # if not header: header=next(csv_reader)
        header_line=next(f)
        if header_line==None: return
        header=list(reader([header_line.strip()]))[0]
        if header!=None:
            iterr=f if not progress else get_tqdm(f,total=num_lines,desc=desc)
            for row in iterr:
                try:
                    data = list(reader([row.strip()]))[0]
                    yield dict(zip(header,data))
                except AssertionError:
                    pass


def readgen(fnfn,**y):
    if issubclass(fnfn.__class__,pd.DataFrame): yield from resetindex(fnfn).to_dict('records')
    if type(fnfn)==str and os.path.exists(fnfn):
        ext=os.path.splitext(fnfn)[-1]
        if ext=='.jsonl':
            yield from readgen_jsonl(fnfn,**y)
        elif ext=='.csv':
            yield from readgen_csv(fnfn,**y)
        elif ext=='.txt':
            yield from readgen_csv(fnfn,sep='\t',**y)
        else:
            # print(f'[readgen()] Resorting to non-generator load for {fnfn}')
            df=read_df(fnfn)
            yield from resetindex(df).to_dict('records')


def header(fnfn,tsep='\t',encoding='utf-8'):
    header=[]

    if fnfn.endswith('.gz'):
        import gzip
        of=gzip.open(fnfn)
    #of = codecs.open(fnfn,encoding=encoding)
    else:
        of=open(fnfn)

    for line in of:
        line = line[:-1]  # remove line end character
        line=line.decode(encoding=encoding)
        header=line.split(tsep)
        break
    of.close()
    return header

# def read(fnfn,to_unicode=True):
# 	if fnfn.endswith('.gz'):
# 		import gzip
# 		try:
# 			with gzip.open(fnfn,'rb') as f:
# 				x=f.read()
# 				if to_unicode: x=x.decode('utf-8')
# 				return x
# 		except IOError as e:
# 			print("!! error:",e, end=' ')
# 			print("!! opening:",fnfn)
# 			print()
# 			return ''
#
# 	elif fnfn.endswith('.txt'):
# 		if to_unicode:
# 			try:
# 				with codecs.open(fnfn,encoding='utf-8') as f:
# 					return f.read()
# 			except UnicodeDecodeError:
# 				return read(fnfn,to_unicode=False)
# 		else:
# 			with open(fnfn) as f:
# 				return f.read()
#
# 	return ''


def read(fnfn):
    try:
        if fnfn.endswith('.gz'):
            import gzip
            with gzip.open(fnfn,'rb') as f:
                return f.read().decode('utf-8',errors='ignore')
        else:
            with open(fnfn) as f:
                return f.read() #.decode('utf-8',errors='ignore')
    except IOError as e:
        print("!! error:",e, end=' ')
        print("!! opening:",fnfn)
        print()
        return ''


def ld2dd(ld,rownamecol='rownamecol'):
    dd={}
    for d in ld:
        dd[d[rownamecol]]=d
        #del dd[d[rownamecol]][rownamecol]
    return dd


def slice(l,num_slices=None,slice_length=None,runts=True,random=False):
    """
    Returns a new list of n evenly-sized segments of the original list
    """
    if random:
        import random
        random.shuffle(l)
    if not num_slices and not slice_length: return l
    if not slice_length: slice_length=int(len(l)/num_slices)
    newlist=[l[i:i+slice_length] for i in range(0, len(l), slice_length)]
    if runts: return newlist
    return [lx for lx in newlist if len(lx)==slice_length]



def noPunc(token):
    from string import punctuation
    return token.strip(punctuation)


def zeropunc(s,allow={}):
    allow=set(allow)
    # return ''.join([x for x in s if x.isalpha() or x in allow])
    return ''.join([x for x in s if x.isalnum() or x in allow])

    # # ok={' '} if spaces_ok else {}
    # import string
    # return s.translate(str.maketrans('', '', string.punctuation))
    # # return ''.join(x for x in s if x.isalpha() or x in ok)



def now(now=None):
    import datetime as dt
    if not now:
        now=dt.datetime.now()
    elif type(now) in [int,float,str]:
        now=dt.datetime.fromtimestamp(now)

    return '{0}-{1}-{2} {3}:{4}:{5}'.format(now.year,str(now.month).zfill(2),str(now.day).zfill(2),str(now.hour).zfill(2),str(now.minute).zfill(2),str(now.second).zfill(2))


def gleanPunc2(aToken):
    aPunct0 = ''
    aPunct1 = ''
    while(len(aToken) > 0 and not aToken[0].isalnum()):
        aPunct0 = aPunct0+aToken[:1]
        aToken = aToken[1:]
    while(len(aToken) > 0 and not aToken[-1].isalnum()):
        aPunct1 = aToken[-1]+aPunct1
        aToken = aToken[:-1]

    return (aPunct0, aToken, aPunct1)


def modernize_spelling_in_txt(txt,spelling_d=None):
    if not spelling_d: spelling_d=get_spelling_modernizer()
    lines=[]
    for ln in txt.split('\n'):
        ln2=[]
        for tok in ln.split(' '):
            p1,tok,p2=gleanPunc2(tok)
            tok=spelling_d.get(tok,tok)
            ln2+=[p1+tok+p2]
        ln2=' '.join(ln2)
        lines+=[ln2]
    return '\n'.join(lines)



def tokenize_fast(line):
    return re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line.lower())







def ngram(l,n=3):
    grams=[]
    gram=[]
    for x in l:
        gram.append(x)
        if len(gram)<n: continue
        g=tuple(gram)
        grams.append(g)
        gram.reverse()
        gram.pop()
        gram.reverse()
    return grams





### PASSAGES



def index(text,phrase,ignorecase=True):
    compiled = re.compile(phrase, re.IGNORECASE) if ignorecase else re.compile(phrase)
    passage_indices = [(match.start(), match.end()) for match in re.finditer(compiled, text)]
    return passage_indices


def passages(text,phrases=[],window=200,indices=None,ignorecase=True,marker='***'):
    txt_lower = text.lower()
    window_radius=int(window/2)
    for phrase in phrases:
        if phrase.lower() in txt_lower:
            if not indices: indices = index(text,phrase,ignorecase=ignorecase)

            for ia,ib in indices:
                pre,post=text[:ia],text[ib:]
                match = text[ia:ib]
                window=get_word_window(pre,window_radius,True) + marker+match+marker+get_word_window(post,window_radius,False)
                dx={'index':ia, 'index_end':ib, 'passage':window,'phrase':phrase}
                yield dx






def yank(text,tag,none=None):
    if type(tag)==type(''):
        tag=tagname2tagtup(tag)

    try:
        return text.split(tag[0])[1].split(tag[1])[0]
    except IndexError:
        return none





def download(url,save_to,force=False,desc=''):
    here=os.getcwd()
    if not force and os.path.exists(save_to): return
    savedir=os.path.dirname(save_to)
    if not os.path.exists(savedir): os.makedirs(savedir)
    # download_wget(url,save_to,desc=desc)
    download_file_tqdm(url,save_to,desc=desc)
    os.chdir(here)


def copyfileobj(fsrc, fdst, total, length=16*1024):
    """Copy data from file-like object fsrc to file-like object fdst
    This is like shutil.copyfileobj but with a progressbar.
    """
    from tqdm import tqdm
    with get_tqdm(unit='bytes', total=total, unit_scale=True) as pbar:
        while 1:
            buf = fsrc.read(length)
            if not buf:
                break
            fdst.write(buf)
            pbar.update(len(buf))


def in_jupyter(): return sys.argv[-1].endswith('json')


class Bunch(object):
    def __init__(self, **adict):
        self.__dict__.update(adict)
    def __getattr__(self,attr):
        return self.__dict__.get(attr,'')
    def __setattr__(self,attr,val):
        sd=self.__dict__
        sd[attr]=val
    def __iter__(self):
        for v in self.__dict__.values():
            yield v



def mask_home_dir(path): return ppath(path)

def ppath(path):
    import os
    return path.replace(
        os.path.expanduser('~'),
        '~'
    )

def rpath(path):
    import os
    return path.replace(
        '~',
        os.path.expanduser('~')
    )



def extract(fn,*x,**attrs):
    if fn.endswith('zip'):
        unzip(fn,*x,**attrs)
    elif fn.endswith('tar') or fn.endswith('tgz') or fn.endswith('tar.gz'):
        untar(fn,*x,**attrs)





def unzip(zipfn, dest='.', flatten=False, overwrite=False, replace_in_filenames={},desc='',progress=True):
    from zipfile import ZipFile
    from tqdm import tqdm

    # Open your .zip file
    if not desc: desc=f'Extracting {os.path.basename(zipfn)} to {dest}'
    with ZipFile(zipfn) as zip_file:
        namelist=zip_file.namelist()

        # Loop over each file
        iterr=get_tqdm(iterable=namelist, total=len(namelist),desc=desc) if progress else namelist
        for member in iterr:
            # Extract each file to another directory
            # If you want to extract to current working directory, don't specify path
            filename = os.path.basename(member)
            if not filename: continue
            target_fnfn = os.path.join(dest,member) if not flatten else os.path.join(dest,filename)
            for k,v in replace_in_filenames.items(): target_fnfn = target_fnfn.replace(k,v)
            if not overwrite and os.path.exists(target_fnfn): continue
            target_dir = os.path.dirname(target_fnfn)
            try:
                if not os.path.exists(target_dir): os.makedirs(target_dir)
            except FileExistsError:
                pass
            except FileNotFoundError:
                continue
            try:
                with zip_file.open(member) as source, open(target_fnfn,'wb') as target:
                    shutil.copyfileobj(source, target)
            except FileNotFoundError:
                print('!! File not found:',target_fnfn)

def get_num_lines(filename):
    from smart_open import open

    def blocks(files, size=65536):
        while True:
            b = files.read(size)
            if not b: break
            yield b

    with open(filename, 'r', errors='ignore') as f:
        numlines=sum(bl.count("\n") for bl in blocks(f))

    return numlines



#print('>>>>',config)



def check_make_dir(path,ask=True,default='y'):
    if os.path.exists(path) and os.path.isdir(path): return True
    if os.path.splitext(path)[0]!=path: return # return if a filename, not a dirname
    path=os.path.abspath(path)
    if not os.path.exists(path) and os.path.splitext(path)[0]==path:
        # create?
        ans=input('>> create this path?: '+path+'\n>> [Y/n] ').strip().lower() if ask else default
        if not ans: ans=default
        if ans=='y':
            print('   creating:',path)
            os.makedirs(path)
            return True
    return False


def symlink(path,link_to,default='y',ask=True):
    # symlink?
    if link_to and os.path.exists(path):
        link_does_not_exist=not os.path.exists(link_to)
        link_already_points_to_file=os.path.realpath(path)==os.path.realpath(link_to)
        link_is_same_as_file=link_to==path

        ext_link=os.path.splitext(link_to)[-1]
        ext_path=os.path.splitext(path)[-1]
        link_has_wrong_file_extension = ext_link and ext_path and ext_link!=ext_path
        if link_is_same_as_file:
            pass
        elif link_has_wrong_file_extension:
            pass
        elif link_already_points_to_file:
            #print('   link exists:',link_to)
            pass
        elif link_does_not_exist or not link_already_points_to_file:
            ans=default if not ask else input('>> create link? [Y/n]\n' + (' '*3) + f'from: {link_to}\n' + (' '*3) + f'to: {path}\n>> ').strip().lower()
            if not ans: ans=default
            if ans=='y':
                print('>> linking to:',link_to)
                if os.path.exists(link_to): os.remove(link_to)
                os.symlink(path, link_to)


SOURCES=[]
try:
    from lltk.imports import PATH_CORPUS
    SOURCES.append(PATH_CORPUS)
except Exception:
    pass
SOURCES+=['.']

#print("SOURCES:",SOURCES)


def get_path_abs(path,sources=SOURCES,rel_to=None):
    if not path: return ''
    if os.path.isabs(path):
        rpath=path
    else:
        rpath=''
        for source in sources:
            spath=os.path.join(source,path)
            #if os.path.isabs(spath): return spath
            if os.path.exists(spath):
                rpath=os.path.abspath(spath)
                break
    if not rpath: return ''

    if rel_to:
        return os.path.relpath(rpath,rel_to)
    else:
        return os.path.abspath(rpath)




def get_config_file_location(pointer_fn=os.path.expanduser('~/.lltk_config')):
    if not os.path.exists(pointer_fn):
        print('!! No configuration file created. Run: lltk configure')
        return

    with open(pointer_fn) as f:
        return f.read()



def remove_duplicates(seq,remove_empty=False):
    seen = set()
    seen_add = seen.add
    l = [x for x in seq if not (x in seen or seen_add(x))]
    if not remove_empty: return l
    return [x for x in l if x]


