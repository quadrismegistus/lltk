from lltk.imports import *


#################
#### XML -> TXT
#################

def _func_ref(func):
    """Return a pickle-safe (file_path, func_name) pair for a function.

    Works for functions in dynamically loaded modules (e.g. corpus classes
    loaded via importlib.util.spec_from_file_location) where the module
    isn't in sys.modules under a standard import path.
    """
    import inspect
    try:
        fpath = inspect.getfile(func)
    except (TypeError, OSError):
        return None
    name = getattr(func, '__name__', None)
    if fpath and name:
        return (fpath, name)
    return None


_func_cache = {}

def _resolve_func_ref(ref):
    """Load and return a function from a (file_path, func_name) pair."""
    if ref in _func_cache:
        return _func_cache[ref]
    fpath, name = ref
    import importlib.util
    spec = importlib.util.spec_from_file_location('_preproc_mod', fpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    func = getattr(mod, name)
    _func_cache[ref] = func
    return func


def preprocess_txt(
        self: BaseCorpus,
        force: bool = False,
        num_proc: Union[int,None] = None,
        verbose: bool =True,
        lim: Union[int,None] = None,
        preview: bool = False,
        **kwargs):

    self.init(force=False)

    objs = []
    for t in self.texts():
        if not (t.path_xml and t.path_txt and t.xml2txt_func):
            continue
        if not os.path.exists(t.path_xml):
            continue
        if not force and os.path.exists(t.path_txt):
            continue
        ref = _func_ref(t.xml2txt_func)
        if ref:
            objs.append((t.path_xml, t.path_txt, ref))
        else:
            objs.append((t.path_xml, t.path_txt, t.xml2txt_func))
    objs = objs[:lim]

    if preview: return objs
    if not objs:
        if log>0: log.error('No XML files to produce plain text files from')
    else:
        can_multiprocess = objs and isinstance(objs[0][2], tuple)
        pmap(
            do_preprocess_txt,
            objs,
            num_proc=num_proc if num_proc else get_ideal_cpu_count(),
            use_threads=not can_multiprocess,
            desc=f'[{self.name}] Saving plain text versions of XML files',
            kwargs=kwargs
        )


def do_preprocess_txt(obj):
    ifnfn, ofnfn, func_or_ref = obj
    if isinstance(func_or_ref, tuple):
        func = _resolve_func_ref(func_or_ref)
    else:
        func = func_or_ref
    try:
        otxt = func(ifnfn)
    except Exception as e:
        print(f'!! Error processing {ifnfn}: {e}')
        return
    if not otxt:
        return
    odir=os.path.dirname(ofnfn)
    if not os.path.exists(odir):
        try:
            os.makedirs(odir)
        except OSError:
            pass

    with open(ofnfn,'w',encoding='utf-8',errors='ignore') as f:
        f.write(otxt)
        if log>1: log(f'>> saved: {ofnfn}')


#################
#### TXT -> FREQS
#################

def preprocess_freqs(
        self: BaseCorpus,
        force: bool = False,
        num_proc: Union[int,None] = None,
        preview: bool = False,
        lim: Union[int,None] = None,
        **kwargs):

    objs = [
        (t.path_txt,t.path_freqs,self.TOKENIZER.__func__)
        for t in self.texts()
        if t.path_txt and t.path_freqs and self.TOKENIZER.__func__
        and os.path.exists(t.path_txt) and (force or not os.path.exists(t.path_freqs))
    ][:lim]
    if preview: return objs
    if not objs:
        if log>0: log('Word freqs already saved')
    else:
        pmap(
            save_freqs_json,
            objs,
            num_proc=num_proc if num_proc else get_ideal_cpu_count(),
            desc=f'[{self.name}] Saving word freqs as jsons',
        )


def preprocess(
        self: BaseCorpus,
        parts: list = ['txt','freqs'],
        **kwargs):
    for part in parts:
        if part=='txt': preprocess_txt(self,**kwargs)
        if part=='freqs': preprocess_freqs(self,**kwargs)


### Attach to BaseCorpus
BaseCorpus.preprocess_txt = preprocess_txt
BaseCorpus.preprocess_freqs = preprocess_freqs
BaseCorpus.preprocess = preprocess
