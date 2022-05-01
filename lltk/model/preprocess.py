from lltk.imports import *


#################
#### XML -> TXT
#################

def preprocess_txt(
        self: BaseCorpus,
        force: bool = False,
        num_proc: Union[int,None] = None,
        verbose: bool =True,
        lim: Union[int,None] = None,
        **kwargs):    
    
    # make sure init
    self.init(force=False)
    
    objs = [
        (t.path_xml,t.path_txt,t.xml2txt_func)
        for t in self.texts()
        if t.path_xml and t.path_txt and t.xml2txt_func
        and os.path.exists(t.path_txt) and (force or not os.path.exists(t.path_txt))
    ][:lim]
    if preview: return objs
    if not objs:
        if log>0: log.error('No XML files to produce plain text files from')
    else:
        pmap(
            do_preprocess_txt,
            objs,
            num_proc=num_proc if num_proc else get_ideal_cpu_count(),
            desc=f'[{self.name}] Saving plain text versions of XML files',
            kwargs=kwargs
        )



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
        if log>1: log(f'>> saved: {ofnfn}')







#################
#### TXT -> FREQS
#################



def preprocess_freqs(
        self: BaseCorpus,
        force: bool = False,
        num_proc: Union[int,None] = None,
        preview: bool = True,
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
            kwargs=kwargs,
            num_proc=num_proc if num_proc else get_ideal_cpu_count(),
            desc=f'[{self.name}] Saving word freqs as jsons',
        )




def preprocess(
        self: BaseCorpus,
        parts: list = ['txt','freqs'],
        **kwargs):
    """_summary_

    Parameters
    ----------
    self : BaseCorpus
        _description_
    part : str, optional
        _description_, by default Literal['part','freqs']
    """            
    for part in parts:
        if part=='txt': return preprocess_txt(self,**kwargs)
        if part=='freqs': return preprocess_freqs(self,**kwargs)












### MODIFY BASE CORPUS CLASS
BaseCorpus.preprocess_txt = preprocess_txt
BaseCorpus.preprocess_freqs = preprocess_freqs
BaseCorpus.preprocess = preprocess
