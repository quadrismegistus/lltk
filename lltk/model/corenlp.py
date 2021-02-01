import os,multiprocessing as mp,json
from tqdm import tqdm

PATH_CORENLP_DEFAULT = os.path.expanduser('~/lltk_data/tools/corenlp')
NUM_CPU = mp.cpu_count()
DEFAULT_ANNO='tokenize,ssplit,pos,lemma,ner,parse,depparse,coref,quote'.split(',')

def get_cmd(
        path_java='java',
        mem='16G',
        port=9000,
        timeout=30000,
        threads=NUM_CPU,
        maxCharLength=100000,
        annotators=DEFAULT_ANNO,
        preload=True,
        quiet=True,
        prettyPrint=True,
        output_format="json"):
    cmd=f"""cd {get_path_corenlp()} && {path_java} -Xmx{mem} -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port {port} -timeout {timeout} -threads {threads} -maxCharLength {maxCharLength} -annotators {','.join(annotators)} -{'preload' if preload else ''} -outputFormat {output_format} {'-prettyPrint' if prettyPrint else ''}"""
    return cmd

def get_path_corenlp():
    import lltk
    path = lltk.config.get('PATH_CORENLP',PATH_CORENLP_DEFAULT)
    return path

def install():
    try:
        import stanza
    except ImportError:
        os.system('pip install stanza -q')
        import stanza
    
    stanza.install_corenlp(dir=get_path_corenlp())

def start_server(**opts):
    os.system(get_cmd(**opts))

def annotate_do(txt):
    if not txt: return {}
    from stanza.server import CoreNLPClient,StartServer
    with CoreNLPClient(
            start_server=StartServer.DONT_START,
            output_format='json') as client:
        try:
            return client.annotate(txt)
        except:
            return

# def annotator_run(txts):
#     with CoreNLPClient(

#             start_server=StartServer.DONT_START,
#             output_format='json') as client:


def annotate(txt,num_proc=4,by_para=True,progress=None):
    import lltk
    if progress is None: progress=num_proc>1
    if by_para:
        paras = [p.strip() for p in txt.split('\n\n')]
        return lltk.pmap(annotate_do, paras, num_proc=num_proc, progress=progress)
    else:
        return annotate_do(txt)

def annotate_path(path,num_proc=4,by_para=True,save=True,ofn=None,force=True,progress=True):
    # exists?
    if not os.path.exists(path): return
    
    # proc
    res = None
    with open(path) as f:
        txt=f.read().strip()
        if txt:
            res = annotate(txt, num_proc=num_proc, by_para=by_para, progress=progress)
    if not res: return {}
    res = {'paragraphs':res}

    # save?
    if save:
        if not ofn: 
            ofn=path.replace('/txt/','/corenlp/')
            ofn='.'.join([os.path.splitext(ofn)[0], 'json'])
        
        if not force and os.path.exists(ofn): return
        odir=os.path.split(ofn)[0]
        if not os.path.exists(odir): os.makedirs(odir)
        with open(ofn,'w') as of:
            json.dump(res, of, indent=4)

    # return
    return res


def _do_annotate_paths(path,**opts):
    opts['num_proc']=1
    opts['progress']=False
    return annotate_path(path,**opts)

def annotate_paths(paths_txt):
    from lltk import tools
    return tools.pmap(_do_annotate_paths, paths_txt, num_proc=8)