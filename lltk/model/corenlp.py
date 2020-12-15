import os,pickle
from tqdm import tqdm
import stanza
stanza.download('en')
nlp = stanza.Pipeline(lang,processors=processors)

def parse(path_txt,lang='en',processors='tokenize,pos,lemma,depparse'):
    global nlp
    
    opath=path_txt.replace('.txt','.stanza')
    if os.path.exists(opath): return

    with open(path_txt) as f: txt=f.read()

    paras_b=[]
    for para in txt.split('\n\n'):#,position=1):
        try:
            para_obj = nlp(para)
            para_obj_b = para_obj.to_serialized()
        except IndexError:
            para_obj_b=b''
        paras_b+=[para_obj_b]

    # opath
    odir=os.path.dirname(opath)
    if not os.path.exists(odir): os.makedirs(odir)
    with open(opath,'wb') as of:
        #of.write(doc_b)
        pickle.dump(paras_b,of)