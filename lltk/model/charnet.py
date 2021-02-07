import os,json
import networkx as nx
import pandas as pd
from pprint import pprint

###
TITLES = {
    "Dr", "Dr.",
    "Mr", "Mr.",
    "Ms", "Ms.",
    "Miss",
    "Master", "Mistress",
    "Sir","Lady",
    "Duke","Baron","Count",
    "Prince","Princess","King","Queen",
    "Bishop","Father","Mother"
}


## Dead simple person NER?

def ner_parse_flair(txt):
    from collections import defaultdict
    from flair.models import SequenceTagger
    from flair.data import Sentence
    tagger = SequenceTagger.load('ner')
    from tqdm import tqdm
    from flair.models import MultiTagger
    from flair.tokenization import SegtokSentenceSplitter
    # initialize sentence splitter
    splitter = SegtokSentenceSplitter()

    # load tagger for POS and NER 
    # tagger = MultiTagger.load(['pos', 'ner'])

    senti=0
    tokeni=0
    res=defaultdict(list)
    incl_sent=set()
    for pi,paragraph in enumerate(tqdm(txt.split('\n\n'))):
        for si,sentence in enumerate(splitter.split(paragraph)):
            senti+=1
            tagger.predict(sentence)
            sentdat=sentence.to_dict()
            for datatype in sentdat:
                print(datatype,sentdat[datatype])
                for datad in sentdat[datatype]:
                    if type(datad)==str: continue
                    sentdx={
                        **datad,
                        **{
                            'para':pi+1,
                            'sent':senti,
                            'sent_txt':datad['text'] if senti not in incl_sent else ''
                        }
                    }
                    print(sentdx)
                    res[datatype]+=[sentdx]
                    incl_sent|={senti} 
    return res

def ner_parse_stanza(txt,nlp=None):
    ### Return df of named entities and types
    import stanza
    from tqdm import tqdm
    import pandas as pd

    if nlp is None: nlp = stanza.Pipeline(lang='en', processors='tokenize,ner')
    
    #doc = nlp("Chris Manning teaches at Stanford University. He lives in the Bay Area.")
    #print(*[f'entity: {ent.text}\ttype: {ent.type}' for ent in doc.ents], sep='\n')

    from collections import Counter
    countd=Counter()

    # clean
    # txt = txt.replace('--','—')
    ent_ld=[]
    para_txts = txt.split('\n\n')
    senti=0
    incl_sent=set()
    char_so_far=0
    for pi,para in enumerate(tqdm(para_txts,desc="Parsing paragraphs")):
        pdoc = nlp(para)

        for si,sentdoc in enumerate(pdoc.sentences):
            senti+=1
            for ent in sentdoc.ents:
                # print(dir(ent))
                # print(dir(ent.words[0]))
                ids=[tok.id[0] for tok in ent.tokens]
                idstr=f'{ids[0]}-{ids[-1]}' if len(ids)>1 else str(ids[0])
                entd = {
                    'num_para':pi+1,
                    'num_sent':senti,
                    'text':ent.text,
                    'start_word_in_sent':ids[0],
                    'end_word_in_sent':ids[-1],
                    'start_char_in_para':ent.tokens[0].start_char,
                    'end_char_in_para':ent.tokens[0].end_char,
                    'type':ent.type,
                    'sent_text':sentdoc.text if senti not in incl_sent else '',
                }
                incl_sent|={senti}
                ent_ld.append(entd)
        char_so_far+=len(para)
    return ent_ld

def ner_parse(txt,*x,**y):
    return ner_parse_stanza(txt,*x,**y)


def ner_txt2names(txt,incl_labels={}):
    import stanza
    from tqdm import tqdm
    
    nlp = stanza.Pipeline(lang='en', processors='tokenize,ner')
    
    
    #doc = nlp("Chris Manning teaches at Stanford University. He lives in the Bay Area.")
    #print(*[f'entity: {ent.text}\ttype: {ent.type}' for ent in doc.ents], sep='\n')

    from collections import Counter
    countd=Counter()

    # clean
    # txt = txt.replace('--','—')
    para_txts = txt.split('\n\n')
    for para in tqdm(para_txts,desc="Parsing paragraphs"):
        pdoc = nlp(para)
        for entd in pdoc.ents:
            countd[entd.text]+=1
    return countd

def ner_txt2names_spacy(txt,incl_labels={}): # 'PERSON'
    import spacy
    from tqdm import tqdm


    nlp = spacy.load("en_core_web_lg")

    from spacy.tokens import Span
    import dateparser,time

    def expand_person_entities(doc):
        new_ents = []
        doc.ents = [e for e in doc.ents if e.label_=='PERSON']
        for ent in doc.ents:
            if ent.label_ == "PERSON" and ent.start != 0:
                prev_token = doc[ent.start - 1]
                if prev_token.text in TITLES:
                    new_ent = Span(doc, ent.start - 1, ent.end, label=ent.label)
                    new_ents.append(new_ent)
            else:
                new_ents.append(ent)
        doc.ents = new_ents
        return doc

    # Add the component after the named entity recognizer
    nlp.add_pipe(expand_person_entities, after='ner')

    from collections import Counter
    countd=Counter()

    
    # clean
    txt = txt.replace('--','—')

    # doc = nlp(txt)
    # now=time.time()
    # for entd in doc.ents:
    #     if entd.label_=='PERSON':
    #         countd[entd.text]+=1
    # print(f'done in {int(time.time()-now)} seconds')

    para_txts = txt.split('\n\n')
    for para in tqdm(para_txts,desc="Parsing paragraphs"):
        pdoc = nlp(para)
        for entd in pdoc.ents:
            countd[entd.text]+=1
    
    return countd


class CharacterNetwork:

    def __init__(self, texts=[]):
        self.texts = texts
        self.paths_txt = [(t if type(t)==str else t.path_txt) for t in texts]
        self.paths_nlp = [(os.path.splitext(t.replace('/txt/','/corenlp/'))[0]+'.json' if type(t)==str else t.path_nlp) for t in texts]
        

    def parse_corenlp(self):
        from lltk.model import corenlp
        corenlp.annotate_paths(self.paths_txt)

    def entities(self):
        from collections import Counter
        from lltk.model.corenlp import CoreDoc

        speakers=Counter()
        for path in self.paths_nlp:
            if not os.path.exists(path): continue
            with open(path) as f: pdat=json.load(f)
            
            doc = CoreDoc(pdat)
            for parse in doc.parses:
                for quote in parse.quotes:
                    speakers[quote.data['speaker']]+=1
        
        return speakers





def test():
    import lltk
    C = lltk.load('Chadwyck')
    texts = [t for t in C.texts() if 'Austen' in t.author and 'Emma' in t.title]
    charnet = CharacterNetwork(texts)
    
    speakers = charnet.entities()
    print(speakers.most_common(100))
    #  dict_keys(['index', 'paragraph', 'parse', 'basicDependencies', 'enhancedDependencies', 'enhancedPlusPlusDependencies', 'entitymentions', 'tokens'])
    # print([os.path.exists(x) for x in charnet.paths_nlp])