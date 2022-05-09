from lltk.imports import *

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
    # for pi,para in enumerate(tqdm(para_txts,desc="Parsing paragraphs")):
    for pi,para in enumerate(para_txts):
        try:
            pdoc = nlp(para)
        except AssertionError as e:
            print('!!',e)
            continue

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
    for para in get_tqdm(para_txts,desc="Parsing paragraphs"):
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
    for para in get_tqdm(para_txts,desc="Parsing paragraphs"):
        pdoc = nlp(para)
        for entd in pdoc.ents:
            countd[entd.text]+=1
    
    return countd









TITLE_WORDS = set("""
Miss Ms.
Mr. Mrs.
Lord Lady
Baron Baroness
Count Countess
Sir Madam
Queen King
Goodman` p
Uncle Aunt
Father Mother
Brother Sister
Cousin
""".strip().split())




NLP_FROMTO=None
def nlp_ner_stanza():
    global NLP_FROMTO
    if NLP_FROMTO is not None: return NLP_FROMTO    
    import stanza
    nlp = stanza.Pipeline(lang='en',verbose=False)#,processors='tokenize,ner')
    NLP_FROMTO=nlp
    return nlp


NLP_DATE=None
def nlp_ner_dates_stanza():
    global NLP_DATE
    if NLP_DATE is not None: return NLP_DATE
    import stanza
    nlp = stanza.Pipeline(lang='en',verbose=False,processors='tokenize,ner')
    NLP_DATE=nlp
    return nlp


def nlp_ner_get_doc(txt):
    if type(txt)!=str: return txt
    nlp = nlp_ner_stanza()
    doc = nlp(txt)
    return doc

def nlp_ner_get_doc_simple(txt):
    if type(txt)!=str: return txt
    nlp = nlp_ner_dates_stanza()
    doc = nlp(txt)
    return doc


def get_propn_i_l(sentdf):
    i=0
    o=[]
    propnow=[]
    was_propn=None
    for upos,deprel in zip(sentdf.upos,sentdf.deprel):
        is_propn=upos=='PROPN'
        if is_propn:
            was_propn=True
            o+=[i+1]
        elif was_propn:
            i+=1
            was_propn=False
            o+=['']
        else:
            o+=['']
    return o


def get_ner_sentdf(doc):
    doc=nlp_ner_get_doc(doc)

    sents=[]
    for sent in doc.sentences:
        for word in sent.tokens:
            worddx=word.to_dict()[0]
            worddx['sent_i']=len(sents)+1
            if worddx['text'] in TITLE_WORDS: worddx['upos']='PROPN'
            sents.append(worddx)
    sentdf=pd.DataFrame(sents).fillna('')
    if len(sentdf):
        sentdf['propn_i']=get_propn_i_l(sentdf)
        # decide_recips(sentdf)
    return sentdf



def extract_ents(txt,types={"DATE","TIME"}):
    doc = nlp_ner_get_doc(txt)

    if doc.entities:
        dates = [ent.text for ent in doc.entities if ent.type in types]
        if dates:
            return ' | '.join(dates)
    return ''

def extract_places(txt,**y): return extract_ents(txt,types={'FAC','ORG','GPE'})
def extract_times(txt,**y): return extract_ents(txt,types={'DATE','TIME'})