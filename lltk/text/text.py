# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import os,codecs,gzip,re
from collections import Counter
from lltk import tools
from six.moves import map
import six,re
from six.moves import zip
nlp=None
from smart_open import open
from collections import Counter
try:
    import ujson as json
except ImportError:
    import json

ENGLISH=None

ADDR_SEP='|'

class Text(object):
    def __init__(self,idx,corpus,path_to_xml=None,path_to_txt=None):
        self._id=str(idx)
        self.corpus=corpus
        self._sections=[]
        self._fnfn_xml=path_to_xml
        self._fnfn_txt=path_to_txt

    @property
    def addr(self):
        return self.corpus.name+ADDR_SEP+self.id
    
    ## Convenience functions
    @property
    def id(self): return self._id
    @property
    def nation(self): return self.meta.get('nation','')
    ###
    
    
    ### PATHS
    
    # get path from id folder
    def get_path(self,fn='text.txt',dirname=None,force=False):
        if not dirname: dirname=self.path
        path=os.path.join(dirname, fn)
        pathgz=path+'.gz'
        for p in [path,pathgz]:
            if os.path.exists(p): return p
        return None if not force else path
    
    def get_paths(self,fn,toplevel=False,bottomlevel=True,texttype=None):
        # if toplevel exists, return that
        if toplevel:
            fnfn=self.get_path(fn)
            if fnfn: return [fnfn]
        
        # otherwise loop
        paths=[]
        for root,dirs,fns in sorted(os.walk(self.path)):
            if fn in fns:
                # if bottomlevel set and there are more folders here
                if bottomlevel and dirs: continue
        
                # check if text type set
                if texttype:
                    metafn=os.path.join(root,'meta.json')
                    if not os.path.exists(metafn): continue
                    with open(metafn) as f:
                        meta=json.load(f)
                    if meta.get('_type')!=texttype: continue
                
                # otherwise append path
                paths.append(os.path.join(root,fn))
        return paths
    
    @property
    def path(self): return os.path.join(self.corpus.path_texts, self.id)       
    @property
    def path_txt(self): return self.get_path('text.txt')
    @property
    def path_xml(self): return self.get_path('text.xml')
    @property
    def path_anno(self): return self.get_path('text.anno.xml')
    @property
    def path_hdr(self): return self.get_path('meta.hdr')
    @property
    def path_meta(self): return self.get_path('meta.json')
    @property
    def path_sgml(self): return self.get_path('text.sgm')
    @property
    def path_freqs(self): return self.get_path('freqs.json')
    @property
    def path_spacy(self): return self.get_path('text.parsed.spacy')
    @property
    def path_stanza(self): return self.get_path('text.parsed.stanza')
    @property
    def paths_txt(self):
        return self.get_paths('text.txt',toplevel=True,bottomlevel=False)
    @property
    def paths_xml(self):
        return self.get_paths('text.xml',toplevel=True,bottomlevel=False)
    @property
    def paths_freqs(self):
        return self.get_paths('freqs.json',toplevel=True,bottomlevel=False)

    ## exists?
    @property
    def exists_xml(self): return bool(self.paths_xml)
    @property
    def exists_txt(self): return bool(self.paths_txt)
    
    
    ## Read text
    def get_text(self,fn='text.txt'):
        txts=[]
        for p in self.get_paths(fn,toplevel=True,bottomlevel=False):
            with open(p) as f:
                txts.append(f.read())
        return '\n\n\n\n'.join(txts)
    
    # text funcs
    @property
    def xml(self): return self.get_text('text.xml')
    @property
    def txt(self):
        if self.exists_txt: return self.get_text('text.txt')
        if self.exists_xml: return self.xml2txt()

    def xml2txt(self,OK={'p','l'}, BAD=[], body_tag='text',xml=None):
        return xml2txt(xml if xml else self.xml, OK=OK, BAD=BAD, body_tag=body_tag)

    
    @property
    def dom(self):
        import bs4
        return bs4.BeautifulSoup(self.get_text('text.xml'),'lxml')
    
    
    
    ### Tokenize
    def save_txt(self,force=False,compress=False):
        if not force and self.exists_txt: return
        # convert from xml?
        for path in self.paths_xml:
            path_txt=path.replace('text.xml','text.txt')
            with open(path) as f_xml, open(path_txt,'w') as of_txt:
                xmlstr=f_xml.read()
                txt=self.xml2txt(xml=xmlstr)
                of_txt.write(txt)
   

    def tokenize(self,modernize_spelling=False):
        # tokenize
        txt=self.txt.replace('\n','\\')
        txt=txt.replace('&hyphen;','-')
        words=re.findall(r"[\w]+|[^\s\w]", txt)
        words=[w if w!='\\' else '\n' for w in words]

        # modernize spelling?
        try:
            if modernize_spelling:
                words=[self.corpus.modernize_spelling(w) for w in words]
        except AttributeError:
            pass

        # return
        return words

    def tokenize(self, modernize_spelling=False):
        return tokenize_text(self.txt)

    def modernize(self,w):
        return self.corpus.modernize_spelling(w)
                
    def count(self):
        return Counter(self.tokenize())

    @property
    def tokens(self): return list(self.tokenize())
    @property
    def counts(self): return self.count()
    
    
    
    
    
    ### Sections
    def sections_xml(self,divider_tag,text_section_class=None):
        xml=self.xml
        o=[]
        for part in xml.split('<'+divider_tag)[1:]:
            part_type=part.split(' TYPE="')[1].split('"')[0]
            part='<'+divider_tag+part
            o+=[(divider_tag,text_section_class,part_type,part)]
        return o

    def get_section(self,idx):
        for s in self.sections:
            if s.id==idx:
                return s

    def load_sections(self):
        o=[]
        for i,(divider_tag,text_section_class,section_type,section_xml) in enumerate(self.sections_xml()):
            if not text_section_class: text_section_class=TextSection
            o+=[text_section_class(text=self,section_num=i+1,section_type=section_type,section_xml=section_xml,section_txt=None,divider_tag=divider_tag)]
        self._sections=o

    def unload_sections(self):
        self._sections=[]

    @property
    def sections(self):
        if not hasattr(self,'_sections') or not self._sections:
            self.load_sections()
        return self._sections



    @property
    def genre(self): return self.meta.get('genre','')

    @property
    def medium(self):
        medium=self.meta.get('medium','')
        return medium

    @property
    def author(self): return self.meta.get('author','')

    def get_author_gender(self,name):
        # Guess
        import re
        import gender_guesser.detector as gender
        isnt_this_problematic_just_to=gender.Detector()
        genders = [isnt_this_problematic_just_to.get_gender(x) for x in re.split('\W',self.author)]
        for x in genders:
            if x!='unknown':
                return x
        return 'unknown'

    @property
    def author_gender(self):
        if 'gender' in self.meta and self.meta['gender']: return self.meta['gender']
        if 'author_gender' in self.meta and self.meta['author_gender']: return self.meta['author_gender']
        return self.get_author_gender(self.author)

    def get_is_posthumous(self,author,year):
        author_dates=self.get_author_dates(author)
        if year and author_dates[0] and year>author_dates[0]:
            if not author_dates[1]:
                if author_dates[0] > 1900: # so it's plausible they're still alive
                    return False
            else: # has death date
                if year>author_dates[1]:
                    return True
                else:
                    return False
        return None


    @property
    def is_posthumous(self):
        return self.get_is_posthumous(self.author,self.year)



    @property
    def year(self):
        if type(self.meta['year']) in {int,float}:
            return int(self.meta['year'])

        try:
            yearstr=''.join([x for x in str(self.meta['year']) if x.isdigit()])[:4]
            while len(yearstr)<4:
                yearstr+='0'
            return int(float(yearstr))
        except (ValueError,TypeError,KeyError) as e:
            #except AttributeError:
            return 0

    @property
    def year_author_is_30(self):
        try:
            dob=int(self.author_dates[0])
            return dob+30
        except (ValueError,TypeError,KeyError) as e:
            return 0

    @property
    def title(self): return six.text_type(self.meta['title'])

    @property
    def decade(self): return int(self.year)/10*10

    @property
    def twentyyears(self): return int(self.year)/20*20

    @property
    def quartercentury(self): return int(self.year)/25*25

    @property
    def halfcentury(self): return int(self.year)/50*50

    @property
    def century(self): return int(self.year)/100*100

    def get_author_dates(self,author=None):
        if not author: author=self.author
        return get_author_dates(author)

    @property
    def author_dates(self,keys=['author','author_dob','a1']):
        if hasattr(self,'_author_dates'): return self._author_dates
        dob=self.meta.get('author_dob')
        dod=self.meta.get('author_dod')

        if dob and dod: self._author_dates=(dob,dod)
        elif dob: self._author_dates=(dob,0)
        elif dod: self._author_dates=(0,dod)
        else: self._author_dates=get_author_dates(self.author)

        return self._author_dates

    def get_meta_from_corpus_metadata(self):
        try:
            return self.corpus._metad.get(self.id)
        except AttributeError:
            return {}

    def get_metadata(self,from_metadata=True,from_files=True):
        """
        This function is called by the corpus class to construct...
            corpus.meta:          a list of dictionaries
            corpus.metad:         a dictionary, keyed {text_id:text_metadata dictionary}
            corpus.metadata:      a pandas dataframe
            corpus.get_text_ids   (only if path_metadata exists)

        This function calls in order:
            self.get_meta_from_corpus_metadata()
            self.get_meta_from_file()
            self.meta_by_file
        """

        def do_return(meta):
            if not 'corpus' in meta: meta['corpus']=self.corpus.name
            if not 'id' in meta: meta['id']=self.id
            if not '_lltk_' in meta: meta['_lltk_']=self.addr
            return meta

        if hasattr(self,'_meta'): return do_return(self._meta)

        if from_metadata:
            meta=self.get_meta_from_corpus_metadata()
            if meta: return do_return(meta)

        if from_files:
            meta=self.get_meta_from_file()
            if meta: return do_return(meta)

            meta=self.meta_by_file
            if meta: return do_return(meta)
        return do_return({})


    @property
    def meta(self):
        return self.get_metadata()

    
    
    ## Parsing
    @property
    def spacy(self):
        return
        return self.get_spacy()

    def get_spacy(self,load_from_file=False,model_name='en_core_web_sm'):
        import spacy
        global nlp
        if not nlp:
            #print('>> loading spacy...')
            nlp = spacy.load(model_name)

        doc=None
        if self.parsed and load_from_file:
            #print self.fnfn_spacy
            from spacy.tokens.doc import Doc

            try:
                for byte_string in Doc.read_bytes(open(self.fnfn_spacy, 'rb')):
                    doc = Doc(nlp.vocab)
                    doc.from_bytes(byte_string)
            except UnicodeDecodeError:
                print("!! UNICODE ERROR:",self.fnfn_spacy)
        #else:

        if not doc:
            #print '>> making spacy document for text',self.id
            txt=self.text
            txt=clean_text(txt)
            doc=nlp(txt)

        return doc


    def text_plain(self, force_xml=None):
        """
        This function returns the plain text file. You may want to modify this.
        """

        # Return plain text version if it exists
        if self.exists_txt and not force_xml:
            return self.text_plain_from_txt()

        # Otherwise, load from XML?
        if self.exists_xml and hasattr(self,'text_plain_from_xml'):
            return self.text_plain_from_xml()

        return ''


    ## TOKENS

    @property
    def nltk(self):
        import nltk
        return nltk.Text(self.tokens)

    @property
    def blob(self):
        from textblob import TextBlob
        return TextBlob(self.txt)

    @property
    def num_words(self,keys=['num_words','_num_words']):
        for k in keys:
            if k in self.meta:
                return float(self.meta[k])
        return len(self.tokens)

    @property
    def words_recognized(self):
        global ENGLISH

        if not ENGLISH:
            from lltk.tools import get_english_wordlist
            ENGLISH=get_english_wordlist()

        return [w for w in self.words if w in ENGLISH]

    @property
    def ocr_accuracy(self):
        return float(len(self.words_recognized)) / len(self.words)

    @property
    def minhash(self):
        from datasketch import MinHash
        m = MinHash()
        for word in self.tokens:
            m.update(word.encode('utf-8'))
        return m

    @property
    def length(self):
        return self.num_words

    @property
    def num_words_recognized(self):
        return len(self.tokens_recognized)
        #return int(self.num_words * self.ocr_accuracy) if self.ocr_accuracy else self.num_words

    @property
    def length_recognized(self):
        return self.num_words_recognized


    @property
    def freqs_tokens(self):
        from collections import Counter
        return Counter([x.lower() for x in self.tokens])

    @property
    def is_tokenized(self):
        ofolder=os.path.join(self.corpus.path, 'freqs', self.corpus.name)
        ofnfn=os.path.join(ofolder,self.id+'.json')
        return os.path.exists(ofnfn) and os.stat(ofnfn).st_size

    def save_freqs_json(self):
        for fn in self.paths_txt:
            save_freqs_json(fn)
    
    @property
    def freqs(self):
        paths=self.paths_freqs
        if not paths:
            self.save_freqs_json()
            paths=self.paths_freqs
            if not paths: return
        c=Counter()
        for path in paths:
            with open(path) as f:
                c.update(json.load(f))
        return c
        

                
                
                
                
                
                
                
                



REPLACEMENTS={
                '&hyphen;':'-',
                '&sblank;':'--',
                '&mdash;':' -- ',
                '&ndash;':' - ',
                '&longs;':'s',
                u'\u2223':'',
                u'\u2014':' -- ',
                '|':'',
                '&ldquo;':u'“',
                '&rdquo;':u'”',
                '&lsquo;':u'‘’',
                '&rsquo;':u'’',
                '&indent;':'     ',
                '&amp;':'&',


            }

import bleach
def clean_text(txt,replacements=REPLACEMENTS):
    for k,v in list(replacements.items()):
        txt=txt.replace(k,v)
    #return bleach.clean(txt,strip=True)
    return txt






class TextMeta(Text):
    def __init__(self,texts):
        self.texts=texts

    @property
    def id(self):
        return self.texts[0].id

    @property
    def corpus(self):
        return self.texts[0].corpus

    def lines_txt(self):
        return self.texts[0].lines_txt()

    @property
    def meta(self):
        if not hasattr(self,'_meta'):
            self._meta=md={'corpus':self.corpus.name}
            for t in reversed(self.texts):
                for k,v in list(t.meta.items()):
                    #if k in md and md[k]:
                    #	k=k+'_'+t.__class__.__name__.replace('Text','').lower()
                    md[k]=v
        return self._meta


    @property
    def is_metatext(self):
        return True




class TextSection(Text):
    def __init__(self,text,section_num,section_type,section_xml=None,section_txt=None,divider_tag=None):
        self.parent=text
        self.num=section_num
        self.type=section_type
        self.is_text_section = True
        self._xml=section_xml
        self._txt=section_txt
        if divider_tag:
            self.divider_tag=divider_tag.lower()
        elif hasattr(self,'DIVIDER_TAG') and self.DIVIDER_TAG:
            self.divider_tag=self.DIVIDER_TAG
        else:
            self.divider_tag=None

    @property
    def fnfn_freqs(self):
        fnfn=os.path.join(self.corpus.path, 'freqs', self.corpus.parent.name, self.id+'.json')
        return fnfn

    @property
    def text_xml(self):
        if not self._xml:
            self.parent.load_sections()
        return self._xml

    def unload(self):
        self._xml=''
        self._txt=''



    @property
    def id(self):
        return self.parent.id+'/section'+str(self.num).zfill(3)

    @property
    def corpus(self):
        return self.parent.corpus.sections

    def lines_txt(self):
        for ln in self.text_plain().split('\n'):
            yield ln

    def load_metadata(self):
        # #md=self._meta=dict(self.parent.meta.items())
        # if not hasattr(self,'_meta'):
        # 	md=self._meta={}
        # else:
        # 	md=self._meta
        #
        md={}
        md['id']=self.id
        md['text_id']=self.parent.id
        md['section_type']=self.type
        return md


    @property
    def meta(self):
        if not hasattr(self,'_meta'):
            md=self._meta={}
        else:
            md=self._meta
        for k,v in list(self.parent.meta.items()):
            if not k in md or not md[k]:
                md[k]=v
        #for k,v in self.load_metadata().items(): md[k]=v
        return self._meta


    @property
    def meta_by_file(self):
        self._meta=self.load_metadata()
        return self._meta


def text_plain_from_xml(xml, OK={'p','l'}, BAD=[], body_tag='text'):
    #print '>> text_plain from stored XML file...'
    import bs4

    ## get dom
    dom = bs4.BeautifulSoup(xml,'lxml') if type(xml) in [str,six.text_type] else xml
    txt=[]
    ## remove bad tags
    for tag in BAD:
        [x.extract() for x in dom.findAll(tag)]
    ## get text
    for doc in dom.find_all(body_tag):
        for tag in doc.find_all():
            if tag.name in OK:
                txt+=[clean_text(tag.text)]
    TXT='\n\n'.join(txt).replace(u'∣','')
    return TXT


def get_author_dates(author):
    import re
    dates = [x for x in re.split('\W',author) if x.isdigit() and len(x)==4]
    if not dates:
        return (None,None)
    elif len(dates)==1:
        return (int(dates[0]),None)
    else:
        return tuple([int(x) for x in dates[:2]])
    return (None,None)

def tokenize_fast(line):
    return re.findall("[A-Z]{2,}(?![a-z])|[A-Z][a-z]+(?=[A-Z])|[\'\w\-]+",line.lower())

def tokenize_text(txt):
    return tokenize_fast(txt)

def tokenize_nltk(txt):
    from nltk import word_tokenize
    txt=txt.lower().strip()
    toks=word_tokenize(txt)
    toks=[tok for tok in toks if tok and tok[0].isalpha()]
    return toks



########################################################
class PlainText(Text):
    def __init__(self,txt=None,path_to_txt=None):
        self._id=None
        self.corpus=None
        self._sections=[]
        self._fnfn_xml=None
        self._fnfn_txt=path_to_txt
        self._txt=txt

    @property
    def txt(self):
        if self._txt: return self._txt
        return self.text









def save_freqs_json(ifnfn):
    if not os.path.exists(ifnfn): return
    ofnfn=ifnfn.replace('text.txt','freqs.json')
    if os.path.exists(ofnfn): return

    # read txt
    with open(ifnfn) as f: txt=f.read()

    # tokenize
    toks=tokenize_text(txt)

    # count
    tokd=dict(Counter(toks))

    # save
    with open(ofnfn,'w') as of: json.dump(tokd,of)

    # return?
    # return tokd
    
def save_xml2txt(path_xml):
    if not force and self.exists_txt: return
    # convert from xml?
    for path in self.paths_xml:
        path_txt=path.replace('text.xml','text.txt')
        with open(path) as f_xml, open(path_txt,'w') as of_txt:
            xmlstr=f_xml.read()
            txt=self.xml2txt(xml=xmlstr)
            of_txt.write(txt)
    
    
def xml2txt(xml,OK={'p','l'}, BAD=[], body_tag='text'):
    import bs4
    dom=bs4.BeautifulSoup(xml,'lxml')
    # start txt
    txt=[]
    # remove bad tags
    for tag in BAD: [x.extract() for x in dom.findAll(tag)]
    # get text
    for doc in dom.find_all(body_tag):
        # get all tags
        for tag in doc.find_all():
            # if ok
            if tag.name in OK:
                txt+=[clean_text(tag.text)]

    TXT='\n\n'.join(txt).replace(u'∣','')
    return TXT

    
    
def xml2txt_save(path_xml,**attrs):
    with open(path_xml,encoding='latin1') as f: xml=f.read()
    txt=xml2txt(xml,**attrs)
    path_txt=path_xml.replace('.txt','.xml')
    print(path_xml,path_txt)
    stop
    with open(path_txt,'w') as of: of.write(txt)