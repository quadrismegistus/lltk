from lltk.imports import *
from lltk.model.characters import CharacterSystem
from lltk.model.charnet import *
from lltk.corpus.corpus import SectionCorpus


CLAR_ID=f'_chadwyck/Eighteenth-Century_Fiction/richards.01'
CLAR_IDX=f'Eighteenth-Century_Fiction/richards.01'





def get_clarissa():
    clarissa = get_canon()['clarissa']
    return clarissa

def get_clarissa_id():
    return 






##
### CLASSES
##


def getxmlcontents(dom):
    return '\n'.join(str(x) for x in dom.contents)

class TextSectionLetter(TextSection): pass


class TextSectionLetterChadwyck(TextSectionLetter):
    sep_sents='\n'
    sep_paras='\n\n'
    sep_txt='\n\n------------\n\n'

    @property
    def meta(self): return self.metadata(force=False)
    
    def metadata(self,force=False):
        # return self._meta
        # already good?
        # if not force and not {'txt_front','sender_tok','recip_tok'} - set(self._meta.keys()): return self._meta
        if not force and len(self._meta)>2: return self._meta
        # have anything?
        ltr_xml=self.xml
        if not ltr_xml: return self._meta
        ltr_dom=self.dom
        

        # otherwise..
        meta = self._meta
        meta['txt_head']=''
        meta['txt_front']=''
        meta_map={
        # 'id_letter':'idref',
        'txt_front':['front','caption']
        }
        
        for newtag,xtag in meta_map.items():
            meta[newtag]=clean_text(grab_tag_text(ltr_dom, xtag, sep_ln=' ', sep_tag=' | ')) if xtag else ''
        if '</collection>' in ltr_xml and '<attbytes>' in ltr_xml:
            ltrtitle=clean_text(unhtml(ltr_xml.split('</collection>')[-1].split('<attbytes>')[0].strip()))
        else:
            ltrtitle=''
        meta['txt_head']=ltrtitle if ltrtitle!=meta['txt_front'] else ''        
        ltr_txt=self.txt
        meta['txt_start'] = clean_text(ltr_txt[:1000].replace('\n',' ').replace('\t',' '))
        meta['txt_end'] = clean_text(ltr_txt[-1000:].replace('\n',' ').replace('\t',' '))
        
        ### NER
        from lltk.model.ner import extract_places, extract_times, nlp_ner_get_doc_simple
        import dateparser

        itxt=meta.get('txt_head','') + '  ...  ' + meta.get('txt_front','')
        meta['sender_tok'], meta['recip_tok'] = get_sender_recip(itxt)
        
        # start of text
        txt=meta['txt_front'] + '  ' + meta['txt_start'][:100]
        doc=nlp_ner_get_doc_simple(txt)
        meta['date_ner']=extract_times(doc)
        meta['date_time']=str(dateparser.parse(meta['date_ner'])) if meta['date_ner'] else ''
        meta['num_words']=len(tokenize_fast(ltr_txt))
        
        return meta

    @property
    def txt(self,*x,_force=False,**y):
        if not _force and os.path.exists(self.path_txt):
            with open(self.path_txt) as f:
                return f.read()
    
        dom = bs4.BeautifulSoup(self.xml,'lxml')
        ltr_dom = remove_bad_tags(dom, BAD_TAGS)
        letters = list(ltr_dom(self.LTR))
        ltxts=[]
        for ltr in letters:
            ptxts=[]
            paras=list(ltr('p'))
            if not len(paras): paras=[ltr]
            for p in paras:
                sents = p('s')
                if not len(sents):
                    sents=nltk.sent_tokenize(p.text)
                else:
                    sents=[s.text.strip() for s in sents]
                # ptxt=self.sep_sents.join([escape_linebreaks(x) for x in sents if x])
                ptxt=self.sep_sents.join([x.replace('\n',' ') for x in sents if x])
                ptxts.append(ptxt)
            ltrtxt=self.sep_paras.join(ptxts).strip()
            ltxts.append(ltrtxt)
        otxt=self.sep_txt.join(ltxts).strip()
        pref=self.meta.get('txt_front')
        if pref: otxt=pref+'\n\n\n'+otxt
        return clean_text(otxt)


class SectionCorpusLetter(SectionCorpus, CharacterSystem):
    CHARACTER_SYSTEM_ID='letters'

    def get_character_token_counts(
            self,
            char_keys=['sender_tok','recip_tok','char_tok','character_tok']):
        counter=Counter()
        sdf=self.metadata().fillna('')
        if sdf is not None and len(sdf):
            for ckey in char_keys:
                if ckey in set(sdf.columns):
                    for cval in sdf[ckey]:
                        for cvalx in cval.strip().split('; '):
                            counter[cvalx.strip()]+=1
        return counter

    def iter_interactions(self,
            col_sender='sender_tok',
            col_recip='recip_tok',
            col_t='id',
            **kwargs):

        df = self.metadata()#.reset_index()
        cols=set(df.reset_index().columns)
        for colx in [col_sender,col_recip,col_t]:
            if not colx in cols:
                log.error(f'Error: columns {colx} not found in epistolary data df ({cols})')
                return pd.DataFrame()
        df['_sender_id'] = df[col_sender].fillna('').apply(self.get_character_id)
        df['_recip_id'] = df[col_recip].fillna('').apply(self.get_character_id)
        df = df[[col for col in df.columns if col not in {'txt_start','txt_end'}]]
        # df['txt']=[self.letters.text(idx).txt for idx in df['id']]

        for text_id,row in df.iterrows():
            # X wrote letter to Y
            yield dict(
                source=row._sender_id,
                rel='wrote_letter_to',
                target=row._recip_id,
                t=(text_id,0),
                text_id=text_id,
                source_tok=row[col_sender],
                target_tok=row[col_recip],
            )

            ## encloser?
            encloser_id=row.get('id_parent')
            if encloser_id:
                try:
                    encloser_row = df.loc[encloser_id]
                    enclosed_row = row

                    # X enclosed a letter written by Y
                    yield dict(
                        source=encloser_row._sender_id,
                        rel='enclosed_letter_from',
                        target=enclosed_row._sender_id,
                        text_id = encloser_id,
                        source_tok=encloser_row[col_sender],
                        target_tok=enclosed_row[col_sender],
                    )
                except KeyError:
                    pass

                # X enclosed a letter written to Y
                # yield dict(
                #     source=encloser_row._sender_id,
                #     rel='enclosed_letter_to',
                #     target=enclosed_row._recip_id,
                #     text_id = encloser_id,
                #     source_tok=encloser_row[col_sender],
                #     target_tok=enclosed_row[col_recip],
                # )
        


        # return xdf



class SectionCorpusLetterChadwyck(SectionCorpusLetter):
    @property
    def txt(self): return self.get_txt(extra_txt_pref=['txt_front'])

    def init_text(self,*x,_force=True,**y):
        t = super().init_text(*x,**y)
        # save xml?
        if _force or not os.path.exists(t.path_xml):
            o=t.xml
            if o:
                ensure_dir_exists(os.path.dirname(t.path_xml))
                with open(t.path_xml,'w') as of:
                    of.write(o) 
        
        # save txt?
        if _force or not os.path.exists(t.path_txt):
            o=t.txt
            if o:
                ensure_dir_exists(os.path.dirname(t.path_txt))
                with open(t.path_txt,'w') as of:
                    of.write(o)


    def redo_xml(self):
        dom = self.dom
        vols = dom(self.DIV_VOL)
        if not len(vols): vols=[dom]
        newxml = []   
        for voldom in get_tqdm(vols,desc='Iterating volumes 1',position=0): 
            divs = voldom(self.DIV_LTR)
            if not len(divs): divs=[voldom]
            newxml+=[f'<{self.DIV_VOL}>']
            
            for divdom in get_tqdm(divs,desc='Iterating volume letters',position=1,disable=True):
                _xmldiv = clean_text(getxmlcontents(divdom))
                for tagposs in [self.LTR,self.BODY,self.P+'>']:
                    try:
                        _xmldiv_hdr,_xmldiv_body = _xmldiv.split(f'<{tagposs}',1)
                        break
                    except ValueError:
                        pass
                else:
                    # print('!?!?!?!')
                    # print(_xmldiv)
                    continue
                
                divxml2 = f'<{self.DIV_LTR}>{_xmldiv_hdr}<{self.LTR}>{_xmldiv_body}</{self.LTR}></{self.DIV_LTR}>'
                newxml.append(divxml2)
            newxml+=[f'</{self.DIV_VOL}>']
        return bs4.BeautifulSoup('\n'.join(newxml),'lxml')

    def init(self,force=False,lim=None,progress=True,**kwargs):
        if not force and self._init: return
        # from meta?
        super().init(force=force)
        if not force and self._init: return
        
        if log>0: log(f'Initializing: {self.addr}')
        from string import ascii_lowercase
        alpha=ascii_lowercase#.replace('x','')
        alpha = (alpha*1000)
        letter_i=0
        letter_ii=0

        ## VOLUME
        dom = self.redo_xml() #self.dom
        vols = dom(self.DIV_VOL)
        if not len(vols): vols=[dom]
        vol_i=0

        for voldom in get_tqdm(vols,desc='Iterating volumes',position=0): 
            vol_i+=1
            divs = voldom(self.DIV_LTR)
            # if not len(divs): continue
            if not len(divs): divs=[voldom]

            for divdom in get_tqdm(divs,desc='Iterating volume letters',position=1,disable=True):
                ltrs=divdom(self.LTR)
                num_letters = len(ltrs)
                # if not num_letters: continue
                _xmldiv = clean_text(str(divdom))
                _xmldiv_hdr = _xmldiv[:_xmldiv.index(f'<{self.LTR}')]
                
                letter_i+=1
                letter_ii = 0 if num_letters<2 else 1
                
                #meta_hdr = letter_xml_hdr_to_meta(_xmldiv_hdr)
                #idz = meta_hdr.get('id_letter')
                idz = grab_tag_text(divdom,'idref',limit=1)

                for ldomi,ltrdom in enumerate(ltrs):
                    num_enclosed = len(ltrdom(self.LTR))
                    depth_enclosed = len(ltrdom.find_parents(self.LTR))
                    letter_subid = alpha[letter_ii]
                    letter_id=f'L{letter_i:03}{letter_subid}'

                    _xml=clean_text(str(ltrdom))
                    if ldomi==0:
                        try:
                            _xml=_xmldiv_hdr + _xml[_xml.index(f'<{self.LTR}'):]
                        except ValueError:
                            _xml=_xmldiv_hdr + _xml[_xml.index(f'<{self.BODY}'):]

                    odx=dict(
                        id=letter_id,
                        id_orig=idz,
                        vol_i=vol_i,
                        letter_i=letter_i,
                        letter_ii=letter_ii,
                        num_enclosed=num_enclosed,
                        depth_enclosed=depth_enclosed,
                        _xml=_xml,
                        _xml_ref=f'<letter id="{letter_id}" />'
                    )
                    ltrdom.odx=odx
                    letter_ii+=1
        
        for ltrdom in dom(self.LTR):
            parent=ltrdom.find_parent(self.LTR)
            if parent:
                ltrdom.odx['id_parent']=parent.odx['id']
                parent.odx['_xml'] = parent.odx['_xml'].replace( ltrdom.odx['_xml'], ltrdom.odx['_xml_ref'] )
            else:
                ltrdom.odx['id_parent']=''

        # o=[]
        oi=0
        for ltrdom in dom(self.LTR):
            # ltrdom.odx['_xml'] = ltrdom.odx['_xml_ref'][:-3] + ltrdom.odx['_xml'][1+len(self.LTR):]
            okeys=[
                # 'id',
                'id_orig',
                'id_parent',
                'vol_i',
                'letter_i',
                'letter_ii',
                'num_enclosed',
                'depth_enclosed',
                '_xml'
            ] #txt_head','txt_front','_xml']
            odx=dict((k,ltrdom.odx.get(k)) for k in okeys)

            ## nowww init text
            t=self.init_text(ltrdom.odx['id'], **odx)
            oi+=1
            # o.append(odx)
        # odf=pd.DataFrame(o).fillna('')
        if oi: self._init=True
        # return self._init
        # return odf

        





from lltk.model.characters import CharacterSystem
class TextEpistolary(BaseText):
    DIV_VOL=''
    DIV_LTR=''
    DIV=''
    LTR=''
    BODY=''
    P=''
    SECTION_CLASS=TextSectionLetter
    SECTION_DIR_NAME='letters'

    def characters(self,systems={'letters','booknlp'},**kwargs):
        return super().characters(systems=systems,**kwargs)

    def interactions(self,ignore_blank=True,**kwargs):
        odf=super().interactions(**kwargs)
        # overwrite NARRATOR with author
        odf_wrote = odf[odf.rel=='wrote_letter_to']
        writers = dict(zip(odf_wrote.text_id, odf_wrote.source))

        if ignore_blank:
            odf=odf[~odf.source.isin(BAD_CHAR_IDS)]
            odf=odf[~odf.target.isin(BAD_CHAR_IDS)]

        def getnewsrc(src,text_id):
            if src!=BOOKNLP_NARRATOR_ID: return src
            if text_id not in writers: return src
            o=writers[text_id]
            if not o or o in BAD_CHAR_IDS: return src
            return o

        odf['source']=[
            getnewsrc(src,text_id)
            for src,text_id in zip(odf.source, odf.text_id)
        ]

        return odf






class TextEpistolaryChadwyck(TextEpistolary):
    DIV_VOL='div2'
    DIV_LTR='div3'
    LTR='letter'
    BODY='body'
    P='p'
    SECTION_CLASS=TextSectionLetterChadwyck
    SECTION_CORPUS_CLASS=SectionCorpusLetterChadwyck

class Epistolary(BaseCorpus):
    NAME='Epistolary'
    ID='epistolary'
    TEXT_CLASS=TextEpistolary
    CORPORA_TO_START_WITH = ['Chadwyck']


    def init_text(self,*args,**kwargs):
        t=super().init_text(*args,**kwargs)
        if t.source and t.source.corpus.id=='chadwyck':
            t.__class__ = TextEpistolaryChadwyck
        return t


##
# Recips
##



def deduce_recip(txt,recip_words={'to'},sender_words={'from'}):
    from lltk.model.ner import get_ner_sentdf
    sentdf=get_ner_sentdf(txt)
    groupd={}
    o=[]
    for propn_i,propndf in sorted(sentdf[sentdf.propn_i!=''].groupby('propn_i')):
        propn_i0 = propndf.index[0] - 1
        if propn_i0>=0:
            prefix_word = sentdf.iloc[propn_i0].text.lower()
            if prefix_word in recip_words:
                groupd[propn_i]='recip'
                
                if propn_i>1:
                    # sender maybe?
                    groupd[propn_i-1] = 'sender'
            elif prefix_word in sender_words:
                groupd[propn_i]='sender'
    
    sentdf['epistolary_role']=sentdf.propn_i.apply(lambda pi: groupd.get(pi,''))
    return sentdf

def get_sender_recip(txt,*x,**y):
    if not txt: return '?','?'
    byline_sentdf = deduce_recip(txt,*x,**y)
    sender,recip = '',''
    if 'epistolary_role' in set(byline_sentdf.columns):
        sender=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='sender'].text)
        recip=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='recip'].text)
    return (sender if sender else '?', recip if recip else '?')


def calculate_tok2id(df_text_letters):
    tdf=df_text_letters

    # count it
    counts=Counter()
    l=[]
    for key in ['sender_tok','recip_tok']:
        for x in tdf[key].fillna(''):
            if x:
                l+=[x]
    char_toks=Counter(l)
    
    o_text=[]
    for char_tok,char_tok_count in char_toks.most_common():
        if type(char_tok)!= str or not char_tok: continue
        char_id=zeropunc(char_tok)
        otextd=dict(char_tok=char_tok,char_id=char_id,char_tok_count=char_tok_count,char_tok_i=len(o_text)+1)
        o_text.append(otextd)
    odf_tok2id=pd.DataFrame(o_text)
    return odf_tok2id.sort_values(
        ['char_tok_count','char_tok_i'],
        ascending=[False,True]
    )


##
# Networks
##




def iter_letter_networks_from_dfletters(
        dfletters,
        key_source='source',
        key_target='target',
        key_rel='rel',
        bad_ids=BAD_CHAR_IDS,
        progress=True,
        *x,**y):
    G=nx.DiGraph()
    iterr=get_tqdm(
        dfletters.to_dict(orient='records'),
        disable=not progress,
        desc='Iterating letters as networks'
    )
    for row in iterr:
        sender_id=row.get(key_source,'')
        recip_id=row.get(key_target,'')
        if not sender_id or not recip_id: continue
        if sender_id in bad_ids or recip_id in bad_ids: continue
        
        # node_types = ['sender','recip']
        # for node_type in node_types:
        #     node_id=row[f'{node_type}_id']
        #     if not G.has_node(node_id):
        #         node_feats=dict((k.replace(f'{node_type}_','char_'),v) for k,v in row.items() if k.startswith(f'{node_type}_'))
        #         G.add_node(node_id,**node_feats)
        
        # edge_attrs = dict((k,v) for k,v in row.items() if not k.split('_')[0] in set(node_types))
        edge_attrs={}
        if not G.has_edge(sender_id, recip_id):
            edge_attrs['weight']=1
            G.add_edge(sender_id,recip_id,**edge_attrs)
        else:
            G.edges[(sender_id,recip_id)]['weight']+=1
            for ek,ev in edge_attrs.items():
                G.edges[(sender_id,recip_id)][ek]=ev


        for a,b,d in G.edges(data=True): G.edges[(a,b)]['color']='black'
        G.edges[(sender_id,recip_id)]['color']='red'
        
        yield G

def get_letter_network_from_dfletters(dfletters,progress=False,*x,**y):
    g=None
    for g in iter_letter_networks_from_dfletters(dfletters,progress=progress,*x,**y): pass
    return g


def get_canon(idx=None):
    C = Epistolary()
    d=dict(
        clarissa=C.text('_chadwyck/Eighteenth-Century_Fiction/richards.01'),
        pamela=C.text('_chadwyck/Eighteenth-Century_Fiction/richards.04'),
        evelina=C.text('_chadwyck/Eighteenth-Century_Fiction/burney.01'),
    )
    return d if not idx else d.get(idx)

def get_clarissa(): return get_canon().get('clarissa')
def get_pamela(): return get_canon().get('pamela')
def get_evelina(): return get_canon().get('evelina')


# t = C.init_text('_chadwyck/Eighteenth-Century_Fiction/richards.01')    # clarissa
# # t = C.init_text('_chadwyck/Eighteenth-Century_Fiction/richards.04')  # pamela
# # t = C.init_text('_chadwyck/Eighteenth-Century_Fiction/smollett.03')  # clinker
# # t=C.init_text('_chadwyck/Eighteenth-Century_Fiction/brookefm.02')    # julia mandeville