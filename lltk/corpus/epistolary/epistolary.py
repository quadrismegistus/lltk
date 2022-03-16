from lltk.imports import *
from lltk.model.charnet import *
from lltk.corpus.corpus import SectionCorpus


CLAR_ID=f'_chadwyck/Eighteenth-Century_Fiction/richards.01'
CLAR_IDX=f'Eighteenth-Century_Fiction/richards.01'

chardata_metakeys_initial = dict(
    char_race='',
    char_gender='',
    char_class='',
    char_geo_birth='',
    char_geo_marriage='',
    char_geo_death='',
    char_geo_begin='',
    char_geo_middle='',
    char_geo_end='',
)




def get_canon():
    Chad = load('Chadwyck')
    return dict(
        clarissa = Chad.textd['Eighteenth-Century_Fiction/richards.01'],
        pamela=Chad.textd['Eighteenth-Century_Fiction/richards.04'],
        evelina=Chad.textd['Eighteenth-Century_Fiction/burney.01'],
    )

def get_clarissa():
    clarissa = get_canon()['clarissa']
    return clarissa

def get_clarissa_id():
    return 






##
### CLASSES
##




class TextSectionLetter(TextSection): pass
    # def deduce_recip(self, meta=None,keys=['txt_front','txt_head']):
    #     from lltk.model.ner import get_ner_sentdf
    #     ltr_meta=meta if meta is not None else self._meta

    #     txt = '     '.join(
    #         ltr_meta.get(argname,'').replace(' | ',' ')
    #         for argname in keys
    #     )
        
    #     byline_sentdf = None
    #     sender,recip = '?','?'
        
    #     if txt:
    #         if not ltr_meta.get('sender_tok'):
    #             if byline_sentdf is None: byline_sentdf = deduce_recip(txt)
    #             if 'epistolary_role' in set(byline_sentdf.columns):
    #                 sender=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='sender'].text)



    #         ## recip
    #         if not ltr_meta.get('recip_tok'):
    #             if byline_sentdf is None: byline_sentdf = deduce_recip(txt)
    #             if 'epistolary_role' in set(byline_sentdf.columns):
    #                 recip=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='recip'].text)
        
    #     ltr_meta['sender_tok']=sender if sender else '?'
    #     ltr_meta['recip_tok']=recip if recip else '?'



class TextSectionLetterChadwyck(TextSectionLetter):
    sep_sents='\n'
    sep_paras='\n\n'
    sep_txt='\n\n------------\n\n'

    @property
    def meta(self):
        # return self._meta
        # already good?
        if not {'txt_front','sender_tok','recip_tok'} - set(self._meta.keys()): return self._meta
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
            ltrtitle=ltr_xml.split('</collection>')[-1].split('<attbytes>')[0].strip()
        else:
            ltrtitle=''
        meta['txt_head']=ltrtitle if ltrtitle!=meta['txt_front'] else ''
        
        ## deduce recips?
        # self.deduce_recip(meta)
        txt = meta.get('txt_front')
        meta['sender_tok'], meta['recip_tok'] = get_sender_recip(txt)
        return meta


    @property
    def txt(self,*x,**y):
        ltr_dom = remove_bad_tags(self.dom, BAD_TAGS)
        letters = list(ltr_dom(self.LTR))
        if not len(letters): letters=[ltr_dom]
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
        return clean_text(otxt)



class SectionCorpusLetterChadwyck(SectionCorpus):
    # def init(self,force=False,lim=None,progress=False,**kwargs):
    #     initd=super().init(force=force)
    #     if not force and initd: return self._init
        
    #     div_strs=[
    #         ltrxml.split(f'<{self.DIV}>',1)[-1].strip()
    #         for ltrxml in self.xml.split(f'</{self.DIV}>')[:-1]
    #         if f'</{self.LTR}>' in ltrxml.split(f'<{self.DIV}>',1)[-1]
    #     ]
    #     letter_i=0
    #     iterr=tqdm(div_strs, disable=not progress, desc='Scanning for letters')
        
    #     for ltrxml in iterr:
    #         letter_i+=1 #len(o)+1
    #         letter_id=f'L{letter_i:03}'
    #         ltr=self.init_text(
    #             id=letter_id,
    #             letter_i=letter_i,
    #             _xml=ltrxml,
    #         )
        
    #     self._init=True


    def init(self,force=False,lim=None,progress=True,**kwargs):
        if not force and self._init: return

        # from meta?
        super().init(force=force)
        if not force and self._init: return
        
        
        log.debug(f'Initializing: {self}')
        from string import ascii_lowercase
        alpha=ascii_lowercase#.replace('x','')
        alpha = (alpha*1000)
        letter_i=0
        letter_ii=0

        ## VOLUME
        dom = self.dom
        vols = dom(self.DIV_VOL)
        if not len(vols): vols=[dom]
        vol_i=0
        for voldom in tqdm(vols,desc='Iterating volumes',position=0): 
            divs = voldom(self.DIV_LTR)
            if not len(divs): continue
            vol_i+=1

            for divdom in tqdm(divs,desc='Iterating volume letters',position=1,disable=True):
                ltrs=divdom(self.LTR)
                num_letters = len(ltrs)
                if not num_letters: continue
                letter_i+=1
                letter_ii = 0 if len(ltrs)==1 else 1

                _xmldiv = clean_text(str(divdom))
                _xmldiv_hdr = _xmldiv[:_xmldiv.index(f'<{self.LTR}')]
                #meta_hdr = letter_xml_hdr_to_meta(_xmldiv_hdr)
                #idz = meta_hdr.get('id_letter')
                idz = grab_tag_text(divdom,'idref',limit=1)

                for ldomi,ltrdom in enumerate(ltrs):
                    num_enclosed = len(ltrdom(self.LTR))
                    depth_enclosed = len(ltrdom.find_parents(self.LTR))
                    letter_subid = alpha[letter_ii]
                    letter_id=f'L{letter_i:03}{letter_subid}'

                    _xml=clean_text(str(ltrdom))
                    if ldomi==0: _xml=_xmldiv_hdr+_xml

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
            ltrdom.odx['_xml'] = ltrdom.odx['_xml_ref'][:-3] + ltrdom.odx['_xml'][1+len(self.LTR):]
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

        



class TextEpistolaryChadwyck(BaseText):
    DIV_VOL='div2'
    DIV_LTR='div3'
    LTR='letter'
    SECTION_CLASS=TextSectionLetterChadwyck
    SECTION_CORPUS_CLASS=SectionCorpusLetterChadwyck




class TextEpistolary(BaseText):
    DIV=''
    LTR=''
    SECTION_CLASS=TextSectionLetter

    @property
    def letters(self): return []


class Epistolary(BaseCorpus):
    NAME='Epistolary'
    ID='epistolary'
    TEXT_CLASS=TextEpistolary
    CORPORA_TO_START_WITH = ['Chadwyck']

    def init(self,*x,**y):
        super().init(*x,**y)
        
        # recast text objects by corpus
        for idx,t in self._textd.items():
            if t.source and t.source.corpus.id=='chadwyck':
                t.__class__ = TextEpistolaryChadwyck


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




def iter_letter_networks_from_dfletters(dfletters,bad_ids={'?',''},progress=True,*x,**y):
    G=nx.DiGraph()
    iterr=tqdm(
        dfletters.to_dict(orient='records'),
        disable=not progress,
        desc='Iterating letters as networks'
    )
    for row in iterr:
        sender_id=row.get('sender_id','')
        recip_id=row.get('recip_id','')
        if not sender_id or not recip_id: continue
        if sender_id in bad_ids or recip_id in bad_ids: continue
        
        node_types = ['sender','recip']
        for node_type in node_types:
            node_id=row[f'{node_type}_id']
            if not G.has_node(node_id):
                node_feats=dict((k.replace(f'{node_type}_','char_'),v) for k,v in row.items() if k.startswith(f'{node_type}_'))
                G.add_node(node_id,**node_feats)
        
        edge_attrs = dict((k,v) for k,v in row.items() if not k.split('_')[0] in set(node_types))
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
    for g in iter_letter_networks_from_dfletters(dfletters,progress=progress,*x,**y): pass
    return g


