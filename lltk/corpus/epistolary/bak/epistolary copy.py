from lltk.imports import *
from lltk.model.charnet import *

CLAR_ID=f'_chadwyck/Eighteenth-Century_Fiction/richards.01'
CLAR_IDX=f'Eighteenth-Century_Fiction/richards.01'




def epistolarized_chadwyck_ltr_recip(ltr_meta, keys=['txt_front','txt_head']):
    txt = '     '.join(
        ltr_meta.get(argname,'').replace(' | ',' ')
        for argname in keys
    )
    byline_sentdf = None
    if not ltr_meta.get('sender_tok'):
        if byline_sentdf is None: byline_sentdf = get_sentdf(txt)
        if 'epistolary_role' in set(byline_sentdf.columns):
            ltr_meta['sender_tok']=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='sender'].text)
        else:
            ltr_meta['sender_tok']=''

    ## recip
    if not ltr_meta.get('recip_tok'):
        if byline_sentdf is None: byline_sentdf = get_sentdf(txt)
        if 'epistolary_role' in set(byline_sentdf.columns):
            ltr_meta['recip_tok']=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='recip'].text)
        else:
            ltr_meta['recip_tok']=''

    return ltr_meta



def epistolarized_chadwyck_ltr(ltr_dom,id_text='',*x,**y):
    ltr_meta = epistolarized_chadwyck_ltr_meta(ltr_dom,*x,**y)
    ltr_meta['id_text']=id_txt
    txt=epistolarized_chadwyck_ltr_txt(ltr_dom,*x,**y)
    return ltr_meta,txt




def join_if(*l,sep):
    return sep.join(str(x) for x in l if x)



def parse_possibly_nested_letters(ltr_dom,depth=0,ltr_tag='letter',idx='',ltr_i=0,id_tag='idref'):
    letters = ltr_dom(ltr_tag)
    ltr_meta=epistolarized_chadwyck_ltr_meta(ltr_dom)
    if not idx: idx=ltr_meta.get('id_letter','')

    ol=[]
    ltr_i=0
    for ltr in letters:
        # orig meta
        odx=dict(ltr_meta.items())
        odx['id']=idx
        ltr_i+=1
        
        ## enclosed meta?
        for k,v in epistolarized_chadwyck_ltr_meta(ltr).items():
            if k and v: odx[k]=v
        ## new meta
        odx['ltr_i']=ltr_i
        odx=epistolarized_chadwyck_ltr_recip(odx)
        # odx['txt']=epistolarized_chadwyck_ltr_txt(ltr)
        odx['xml']=clean_text(str(ltr))
        odx['ltr_num_enclosed']=len(ltr(ltr_tag))
        ## add
        ol.append(odx)
    odf=pd.DataFrame(ol).fillna('')

    
    odf['ltr_depth']=odf.ltr_num_enclosed.max() - odf.ltr_num_enclosed
    odf=odf.sort_values(['ltr_depth','ltr_i'])
    odf['ltr_depth_i']=[
        i+1
        for ltr_depth,ltr_depth_df in sorted(odf.groupby('ltr_depth'))
        for i in range(len(ltr_depth_df.sort_values('ltr_i')))
    ]
    odf['id']=[
        join_if(idx,ltr_i, sep='/')
        for idx,ltr_i in zip(odf.id, odf.ltr_i)
    ]

    # remove enclosed
    odf = odf.sort_values(['ltr_depth','ltr_depth_i'],ascending=True)
    for ltr_id,ltr_xml in zip(odf.id, odf.xml):
        ltr_xml_ref=f'<letter id="{ltr_id}" />'
        newxml_l=[]
        for ltr_id2,ltr_xml2 in zip(odf.id, odf.xml):
            if ltr_id!=ltr_id2 and ltr_xml in ltr_xml2:
                ltr_newxml = ltr_xml2.replace(ltr_xml, ltr_xml_ref)
            else:
                ltr_newxml = ltr_xml2
            # print([ltr_id2,ltr_newxml])
            
            newxml_l.append(ltr_newxml)
        odf['xml']=newxml_l
    
    newxml_l=[]
    for ltr_id,ltr_xml in zip(odf.id, odf.xml):
        if ltr_xml.startswith('<letter'):
            ltr_xml=f'<letter id="{ltr_id}"' + ltr_xml[len("<letter"):]
            newxml_l.append(ltr_xml)
    odf['xml']=newxml_l
    odf['txt']=odf.xml.apply(epistolarized_chadwyck_ltr_txt)

    for tkey in ['xml','txt']:
        odf[tkey]=odf[tkey].apply(escape_linebreaks)
    

    # give ids to top level
    

    prefcols = [
        'id','id_letter','ltr_i','ltr_depth','ltr_depth_i','ltr_num_enclosed',
        'txt_head','txt_front',

    ]
    colcols = [col for col in odf.columns if col not in set(prefcols)]
    return odf[prefcols + colcols]






NLP_FROMTO=None
def nlp_epistolarized_fromto_stanza():
    global NLP_FROMTO
    if NLP_FROMTO is not None: return NLP_FROMTO    
    import stanza
    nlp = stanza.Pipeline(lang='en',verbose=False)#, processors='tokenize,ner')
    NLP_FROMTO=nlp
    return nlp


# def epistolarized_chadwyck_ltr(ltr_dom,id_text='',*x,**y):
#     ltr_meta = epistolarized_chadwyck_ltr_meta(ltr_dom,id_text=id_text,*x,**y)
#     txt= epistolarized_chadwyck_ltr_txt(ltr_dom,*x,**y)
#     return ltr_meta,txtometa

def nlp_get_doc(txt):
    if type(txt)!=str: return txt
    nlp = nlp_epistolarized_fromto_stanza()
    doc = nlp(txt)
    return doc

def nlp_get_ents(txt):
    dom=nlp_get_doc(txt) if type(txt)==str else txt
    return list(doc.ents)

# s = 'Miss Belford, To Robert Lovelace, Esq; | [In answer to his Letters, Num. LIV. LVII.]'
# doc = nlp_get_doc(s)
# doc.sentences[0].tokens[0]


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


def deduce_recip(sentdf,recip_words={'to'},sender_words={'from'}):
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
            
    




def epistolarized_chadwyck_ltr(ltr,*x,**y):
    ltr_meta = epistolarized_chadwyck_ltr_meta(ltr,*x,**y)
    txt = epistolarized_chadwyck_ltr_txt(ltr,*x,**y)
    ltr_meta = epistolarized_chadwyck_ltr_recip(ltr_meta)
    return ltr_meta,txt


def epistolarized_chadwyck_t(C,t,div1='div3',force=True,lim=None,progress=False,i=None,**kwargs):
    import bs4
    odir_xml=C.paths.get('path_xml')
    odir_txt=C.paths.get('path_txt')
    ifn_xml=t.path_xml
    # print(odir_xml,ifn_xml)
    if not os.path.exists(ifn_xml): return pd.DataFrame()
    with open(ifn_xml) as f: xml=f.read()
    dom = bs4.BeautifulSoup(xml,'lxml')
    # spit out meta
    ometa=[]
    letters=[ltrdiv for i,ltrdiv in enumerate(dom(div1)) if 'letter' in set([tag.name for tag in ltrdiv()])][:lim]
    if not letters: return pd.DataFrame()
    
    iterr=tqdm(letters,disable=not progress,position=0,desc='Scanning XML into letters')
    for letter in iterr:
        letter_i=len(ometa)+1
        # iterr.set_description(f'Computing letter (t={i}): {letter_i}')
        meta,otxt = epistolarized_chadwyck_ltr(letter,id_text=t.id,id_corpus=t.corpus.id)
        meta['letter_i']=letter_i
        meta['path_xml']=os.path.join(odir_xml, meta['id'] + '.xml')
        meta['path_txt']=os.path.join(odir_txt, meta['id'] + '.txt')
        meta['xml']=str(letter)
        meta['txt']=otxt
        ometa.append(meta)
        # iterr.set_description(f'Computing letter #{letter_i} (text #{i+1}): ')
    odf=pd.DataFrame(ometa).fillna('')
    odf=fix_meta(odf)
    odf=odf.sort_values('id') if 'id' in set(odf.columns) else odf
    return odf
    #return odf.set_index('id').sort_index() if 'id' in set(odf.columns) else odf

    
    
    
def update_df(odf, opath, reset=False, force=False, index=False, verbose=False, idcols=['id'],sort_by=None, sort_by_asc=[]):
    if not os.path.exists(opath):
        odf=odf
    else:
        dfl=[odf,read_df(opath)]
        odf=pd.concat(dfl).fillna('')
    odf=odf.drop_duplicates(idcols,keep='last').fillna('')        
    odf=fix_meta(odf.fillna(''))
    if sort_by: odf=odf.sort_values(sort_by, ascending=sort_by_asc)
    return odf



# save txt
def ensure_abs(path_root,path):
    return os.path.join(path_root,path) if not os.path.isabs(path) else path






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




#### CLASSES


# class TextEpistolary(BaseText): pass


# class Epistolary(BaseCorpus):
#     NAME='Epistolary'
#     ID='epistolary'
#     TEXT_CLASS=TextEpistolary
#     CORPORA_TO_START_WITH = ['Chadwyck']

    

#     def load_metadata(self):
#         meta=super().load_metadata()
#         meta['genre']='Fiction'
#         return meta.fillna('')

#     def get_init_texts(self,texts=[],lim=None):
#         # get initial texts
#         texts=load('Chadwyck').texts() if not texts else texts
#         return texts[:lim]

#     def compile(self,
#             parts=['texts','chars'],
#             *x,
#             **y):
#         # Get parts
#         parts=set(parts)
#         if 'texts' in parts: self.compile_texts(*x,**y)
#         if 'chars' in parts: self.compile_chars(*x,**y)


#     def compile_texts(self,texts=None,lim=None,lim_inner=None,progress=True,progress_inner=False,force=False,force_inner=False,*x,**y):
#         ofn_meta=self.paths['path_metadata']
#         if not force and os.path.exists(ofn_meta): return 
        
#         texts = self.get_init_texts(texts,lim)
#         iterr=tqdm(texts,desc='Compiling letter texts',disable=not progress,position=1)
        
#         ol=[
#             self.compile_text(t,lim=lim_inner,progress=progress_inner,force=force_inner,*x,**y)
#             for t in iterr
#         ]   
#         odf=pd.concat(ol).fillna('') if len(ol) else pd.DataFrame()
#         save_df(odf, ofn_meta, verbose=False)
#         return odf

#     def compile_text(C,t,lim=None,progress=True,force=False,*x,**y):
#         ofn_ltrs = os.path.join(C.paths['path_letters'], t.corpus.id, t.id + '.csv')
#         ofn_xml = os.path.join(C.paths['path_xml'], t.corpus.id, t.id + '.xml')
#         ofn_txt = os.path.join(C.paths['path_txt'], t.corpus.id, t.id + '.txt')

#         if not force and os.path.exists(ofn_ltrs):
#             odf = read_df(ofn_ltrs)
#             odf_anno=load_with_anno(ofn_ltrs)
#             if len(odf_anno) and 'id' in set(odf_anno.columns):
#                 odf_anno=odf_anno.set_index('id')
#                 odf_anno=odf_anno[[col for col in odf_anno.columns if not col.startswith('id_')]]
#                 odx_anno=dict((idx, dict(row)) for idx,row in odf_anno.iterrows())

#                 newkeys=set(rowdk for idx,rowd in odx_anno.items() for rowdk in rowd)
#                 for nk in newkeys:
#                     odf[nk]=[odx_anno.get(idx,{}).get(nk,'') for idx in odf.id]
            
#             return fix_meta(odf)
            

#         ol=[]
#         #if force or not os.path.exists(ofn_ltrs) or not os.path.exists(ofn_xml) or not os.path.exists(ofn_txt):
#         if not os.path.exists(os.path.dirname(ofn_xml)): os.makedirs(os.path.dirname(ofn_xml))
#         if not os.path.exists(os.path.dirname(ofn_txt)): os.makedirs(os.path.dirname(ofn_txt))            
#         if not os.path.exists(os.path.dirname(ofn_ltrs)): os.makedirs(os.path.dirname(ofn_ltrs))            
        
#         tdf=epistolarized_chadwyck_t(C,t=t,lim=lim,progress=progress,*x,**y)
#         if not len(tdf): return pd.DataFrame()

    
#         todf=fix_meta(tdf)

#         for pxml,txml in zip(tdf.path_xml,tdf.xml):
#             if pxml and txml:
#                 with open(pxml,'w') as of: of.write(txml)
#         for ptxt,ttxt in zip(tdf.path_txt,tdf.txt):
#             if ptxt and ttxt:
#                 with open(ptxt,'w') as of: of.write(ttxt)

#         for needcol in {'sender_tok','sender_id','recip_tok','recip_id','front','title_letter'}:
#             if not needcol in set(todf.columns): todf[needcol]=''
#         for badcol in {'txt','xml','path_txt','path_xml'} & set(todf.columns):
#             todf=todf.drop(badcol,1)
        
#         cols=[
#             'id','id_corpus','id_text','id_letter','letter_i',
#             'txt_head',
#             'txt_front',
#             'sender_tok',#'sender_id',
#             'recip_tok',#'recip_id',
#         ]
#         odf_ltrs=todf[[c for c in cols if c in set(todf.columns)]].fillna('')
#         save_df(fix_meta(odf_ltrs), ofn_ltrs, verbose=True)
#         return odf_ltrs

        
#     def compile_text_chars(self,t,tdf=None,force=False,force_inner=False,verbose=False,*x,**y):
#         id_corpus=t.corpus.id
#         id_text=t.id

#         ofn_tok2id = os.path.join(self.path_chars, id_corpus, id_text, 'tok2id.csv')
#         ofn_id2meta = os.path.join(self.path_chars, id_corpus, id_text, 'id2meta.csv')

#         if force or not os.path.exists(ofn_tok2id):
#             # get latest sender,recip data
#             if tdf is None: tdf=self.compile_text(t,force=force_inner,*x,**y)
#             odf_tok2id=calculate_tok2id(tdf.fillna(''))
#             save_df(odf_tok2id, ofn_tok2id, verbose=verbose, index=False)
#             if verbose: display(odf_tok2id)
#         else:
#             odf_tok2id=read_df(ofn_tok2id)
        
#         # update with anno
#         odf_tok2id_anno=load_with_anno(ofn_tok2id)
#         if len(odf_tok2id_anno):
#             d_tok2id_anno=dict(zip(odf_tok2id_anno.char_tok, odf_tok2id_anno.char_id)) if len(odf_tok2id_anno) else {}
#             odf_tok2id['char_id']=[d_tok2id_anno.get(ctok,cid) for ctok,cid in zip(odf_tok2id.char_tok,odf_tok2id.char_id)]
        
        

#         ## id2meta
#         if force or not os.path.exists(ofn_id2meta):
#             ## id2meta
#             char_ids = Counter()
#             for char_id,char_tok_count in zip(odf_tok2id.char_id, odf_tok2id.char_tok_count):
#                 char_ids[char_id]+=char_tok_count
#             # init?
#             id2meta_l=[]
#             for char_id,char_id_count in char_ids.most_common():
#                 char_dx={'char_id':char_id, 'char_id_count':char_id_count, **chardata_metakeys_initial}
#                 id2meta_l.append(char_dx)
#             odf_id2meta = pd.DataFrame(id2meta_l).fillna('')
#             save_df(odf_id2meta, ofn_id2meta, verbose=verbose, index=False)
#             if verbose: display(odf_id2meta)
#         else:
#             odf_id2meta=read_df(ofn_id2meta)

#         # update with anno
#         odf_id2meta=odf_id2meta.set_index('char_id')
#         odf_id2meta_anno=load_with_anno(ofn_id2meta)
#         if len(odf_id2meta_anno) and 'char_id' in set(odf_id2meta_anno.columns):
#             odf_id2meta_anno=odf_id2meta_anno.fillna('').set_index('char_id')
#             odf_id2meta_anno=odf_id2meta_anno[[col for col in odf_id2meta_anno if col not in {'char_id_count'}]]
#             odf_id2meta.update(odf_id2meta_anno)
#         odf_id2meta=odf_id2meta.reset_index()
        
#         return odf_tok2id.fillna(''), odf_id2meta.fillna('')
        
#     def get_letters(self,t,force=False,*x,**y):
#         df_letters=self.compile_text(t,force=force,*x,**y)
#         df_tok2id,df_id2meta=self.compile_text_chars(t,force=force,*x,**y)
#         d_tok2id=dict(zip(df_tok2id.char_tok, df_tok2id.char_id))
#         cols_id2meta_uniq=[col for col in df_id2meta if col not in set(df_letters.columns)]
#         df_id2meta=df_id2meta[cols_id2meta_uniq]

#         for sndr in ['sender','recip']:
#             sidkey=f'{sndr}_id'
#             df_letters[sidkey]=df_letters[f'{sndr}_tok'].apply(lambda stok: d_tok2id.get(stok,stok))
#             senders = set(df_letters[sidkey])
#             df_id2meta_now = df_id2meta[df_id2meta.char_id.isin(senders)]
#             df_id2meta_now.columns=[col.replace('char_',f'{sndr}_') for col in df_id2meta.columns]
#             if sidkey in set(df_id2meta_now.columns):
#                 df_letters = df_letters.merge(df_id2meta_now, how='left', on=sidkey)
            
#         return df_letters


#     def iter_letter_networks(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return iter_letter_networks_from_dfletters(dfletters,*x,**y)

#     def get_letter_network(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return get_letter_network_from_dfletters(dfletters,*x,**y)








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


# # def cast_init(obj, Class,*x,**y):
# #     obj.__class__=Class
# #     obj.__init__(*x,**y)
# #     return obj

# # def CastObjCls(old_text_obj,new_text_class,*x,**y):
# #     return cast_init(old_text_obj,new_text_class,*x, **y)

# # def SourceTextEpistolary(text_obj,*x, **y):
# #     return CastObjCls(text_obj, SourceTextEpistolaryCls,*x,**y)

# class TextEpistolary(BaseText):
#     pass




# class SourceTextEpistolaryCls(BaseText):
#     def __init__(self,epistolary_corpus,**kwargs):
#         self.C=epistolary_corpus
#         for k,v in kwargs.items(): setattr(self,k,v)
        
#     def compile(self,lim=None,progress=True,force=False,*x,**y):
#         ofn_ltrs = os.path.join(C.paths['path_letters'], t.corpus.id, t.id + '.csv')
#         ofn_xml = os.path.join(C.paths['path_xml'], t.corpus.id, t.id + '.xml')
#         ofn_txt = os.path.join(C.paths['path_txt'], t.corpus.id, t.id + '.txt')

#         if not force and os.path.exists(ofn_ltrs):
#             odf = read_df(ofn_ltrs)
#             odf_anno=load_with_anno(ofn_ltrs)
#             if len(odf_anno) and 'id' in set(odf_anno.columns):
#                 odf_anno=odf_anno.set_index('id')
#                 odf_anno=odf_anno[[col for col in odf_anno.columns if not col.startswith('id_')]]
#                 odx_anno=dict((idx, dict(row)) for idx,row in odf_anno.iterrows())

#                 newkeys=set(rowdk for idx,rowd in odx_anno.items() for rowdk in rowd)
#                 for nk in newkeys:
#                     odf[nk]=[odx_anno.get(idx,{}).get(nk,'') for idx in odf.id]
            
#             return fix_meta(odf)
            

#         ol=[]
#         #if force or not os.path.exists(ofn_ltrs) or not os.path.exists(ofn_xml) or not os.path.exists(ofn_txt):
#         if not os.path.exists(os.path.dirname(ofn_xml)): os.makedirs(os.path.dirname(ofn_xml))
#         if not os.path.exists(os.path.dirname(ofn_txt)): os.makedirs(os.path.dirname(ofn_txt))            
#         if not os.path.exists(os.path.dirname(ofn_ltrs)): os.makedirs(os.path.dirname(ofn_ltrs))            
        
#         tdf=epistolarized_chadwyck_t(C,t=t,lim=lim,progress=progress,*x,**y)
#         if not len(tdf): return pd.DataFrame()

    
#         todf=fix_meta(tdf)

#         for pxml,txml in zip(tdf.path_xml,tdf.xml):
#             if pxml and txml:
#                 with open(pxml,'w') as of: of.write(txml)
#         for ptxt,ttxt in zip(tdf.path_txt,tdf.txt):
#             if ptxt and ttxt:
#                 with open(ptxt,'w') as of: of.write(ttxt)

#         for needcol in {'sender_tok','sender_id','recip_tok','recip_id','front','title_letter'}:
#             if not needcol in set(todf.columns): todf[needcol]=''
#         for badcol in {'txt','xml','path_txt','path_xml'} & set(todf.columns):
#             todf=todf.drop(badcol,1)
        
#         cols=[
#             'id','id_corpus','id_text','id_letter','letter_i',
#             'txt_head',
#             'txt_front',
#             'sender_tok',#'sender_id',
#             'recip_tok',#'recip_id',
#         ]
#         odf_ltrs=todf[[c for c in cols if c in set(todf.columns)]].fillna('')
#         save_df(fix_meta(odf_ltrs), ofn_ltrs, verbose=True)
#         return odf_ltrs

        
#     def compile_text_chars(self,t,tdf=None,force=False,force_inner=False,verbose=False,*x,**y):
#         id_corpus=t.corpus.id
#         id_text=t.id

#         ofn_tok2id = os.path.join(self.path_chars, id_corpus, id_text, 'tok2id.csv')
#         ofn_id2meta = os.path.join(self.path_chars, id_corpus, id_text, 'id2meta.csv')

#         if force or not os.path.exists(ofn_tok2id):
#             # get latest sender,recip data
#             if tdf is None: tdf=self.compile_text(t,force=force_inner,*x,**y)
#             odf_tok2id=calculate_tok2id(tdf.fillna(''))
#             save_df(odf_tok2id, ofn_tok2id, verbose=verbose, index=False)
#             if verbose: display(odf_tok2id)
#         else:
#             odf_tok2id=read_df(ofn_tok2id)
        
#         # update with anno
#         odf_tok2id_anno=load_with_anno(ofn_tok2id)
#         if len(odf_tok2id_anno):
#             d_tok2id_anno=dict(zip(odf_tok2id_anno.char_tok, odf_tok2id_anno.char_id)) if len(odf_tok2id_anno) else {}
#             odf_tok2id['char_id']=[d_tok2id_anno.get(ctok,cid) for ctok,cid in zip(odf_tok2id.char_tok,odf_tok2id.char_id)]
        
        

#         ## id2meta
#         if force or not os.path.exists(ofn_id2meta):
#             ## id2meta
#             char_ids = Counter()
#             for char_id,char_tok_count in zip(odf_tok2id.char_id, odf_tok2id.char_tok_count):
#                 char_ids[char_id]+=char_tok_count
#             # init?
#             id2meta_l=[]
#             for char_id,char_id_count in char_ids.most_common():
#                 char_dx={'char_id':char_id, 'char_id_count':char_id_count, **chardata_metakeys_initial}
#                 id2meta_l.append(char_dx)
#             odf_id2meta = pd.DataFrame(id2meta_l).fillna('')
#             save_df(odf_id2meta, ofn_id2meta, verbose=verbose, index=False)
#             if verbose: display(odf_id2meta)
#         else:
#             odf_id2meta=read_df(ofn_id2meta)

#         # update with anno
#         odf_id2meta=odf_id2meta.set_index('char_id')
#         odf_id2meta_anno=load_with_anno(ofn_id2meta)
#         if len(odf_id2meta_anno) and 'char_id' in set(odf_id2meta_anno.columns):
#             odf_id2meta_anno=odf_id2meta_anno.fillna('').set_index('char_id')
#             odf_id2meta_anno=odf_id2meta_anno[[col for col in odf_id2meta_anno if col not in {'char_id_count'}]]
#             odf_id2meta.update(odf_id2meta_anno)
#         odf_id2meta=odf_id2meta.reset_index()
        
#         return odf_tok2id.fillna(''), odf_id2meta.fillna('')
        
#     def get_letters(self,t,force=False,*x,**y):
#         df_letters=self.compile_text(t,force=force,*x,**y)
#         df_tok2id,df_id2meta=self.compile_text_chars(t,force=force,*x,**y)
#         d_tok2id=dict(zip(df_tok2id.char_tok, df_tok2id.char_id))
#         cols_id2meta_uniq=[col for col in df_id2meta if col not in set(df_letters.columns)]
#         df_id2meta=df_id2meta[cols_id2meta_uniq]

#         for sndr in ['sender','recip']:
#             sidkey=f'{sndr}_id'
#             df_letters[sidkey]=df_letters[f'{sndr}_tok'].apply(lambda stok: d_tok2id.get(stok,stok))
#             senders = set(df_letters[sidkey])
#             df_id2meta_now = df_id2meta[df_id2meta.char_id.isin(senders)]
#             df_id2meta_now.columns=[col.replace('char_',f'{sndr}_') for col in df_id2meta.columns]
#             if sidkey in set(df_id2meta_now.columns):
#                 df_letters = df_letters.merge(df_id2meta_now, how='left', on=sidkey)
            
#         return df_letters


#     def iter_letter_networks(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return iter_letter_networks_from_dfletters(dfletters,*x,**y)

#     def get_letter_network(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return get_letter_network_from_dfletters(dfletters,*x,**y)



    

# class Epistolary(BaseCorpus):
#     NAME='Epistolary2'
#     ID='epistolary2'
#     TEXT_CLASS=TextEpistolary
#     CORPORA_TO_START_WITH = ['Chadwyck']

    


    
                




#     # if not self.col_id in metacols:
#     #     if self.col_fn in metacols:
#     #         meta[self.col_id]=meta[self.col_fn].apply(lambda x: os.path.splitext(x)[0])
#     #     else:
#     #         # print(f'!! [{self.name}] Corpus does not have "id" column in {self.col_id}')
#     #         meta[self.col_id]=[f't{i+1:08}' for i in range(len(meta))]
        
#     #     if not 'id_corpus' in metacols:
            
    

#     #     # outfit with extra    
#     #     meta['id_corpus']=self.id
#     #     meta['id_text']=meta[self.col_id]
        
#     #     meta['id']=[f'']

#     #     meta['_id']=[f'']
#     #     meta['path_freqs']=meta[self.col_id].apply(lambda idx: os.path.join(self.path_freqs, idx + self.EXT_FREQS))
#     #     meta['path_txt']=meta[self.col_id].apply(lambda idx: os.path.join(self.path_txt, idx + self.EXT_TXT))
#     #     meta['path_xml']=meta[self.col_id].apply(lambda idx: os.path.join(self.path_xml, idx + self.EXT_XML))
        
#     #     if 'year' in metacols:
#     #         meta['_year_orig']=meta['year']
#     #         meta['year']=pd.to_numeric(meta['year'],errors='coerce',downcast='integer')
    
#     #     # reorder
#     #     meta=fix_meta(meta)
#     #         # cache
#     #         # save_df(meta, self.path_metadata_cache)
#     #     else:
#     #         meta=pd.DataFrame()
#     #         textld=None

#     #     if len(meta):
#     #         # filter
#     #         if self.year_start is not None and str(self.year_start).isdigit() and 'year' in set(meta.columns):
#     #             meta=meta[meta.year>=self.year_start]
#     #         if self.year_end is not None and str(self.year_end).isdigit() and 'year' in set(meta.columns):
#     #             meta=meta[meta.year<self.year_end]                
#     #         self._metadf=meta.set_index(self.col_id,drop=True)

#     #         if init_texts:
#     #             self.load_texts()


#     #     else:
#     #         self._metadf=meta
#     #         self._textd={}
#     #         self._texts=[]
    
#     # return self._metadf


#     # def init_metadata(self, meta_init=pd.DataFrame(), set_index=['id','id_corpus','id_text']):
#     #     # local too?
#     #     meta_local = read_df(self.path_metadata_init) if os.path.exists(self.path_metadata_init) else pd.DataFrame()
        
#     #     # all of chad?
#     #     Chad = load('Chadwyck')
#     #     chad_meta = Chad.meta.reset_index()
#     #     chad_meta['id'] = chad_meta['id'].apply(lambda x: f'{Chad.id}|{x}')

#     #     # updated?
#     #     meta_l = map(df_requiring_id,[chad_meta, meta_local, meta_init])
#     #     meta_l=[mdf for mdf in meta_l if mdf is not None and len(mdf)]
#     #     for mdf in meta_l:
#     #         display(mdf)
            


#     #     if meta_l:
#     #         meta=meta_l[0]
#     #         display(meta)
#     #         for meta2 in meta_l[1:]:
#     #             display(meta2)
#     #             meta.update(meta2)

        
        
#     #     set_index=[si for si in set_index if si in set(meta_chad)]
        



class TextEpistolary(BaseText):
    pass


class Epistolary(BaseCorpus):
    NAME='Epistolary'
    ID='epistolary'
    TEXT_CLASS=TextEpistolary
    CORPORA_TO_START_WITH = ['Chadwyck']


class TextSectionLetter(TextSection):
    def deduce_recip(self, meta=None,keys=['txt_front','txt_head']):
        from lltk.model.ner import get_ner_sentdf

        ltr_meta=meta if meta is not None else self._meta

        txt = '     '.join(
            ltr_meta.get(argname,'').replace(' | ',' ')
            for argname in keys
        )
        
        byline_sentdf = None
        sender,recip = '',''
        if not ltr_meta.get('sender_tok'):
            if byline_sentdf is None: byline_sentdf = deduce_recip(get_ner_sentdf(txt))
            if 'epistolary_role' in set(byline_sentdf.columns):
                sender=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='sender'].text)

        ## recip
        if not ltr_meta.get('recip_tok'):
            if byline_sentdf is None: byline_sentdf = deduce_recip(get_ner_sentdf(txt))
            if 'epistolary_role' in set(byline_sentdf.columns):
                recip=' '.join(byline_sentdf[byline_sentdf.epistolary_role=='recip'].text)
        
        return sender,recip


class TextSectionLetterChadwyck(TextSectionLetter):
    sep_sents='\n'
    sep_paras='\n\n'
    sep_txt='\n\n------------\n\n'

    @property
    def meta(self):
        if self._meta_: return self._meta_
        meta=self._meta
        ltr_xml=self.xml
        ltr_dom=self.dom

        meta_map={
            'id_letter':'idref',
            'txt_front':['front','caption']
        }
        for newtag,xtag in meta_map.items():
            meta[newtag]=clean_text(grab_tag_text(ltr_dom, xtag)) if xtag else ''
        ltrtitle=''
        if '</collection>' in ltr_xml and '<attbytes>' in ltr_xml:
            ltrtitle=ltr_xml.split('</collection>')[-1].split('<attbytes>')[0].strip()
        meta['txt_head']=ltrtitle if ltrtitle!=meta['txt_front'] else ''
        meta['letter_i']=self.letter_i
        meta['id']=f'L{self.letter_i:03}' if self.letter_i else meta['id_letter']

        ## deduce recips?
        meta['sender_tok'], meta['recip_tok'] = self.deduce_recip(meta)

        self._meta_=meta
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


class TextEpistolaryChadwyck(BaseText):
    DIV='div3'
    LTR='letter'
    SECTION_CLASS=TextSectionLetterChadwyck

    @property
    def letters(self,lim=None,progress=False,**kwargs):
        if self._letters is None:
            self._letters=[]
            div_strs=[
                ltrxml.split(f'<{self.DIV}>',1)[-1].strip()
                for ltrxml in self.xml.split(f'</{self.DIV}>')[:-1]
                if f'</{self.LTR}>' in ltrxml.split(f'<{self.DIV}>',1)[-1]
            ]
            letter_i=0
            iterr=tqdm(div_strs, disable=not progress, desc='Scanning for letters')
            for ltrxml in iterr:
                letter_i+=1 #len(o)+1
                letter_id=f'L{letter_i:03}'
                #ltr=TextSectionLetterChadwyck(letter_id, _source=self,letter_i=letter_i)
                ltr=self.init_section(letter_id, letter_i=letter_i)
                ltr._xml=ltrxml
                self._letters.append(ltr)
        return self._letters


# C = Epistolary()
# C.meta




# class TextEpistolary(Text):
            
#     def compile_text_chars(self,t,tdf=None,force=False,force_inner=False,verbose=False,*x,**y):
#         id_corpus=t.corpus.id
#         id_text=t.id

#         ofn_tok2id = os.path.join(self.path_chars, id_corpus, id_text, 'tok2id.csv')
#         ofn_id2meta = os.path.join(self.path_chars, id_corpus, id_text, 'id2meta.csv')

#         if force or not os.path.exists(ofn_tok2id):
#             # get latest sender,recip data
#             if tdf is None: tdf=self.compile_text(t,force=force_inner,*x,**y)
#             odf_tok2id=calculate_tok2id(tdf.fillna(''))
#             save_df(odf_tok2id, ofn_tok2id, verbose=verbose, index=False)
#             if verbose: display(odf_tok2id)
#         else:
#             odf_tok2id=read_df(ofn_tok2id)
        
#         # update with anno
#         odf_tok2id_anno=load_with_anno(ofn_tok2id)
#         if len(odf_tok2id_anno):
#             d_tok2id_anno=dict(zip(odf_tok2id_anno.char_tok, odf_tok2id_anno.char_id)) if len(odf_tok2id_anno) else {}
#             odf_tok2id['char_id']=[d_tok2id_anno.get(ctok,cid) for ctok,cid in zip(odf_tok2id.char_tok,odf_tok2id.char_id)]
        
        

#         ## id2meta
#         if force or not os.path.exists(ofn_id2meta):
#             ## id2meta
#             char_ids = Counter()
#             for char_id,char_tok_count in zip(odf_tok2id.char_id, odf_tok2id.char_tok_count):
#                 char_ids[char_id]+=char_tok_count
#             # init?
#             id2meta_l=[]
#             for char_id,char_id_count in char_ids.most_common():
#                 char_dx={'char_id':char_id, 'char_id_count':char_id_count, **chardata_metakeys_initial}
#                 id2meta_l.append(char_dx)
#             odf_id2meta = pd.DataFrame(id2meta_l).fillna('')
#             save_df(odf_id2meta, ofn_id2meta, verbose=verbose, index=False)
#             if verbose: display(odf_id2meta)
#         else:
#             odf_id2meta=read_df(ofn_id2meta)

#         # update with anno
#         odf_id2meta=odf_id2meta.set_index('char_id')
#         odf_id2meta_anno=load_with_anno(ofn_id2meta)
#         if len(odf_id2meta_anno) and 'char_id' in set(odf_id2meta_anno.columns):
#             odf_id2meta_anno=odf_id2meta_anno.fillna('').set_index('char_id')
#             odf_id2meta_anno=odf_id2meta_anno[[col for col in odf_id2meta_anno if col not in {'char_id_count'}]]
#             odf_id2meta.update(odf_id2meta_anno)
#         odf_id2meta=odf_id2meta.reset_index()
        
#         return odf_tok2id.fillna(''), odf_id2meta.fillna('')
        
#     def get_letters(self,t,force=False,*x,**y):
#         df_letters=self.compile_text(t,force=force,*x,**y)
#         df_tok2id,df_id2meta=self.compile_text_chars(t,force=force,*x,**y)
#         d_tok2id=dict(zip(df_tok2id.char_tok, df_tok2id.char_id))
#         cols_id2meta_uniq=[col for col in df_id2meta if col not in set(df_letters.columns)]
#         df_id2meta=df_id2meta[cols_id2meta_uniq]

#         for sndr in ['sender','recip']:
#             sidkey=f'{sndr}_id'
#             df_letters[sidkey]=df_letters[f'{sndr}_tok'].apply(lambda stok: d_tok2id.get(stok,stok))
#             senders = set(df_letters[sidkey])
#             df_id2meta_now = df_id2meta[df_id2meta.char_id.isin(senders)]
#             df_id2meta_now.columns=[col.replace('char_',f'{sndr}_') for col in df_id2meta.columns]
#             if sidkey in set(df_id2meta_now.columns):
#                 df_letters = df_letters.merge(df_id2meta_now, how='left', on=sidkey)
            
#         return df_letters


#     def iter_letter_networks(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return iter_letter_networks_from_dfletters(dfletters,*x,**y)

#     def get_letter_network(self,t,dfletters=None,*x,**y):
#         dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
#         return get_letter_network_from_dfletters(dfletters,*x,**y)







# class Epistolary(Corpus):
#     NAME='Epistolary'
#     ID='epistolary'
#     TEXT_CLASS=TextEpistolary
#     CORPORA_TO_START_WITH = ['Chadwyck']


# # C = Epistolary()
# # C.meta