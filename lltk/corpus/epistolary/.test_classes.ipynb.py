#!/usr/bin/env python
# coding: utf-8

# In[3]:


import os,sys; sys.path.insert(0,os.path.abspath('../../..'))
from epistolary import *


# In[32]:


def setup_log():
    log.remove()
    format="""<level>{message:<150}</level> <cyan>[{name}.{function}()]</cyan> ({time:HH:mm:ss})"""
    log.add(sys.stderr, colorize=True, format=format)
setup_log()


# In[33]:


# printm('### Testing Text()')
t = Text('hello', corpus='chadwyck')


# In[34]:


C=Corpus('chadwyck')


# In[ ]:


stop


# In[4]:





# In[24]:




def _show(t):
    printm(f"""
* ID: {t.id}
* name: {t.name}
* corpus: {t.corpus}

* corpus copy: {C.textd.get(t.id)}
""")
    print()

printm('#### t = C.text()')
t1 = C.text()
_show(t1)

printm('#### t = Text("id")')
t2 = Text('plain_text')
_show(t2)

printm('##### C.add_text(t)')
C.add_text(t2)
_show(t2)


printm('#### t = Text("_corpus/id")')
t3 = Text('_chadwyck/with_corpus_ref')
_show(t3)


# In[14]:


t2 = Text('_chadwyck/hello2')


# In[ ]:





# In[6]:


# resetindex(C.meta)


# In[7]:


# def init(self, meta_final=None, other_meta=[], expand_meta=True, force=False,**kwargs):
#     # metas
#     meta_objs=[
#         self.path_metadata_int,
#         self.path_metadata,
#         self._metadf if not force else None,
#     ]

#     # first build objects
#     for t in 

    
#     # so far?
#     meta_l=[]
#     # local too?
#     if self.path_metadata_init and os.path.exists(self.path_metadata_init):
#         meta_l.append(self.path_metadata_init)
    
#     # fn?
#     if self.path_metadata and os.path.exists(self.path_metadata):
#         meta_l.append(self.path_metadata)
    
#     # already?
#     if not force and self._metadf is not None:
#         meta_l.append(self._metadf)
    
#     # others?
#     meta_l.extend(other_meta)

#     # final?
#     if meta_final is not None: meta_l.append(meta_final)

#     meta=None
#     for mdf_or_fn in meta_l:
#         # display('MERGING',mdf_or_fn)

#         mdf = load_metadata_from_df_or_fn(mdf_or_fn,**kwargs)
#         # display(mdf)

#         if meta is None:
#             meta=mdf
#         else:
#             for col in set(mdf.columns) - set(meta.columns): meta[col]=''
#             meta.update(mdf)

#         # display(meta,'\n\n\n\n\n\n')

    
#     self._metadf = meta

#     if meta is not None: 
#         meta = meta.fillna('')
#         text_il=meta.index
#         text_ld=meta.reset_index().to_dict(orient='records')
#         self._texts=[
#             self.init_text(id=idx,meta=text_d)
#             for idx,text_d in zip(
#                 meta.index,
#                 meta.to_dict(orient='records')
#             )
#         ]
#         self._textd=dict(zip(meta.index, self._texts))

#         if expand_meta:
#             meta = self._metadf = load_metadata_from_df_or_fn(
#                 pd.DataFrame([t.meta for t in self._texts])
#             )
    
#     return self._metadf
    


# In[9]:


init(C)


# In[6]:


gen=iter_metadata_from_df_or_fn(C.meta)
next(gen)


# In[ ]:





# In[15]:


c.textd['hello']


# In[ ]:





# In[14]:


c.init()

c.textd[CLAR_IDX]


# In[10]:


get_ipython().run_line_magic('pinfo2', 'meta_iter_corpora')


# In[15]:


get_ipython().run_line_magic('pinfo2', 'c.text')


# In[16]:





# In[3]:


t.corpus.name, t.id


# In[4]:


# t=Text()
# t=Text('hello')
t=Text(CLAR_ID)

print(t)
print(t.id, type(t))
print(t.corpus.id, t.corpus.name, type(t))


# In[5]:


t=Text('hello')


# In[ ]:





# In[ ]:





# In[4]:


type(c.text('hello'))


# In[ ]:





# In[11]:


clar = Text(CLAR_ID)
clar.corpus


# In[14]:


type(clar)


# In[ ]:





# In[16]:


Text('testing testing')


# In[ ]:





# In[37]:


x = Text('testing testing')


# In[38]:


x.corpus


# In[19]:


type(t.corpus)


# In[ ]:





# In[4]:


c=Corpus('chadwyck')
c.id, c.name, type(c)


# In[6]:


Corpus(c)


# In[8]:


Corpus(c) is c


# In[ ]:





# In[ ]:


c.XML


# In[21]:


c = Corpus('cref')


# In[24]:


c.idacef


# In[17]:


t.path_texts


# In[11]:


c.path_root


# In[ ]:





# In[3]:


clarid = get_clarissa_id()
t =clar = get_clarissa()
clar


# In[4]:


t=Text(id=clarid)


# In[6]:


t.corpus


# In[10]:





# In[10]:


class TextEpistolary(Text):

    def __init__(self):
        self.path_te

    def compile(self,lim=None,progress=True,force=False,*x,**y):
        ofn_ltrs = os.path.join(C.paths['path_letters'], t.corpus.id, t.id + '.csv')
        ofn_xml = os.path.join(C.paths['path_xml'], t.corpus.id, t.id + '.xml')
        ofn_txt = os.path.join(C.paths['path_txt'], t.corpus.id, t.id + '.txt')

        if not force and os.path.exists(ofn_ltrs):
            odf = read_df(ofn_ltrs)
            odf_anno=load_with_anno(ofn_ltrs)
            if len(odf_anno) and 'id' in set(odf_anno.columns):
                odf_anno=odf_anno.set_index('id')
                odf_anno=odf_anno[[col for col in odf_anno.columns if not col.startswith('id_')]]
                odx_anno=dict((idx, dict(row)) for idx,row in odf_anno.iterrows())

                newkeys=set(rowdk for idx,rowd in odx_anno.items() for rowdk in rowd)
                for nk in newkeys:
                    odf[nk]=[odx_anno.get(idx,{}).get(nk,'') for idx in odf.id]
            
            return fix_meta(odf)
            

        ol=[]
        #if force or not os.path.exists(ofn_ltrs) or not os.path.exists(ofn_xml) or not os.path.exists(ofn_txt):
        if not os.path.exists(os.path.dirname(ofn_xml)): os.makedirs(os.path.dirname(ofn_xml))
        if not os.path.exists(os.path.dirname(ofn_txt)): os.makedirs(os.path.dirname(ofn_txt))            
        if not os.path.exists(os.path.dirname(ofn_ltrs)): os.makedirs(os.path.dirname(ofn_ltrs))            
        
        tdf=epistolarized_chadwyck_t(C,t=t,lim=lim,progress=progress,*x,**y)
        if not len(tdf): return pd.DataFrame()

    
        todf=fix_meta(tdf)

        for pxml,txml in zip(tdf.path_xml,tdf.xml):
            if pxml and txml:
                with open(pxml,'w') as of: of.write(txml)
        for ptxt,ttxt in zip(tdf.path_txt,tdf.txt):
            if ptxt and ttxt:
                with open(ptxt,'w') as of: of.write(ttxt)

        for needcol in {'sender_tok','sender_id','recip_tok','recip_id','front','title_letter'}:
            if not needcol in set(todf.columns): todf[needcol]=''
        for badcol in {'txt','xml','path_txt','path_xml'} & set(todf.columns):
            todf=todf.drop(badcol,1)
        
        cols=[
            'id','id_corpus','id_text','id_letter','letter_i',
            'txt_head',
            'txt_front',
            'sender_tok',#'sender_id',
            'recip_tok',#'recip_id',
        ]
        odf_ltrs=todf[[c for c in cols if c in set(todf.columns)]].fillna('')
        save_df(fix_meta(odf_ltrs), ofn_ltrs, verbose=True)
        return odf_ltrs

        
    def compile_text_chars(self,t,tdf=None,force=False,force_inner=False,verbose=False,*x,**y):
        id_corpus=t.corpus.id
        id_text=t.id

        ofn_tok2id = os.path.join(self.path_chars, id_corpus, id_text, 'tok2id.csv')
        ofn_id2meta = os.path.join(self.path_chars, id_corpus, id_text, 'id2meta.csv')

        if force or not os.path.exists(ofn_tok2id):
            # get latest sender,recip data
            if tdf is None: tdf=self.compile_text(t,force=force_inner,*x,**y)
            odf_tok2id=calculate_tok2id(tdf.fillna(''))
            save_df(odf_tok2id, ofn_tok2id, verbose=verbose, index=False)
            if verbose: display(odf_tok2id)
        else:
            odf_tok2id=read_df(ofn_tok2id)
        
        # update with anno
        odf_tok2id_anno=load_with_anno(ofn_tok2id)
        if len(odf_tok2id_anno):
            d_tok2id_anno=dict(zip(odf_tok2id_anno.char_tok, odf_tok2id_anno.char_id)) if len(odf_tok2id_anno) else {}
            odf_tok2id['char_id']=[d_tok2id_anno.get(ctok,cid) for ctok,cid in zip(odf_tok2id.char_tok,odf_tok2id.char_id)]
        
        

        ## id2meta
        if force or not os.path.exists(ofn_id2meta):
            ## id2meta
            char_ids = Counter()
            for char_id,char_tok_count in zip(odf_tok2id.char_id, odf_tok2id.char_tok_count):
                char_ids[char_id]+=char_tok_count
            # init?
            id2meta_l=[]
            for char_id,char_id_count in char_ids.most_common():
                char_dx={'char_id':char_id, 'char_id_count':char_id_count, **chardata_metakeys_initial}
                id2meta_l.append(char_dx)
            odf_id2meta = pd.DataFrame(id2meta_l).fillna('')
            save_df(odf_id2meta, ofn_id2meta, verbose=verbose, index=False)
            if verbose: display(odf_id2meta)
        else:
            odf_id2meta=read_df(ofn_id2meta)

        # update with anno
        odf_id2meta=odf_id2meta.set_index('char_id')
        odf_id2meta_anno=load_with_anno(ofn_id2meta)
        if len(odf_id2meta_anno) and 'char_id' in set(odf_id2meta_anno.columns):
            odf_id2meta_anno=odf_id2meta_anno.fillna('').set_index('char_id')
            odf_id2meta_anno=odf_id2meta_anno[[col for col in odf_id2meta_anno if col not in {'char_id_count'}]]
            odf_id2meta.update(odf_id2meta_anno)
        odf_id2meta=odf_id2meta.reset_index()
        
        return odf_tok2id.fillna(''), odf_id2meta.fillna('')
        
    def get_letters(self,t,force=False,*x,**y):
        df_letters=self.compile_text(t,force=force,*x,**y)
        df_tok2id,df_id2meta=self.compile_text_chars(t,force=force,*x,**y)
        d_tok2id=dict(zip(df_tok2id.char_tok, df_tok2id.char_id))
        cols_id2meta_uniq=[col for col in df_id2meta if col not in set(df_letters.columns)]
        df_id2meta=df_id2meta[cols_id2meta_uniq]

        for sndr in ['sender','recip']:
            sidkey=f'{sndr}_id'
            df_letters[sidkey]=df_letters[f'{sndr}_tok'].apply(lambda stok: d_tok2id.get(stok,stok))
            senders = set(df_letters[sidkey])
            df_id2meta_now = df_id2meta[df_id2meta.char_id.isin(senders)]
            df_id2meta_now.columns=[col.replace('char_',f'{sndr}_') for col in df_id2meta.columns]
            if sidkey in set(df_id2meta_now.columns):
                df_letters = df_letters.merge(df_id2meta_now, how='left', on=sidkey)
            
        return df_letters


    def iter_letter_networks(self,t,dfletters=None,*x,**y):
        dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
        return iter_letter_networks_from_dfletters(dfletters,*x,**y)

    def get_letter_network(self,t,dfletters=None,*x,**y):
        dfletters=self.get_letters(t,*x,**y) if dfletters is None else dfletters
        return get_letter_network_from_dfletters(dfletters,*x,**y)







class Epistolary(Corpus):
    NAME='Epistolary'
    ID='epistolary'
    TEXT_CLASS=TextEpistolary
    CORPORA_TO_START_WITH = ['Chadwyck']


# C = Epistolary()
# C.meta


# In[5]:


C=Epistolary()
C.init()


# In[6]:


t=C.texts()[-1]
t, t.source


# In[8]:


C.texts()


# In[ ]:





# In[15]:


# t = Text(id='_chadwyck/Eighteenth-Century_Fiction/burney.01')
# t.meta


# In[17]:


# Chad=load('Chadwyck')
# Chad.init()
# Chad.meta


# In[12]:


meta=Chad.load_metadata_file()


# In[16]:




# def load_metadata_from_df_or_fn(idf,force=False,**attrs):
#     if type(idf)==str: idf=read_df(idf)
#     if idf is None or not len(idf): return pd.DataFrame()
#     #return df_requiring_id_and_corpus(idf,**attrs)
#     return df_requiring_id(idf,**attrs).fillna('')


# def df_requiring_id(df,idkey='id',fillna='',*x,**y):
#     if df is None or not len(df): return pd.DataFrame(columns=[],index=[]).rename_axis(idkey)
#     if df.index.name==idkey and not idkey in set(df.columns): df=df.reset_index()
#     if not idkey in set(df.columns): df[idkey]=''
#     df[idkey]=df[idkey].fillna('')
#     df[idkey]=[(idx if idx else f'X{i+1:04}') for i,idx in enumerate(df[idkey])]
#     df=df.fillna(fillna) if fillna is not None else df
#     df=df.set_index(idkey)
#     return df


# In[ ]:





# In[ ]:





# In[ ]:





# In[20]:





# In[5]:


t=C.texts()[1]
t


# In[10]:


t.source.meta


# In[11]:


# !echo $PATH


# In[2]:





# In[4]:





# In[3]:


idx='_chadwyck/Eighteenth-Century_Fiction/richards.04'


# In[29]:


C = Epistolary()
C.init()
C.t.meta


# In[37]:


t=C.t
t._meta


# In[41]:


t.source.corpus.init_metadata()


# In[21]:


# t.id, t.source.id


# In[23]:





# In[ ]:





# In[9]:


t.meta


# In[13]:


t.source_text()


# In[ ]:





# In[ ]:





# In[22]:





# In[23]:


source_text(t)


# In[ ]:





# In[7]:





# In[8]:


Chad=load('Chadwyck')
Chad.meta


# In[ ]:





# In[43]:


# load_metadata_from_df_or_fn(meta, id_corpus_default='chad')
meta_final=pd.DataFrame([
    {'id_text':'Eighteenth-Century_Fiction/richards.01', 'final':'!', 'id_corpus':'chadwyck'},
])
load_metadata_from_df_or_fn(meta_final)
# meta_final


# In[ ]:





# In[44]:


C = Epistolary()


# In[161]:


# load_metadata_from_df_or_fn(meta)


# In[ ]:





# In[125]:


meta


# In[106]:


# meta.update??


# In[107]:


init_metadata(Epistolary(),meta_init=meta,other_meta=[lltk.load('Chadwyck')])


# In[24]:





# In[23]:





# In[27]:


C=Epistolary()
C.init_metadata()


# In[26]:


C.path_metadata_init


# In[25]:


C.load_metadata()


# In[19]:


C=Epistolary()


# In[20]:


C.texts


# In[22]:





# In[25]:


clary = get_clarissa()
clary = SourceTextEpistolary(clary, hello='x!?')


# In[19]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[3]:





# In[ ]:




