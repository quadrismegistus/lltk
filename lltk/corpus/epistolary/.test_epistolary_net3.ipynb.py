#!/usr/bin/env python
# coding: utf-8

# # Epistolary

# In[ ]:


import os,sys; sys.path.insert(0,os.path.abspath('../../..'))
from epistolary import *


# In[2]:


novel = get_pamela()
# dom = novel.letters.dom
# len(dom('div2'))


# In[3]:



# novel.letters.metadata(force=True)
# novel.letters.save_metadata()
# novel.letters.metadata(force=True)


# In[ ]:


#novel.letters.interactions()
novel.booknlp.init()


# In[ ]:





# In[ ]:





# In[4]:


odf=novel.interactions()
odf


# In[5]:


get_ipython().run_line_magic('pinfo2', 'np.isnan')


# In[1]:


g=get_letter_network_from_dfletters(novel.letters.interactions())


# In[19]:


from lltk.model.characters import filter_graph
g=filter_graph(g,min_weight=1,remove_isolates=True)
draw_nx(g)


# In[5]:


novel.letters.names()


# In[6]:


novel.booknlp.names()


# In[7]:


CS.names()


# In[10]:


CS.init_names()


# In[16]:





# In[17]:


names(CS)


# In[11]:


novel.characters().init_names_auto()


# In[3]:


novel.letters.names()


# In[9]:


novel.booknlp.init_names()


# In[10]:





# In[6]:


novel.booknlp.names()


# In[15]:


CS.init_names()
CS.names()


# In[25]:


len(novel.booknlp.init_names_anno())


# In[5]:


# novel.booknlp.models()


# In[ ]:





# In[3]:


# novel.letters.init_names()
# novel.letters._df_names


# In[4]:


# novel.booknlp.init_names()


# In[5]:


# novel.letters.path_names_anno


# In[7]:


len(novel.letters.init_names_anno())


# In[5]:





# In[8]:


res=init_names_auto(novel.letters,force=True)


# In[8]:





# In[10]:


for model in CS.iter_models():
    print(model.path_interactions)


# In[13]:


CS.interactions(force=True)


# In[5]:


novel.booknlp.interactions()


# In[14]:


next(novel.booknlp.iter_interactions())


# In[20]:


xdf=interactions(novel)
odf=pd.DataFrame(xdf)
odf
odf[(odf.target=="") | (odf.source=="")]
odf[odf.rel.str.contains('enclose')]
odf[~odf.rel.str.contains('enclose')]


# In[ ]:





# In[8]:





# In[9]:





# In[26]:


letters.meta


# In[13]:


xdf[xdf.source==""]


# In[11]:


# !pip install dateparser


# In[2]:





# In[4]:


pd.to_numeric(clarissa_letters.meta.num_words).sum(), clarissa_letters.length


# In[5]:


# clarissa_letters.meta.groupby('sender_tok').num_words.sum().sort_values(ascending=False).head(10)
# clarissa_letters.meta.groupby('recip_tok').num_words.sum().sort_values(ascending=False).head(10)


# ## Pamela

# In[49]:


# pamela = C.textd['_chadwyck/Eighteenth-Century_Fiction/richards.04']
# pamela_letters = pamela.sections
# pamela_letters.meta


# In[50]:


# pd.to_numeric(pamela_letters.meta.num_words).sum(), pamela.length


# ## Evelina

# In[51]:


evelina = C.textd['_chadwyck/Eighteenth-Century_Fiction/burney.01']
evelina_letters = evelina.sections
evelina_letters.meta


# In[52]:


evelina_letters.meta.num_words.sum(), evelina.length


# In[53]:


# evelina_letters.meta.groupby('sender_tok').num_words.sum().sort_values(ascending=False)
# evelina_letters.meta.groupby('recip_tok').num_words.sum().sort_values(ascending=False)


# ## Haywood

# In[54]:


betsy = C.textd['_chadwyck/Eighteenth-Century_Fiction/haywood.02']
betsy_letters = betsy.sections
betsy_letters.meta


# In[55]:


betsy_letters.meta.groupby('sender_tok').num_words.sum().sort_values(ascending=False)
# betsy_letters.meta.groupby('recip_tok').num_words.sum().sort_values(ascending=False)


# In[ ]:


betsy_letters.meta.num_words.sum(), betsy.length


# In[ ]:




