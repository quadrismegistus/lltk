#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys; sys.path.insert(0,'../../yapmap')
import sys; sys.path.insert(0,'..')
from lltk import *


# In[2]:


t=Text(CLAR_ID)


# In[3]:


# stopx


# In[4]:


t.matcher_global.g.edges(data=True)


# In[5]:


t.match(t.source, rel='other_rel')


# In[ ]:





# In[6]:


# %%timeit
qidx='Q50377310'
Corpus('wikidata').get_text_cache_json(qidx)


# In[7]:


t=Text(title='Outline',author='Cusk', _new=True)


# In[8]:


t.wiki


# In[ ]:




