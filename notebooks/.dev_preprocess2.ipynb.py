#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys; sys.path.insert(0,'../../yapmap')
import sys; sys.path.insert(0,'..')
from lltk import *


# In[2]:


C=Corpus('chadwyck')


# In[3]:


# C.metadata()


# In[6]:


# with log.shown():
#     df=C.metadata(from_sources=True,force=True,remote=True,from_cache=True,lim=25)
#     dfx=df


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[5]:


# with log.hidden():
df=C.metadata(from_sources=True,force=True,remote=True,from_cache=True,lim=None)
dfx=df

