#!/usr/bin/env python
# coding: utf-8

# In[8]:


import sys; sys.path.insert(0,'..')
from lltk.imports import *


# In[9]:


addr='_testdb/ulysses7'


# In[10]:


tdb=DB[addr]
if tdb is not None:
    pprint(tdb._meta)
    print(tdb.author, tdb.title, tdb.year)


# In[11]:


tdb.wiki


# In[12]:


tdb.sources


# In[13]:


tdb.get_sources()


# In[14]:


tdb.corpus.matcher[tdb.addr]


# In[15]:


t = Text(addr,title='Ulysses',author='Joyce',year=19223432423)
if t is not None:
    pprint(t._meta)
    print(t.author, t.title, t.year)


# In[ ]:





# In[5]:


t.sources


# In[6]:


print(t.get_sources(force=True))


# In[7]:


print(t.get_sources())


# In[ ]:





# In[ ]:





# In[ ]:




