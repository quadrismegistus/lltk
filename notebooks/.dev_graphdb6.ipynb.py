#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys; sys.path.insert(0,'../../yapmap')
import sys; sys.path.insert(0,'..')
from lltk import *
# log.show()


# In[2]:


C=Corpus('chadwyck')
t=C.au.Austen.Emma
th=Corpus('hathi').text_from(t)
tw=Corpus('wikidata').text_from(t)
th,tw


# In[4]:


with log: x=t.metadata_from_sources()
x


# In[ ]:





# In[6]:


t.get_sources()


# In[7]:


th.metadata(remote=True)


# In[8]:


t=Text(title="Ulysses",author="Joyce")
with log: t.metadata(remote=True)


# In[9]:


with log: tw = Corpus('wikidata').text_from(t)
tw


# In[ ]:





# In[10]:


t1=Text(id=1)
t2=Text(id=2)
t3=Text(id=3)


# In[11]:


t1.match(t2)
t2.match(t3)
t2.match(t3)


# In[12]:


t4=Text()
t4.match(t1)
t4.get_matches()


# In[ ]:





# In[13]:


gdb.get_neighbs(t1.addr)


# In[ ]:


gdb.get_rels(t1.addr)


# In[ ]:


t.gdb.get_neighbs(t.addr)


# In[ ]:





# In[ ]:





# In[ ]:


with log:
    C=Corpus('chadwyck')
    C.init()
    print(C.gdb)


# In[ ]:


C.t.sources


# In[ ]:


t=C.au.Austen.Pride_And_Prejudice


# In[ ]:


with log: Corpus('wikidata').text_from(t)


# In[ ]:




