#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Management


# In[2]:


import lltk


# In[3]:


def cleancorpmeta(C):
    # clean meta
#     print(C.name,'...')
    lltk.save_df(C.meta, C.path_metadata)


# In[4]:


# cleancorpmeta(lltk.load('internet_archive'))


# In[5]:


Cs=[C for cname,C in lltk.corpora()]


# In[6]:


# res=lltk.pmap(cleancorpmeta, Cs[-4:], num_proc=4)


# In[7]:


def preproc(C):
    print(C.name)
    get_ipython().system('rm -r {C.path_data}')
    C.preprocess()


# In[8]:


res=lltk.pmap(
    preproc,
    Cs[3:],
    num_proc=1
)


# In[ ]:


def share(C):
    C.zip()
    C.upload()
    C.share()


# In[ ]:




