#!/usr/bin/env python
# coding: utf-8

# # MarkMark 

# In[1]:


import sys; sys.path.insert(0,'/Users/ryan/github/yapmap')
import sys; sys.path.insert(0,'../../..')
from markmark import *
log.hide()


# In[2]:


# load corpus
C=Corpus('markmark')
C.metadata()


# In[3]:


C.metadata(from_sources=True,remote=True)


# In[ ]:





# In[ ]:





# In[ ]:


stopx


# In[ ]:


C.meta['instance_of|P31__wikidata'].value_counts()


# In[ ]:


t=C.t
t, t.qstr


# In[ ]:


C.metadata()


# In[ ]:





# In[ ]:


C.meta


# In[ ]:


C.meta.year.plot.density()


# In[ ]:


C.metadata.gender.value_counts().plot.pie()


# In[ ]:


C.metadata.nation.value_counts().plot.pie()


# In[ ]:


# Top 100 words overall, as determined by `n_agg` function over `valtype`
# Then a row for each of these 100 words in each period (`keep_periods` == True) if it's in there 
mfw_df = C.mfw_df(
    n=25000,   # limit to top N words,
    yearbin = 25, # any year delimiter. set to False for no periodizing
    n_by_period=False,    # top N per period or top N overall?
)
mfw_df


# In[ ]:





# In[ ]:


C.mdw('gender')


# In[ ]:


# plot overall top 10 words over the separate periods, where a period is a decade
import plotnine as p9
fig=p9.ggplot(
    p9.aes(x='period',y='fpm',color='word'),
    data=C.mfw_df(n=10, yearbin=25, keep_periods=True, excl_stopwords=True, excl_top=100)
)
fig+=p9.geom_point()
fig+=p9.geom_line(p9.aes(group='word'))
fig


# In[ ]:


C.mdw('gender')


# In[ ]:


C.mdw('nation')


# In[ ]:


p9.ggplot(
    p9.aes(x='M',y='F',label='word'),
    data=C.mdw('gender').reset_index().sample(n=100)
) + p9.geom_point() + p9.geom_label(size=6)# + p9.scale_y_log10() + p9.scale_x_log10()


# In[ ]:




